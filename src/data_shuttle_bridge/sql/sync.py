from __future__ import annotations

from collections import defaultdict, deque
from enum import Enum
from typing import Dict, List, Iterable, Tuple, Set

from sqlmodel import Session, select

from data_shuttle_bridge.sql.changelog import ChangeLog, SyncState
from data_shuttle_bridge.sql.payloads import TableSchema, apply_row, serialize_row
from data_shuttle_bridge.sql.typing_ import ChangePayload
from data_shuttle_bridge.sql.wiring import set_current_node_id, get_current_node_id


class ConflictPolicy(str, Enum):
    LWW = "last_write_wins"
    VERSION = "version_strict"


class SyncEngine:
    def __init__(
        self,
        session: Session,
        peer_id: str,
        schema: Dict[str, TableSchema],
        policy: ConflictPolicy = ConflictPolicy.LWW,
        parent_first_order: Iterable[str] | None = None,
        node_id: str | None = None,
    ):
        self.sess = session
        self.peer_id = peer_id
        self.schema = schema
        self.policy = policy
        self.node_id = node_id
        self.order = (
            list(parent_first_order) if parent_first_order else self._compute_order()
        )

    def _compute_order(self) -> List[str]:
        deps: Dict[str, Set[str]] = {
            name: set(ts.parents) for name, ts in self.schema.items()
        }
        indeg: Dict[str, int] = {name: 0 for name in deps}
        for n in deps:
            for p in deps[n]:
                if p in deps:
                    indeg[n] += 1
        q = deque([n for n, d in indeg.items() if d == 0])
        out: List[str] = []
        while q:
            u = q.popleft()
            out.append(u)
            for v in deps:
                if u in deps[v]:
                    indeg[v] -= 1
                    if indeg[v] == 0:
                        q.append(v)
        if len(out) != len(deps):
            remain = [n for n in deps if n not in out]
            out.extend(remain)
        return out

    def _serialize_change(self, ch: ChangeLog) -> ChangePayload:
        ts = self.schema[ch.table]
        data = None
        if ch.op in ("I", "U"):
            obj = self.sess.get(ts.model, ch.pk)
            if obj:
                data = serialize_row(obj, ts.fields)
        return {
            "id": ch.id,
            "table": ch.table,
            "pk": ch.pk,
            "op": ch.op,  # type: ignore
            "version": ch.version,
            "data": data,
            "at": ch.at.isoformat() if ch.at else None,
        }

    def local_changes_since(
        self, since_id: int, limit: int = 1000
    ) -> List[ChangePayload]:
        """Get changes since a given ID. For pushing to remote, we only push changes from OTHER nodes, not our own."""
        query = (
            select(ChangeLog)
            .where(ChangeLog.id > since_id)
            .order_by(ChangeLog.id.asc())
            .limit(limit)
        )
        # When pushing, exclude our own node's changes (we already have them)
        # Only push changes from other nodes to maintain proper sync
        if self.node_id:
            from sqlalchemy import or_

            query = query.where(
                or_(
                    ChangeLog.node_id.is_(
                        None
                    ),  # Include legacy changes with no node_id
                    ChangeLog.node_id
                    != self.node_id,  # Include changes from other nodes only
                )
            )
        rows = self.sess.exec(query).all()
        return [self._serialize_change(r) for r in rows]

    def remote_changes_since(
        self, since_id: int, limit: int = 1000, exclude_node_id: str | None = None
    ) -> List[ChangePayload]:
        """Get changes since a given ID, optionally excluding changes from a specific node (watermarking)."""
        query = (
            select(ChangeLog)
            .where(ChangeLog.id > since_id)
            .order_by(ChangeLog.id.asc())
            .limit(limit)
        )
        # Filter out changes from the specified node (watermarking on pull)
        # Include both NULL node_id (legacy) and any node_id that's not the excluded one
        if exclude_node_id:
            from sqlalchemy import or_

            query = query.where(
                or_(
                    ChangeLog.node_id.is_(
                        None
                    ),  # Include changes with no node_id (legacy)
                    ChangeLog.node_id
                    != exclude_node_id,  # Include changes from other nodes
                )
            )
        rows = self.sess.exec(query).all()
        return [self._serialize_change(r) for r in rows]

    def _apply_one(self, cp: ChangePayload):
        ts = self.schema[cp["table"]]
        model = ts.model
        obj = self.sess.get(model, cp["pk"])
        if cp["op"] == "D":
            if obj:
                self.sess.delete(obj)
            return
        incoming_version = cp["version"]
        if obj is None:
            obj = model(id=cp["pk"])  # type: ignore
            self.sess.add(obj)
            if cp["data"]:
                apply_row(obj, cp["data"])
            else:
                # Data is missing - this shouldn't happen for I/U operations
                import sys

                print(
                    f"WARNING: No data for {cp['table']} {cp['pk']} operation {cp['op']}",
                    file=sys.stderr,
                )
            setattr(obj, "version", incoming_version)
            # Flush immediately after creating new object so it's tracked in the session
            self.sess.flush()
            return
        current_version = getattr(obj, "version", 1)
        if (
            self.policy == ConflictPolicy.VERSION
            and incoming_version <= current_version
        ):
            return
        if cp["data"]:
            apply_row(obj, cp["data"])
        setattr(obj, "version", max(current_version, incoming_version))

    def apply_remote_changes(self, changes: Iterable[ChangePayload]):
        by_table: Dict[str, List[ChangePayload]] = defaultdict(list)
        for c in changes:
            by_table[c["table"]].append(c)
        for table in self.order:
            for c in by_table.get(table, []):
                self._apply_one(c)
        # Flush after all changes in a batch to ensure new objects are tracked
        self.sess.flush()

    def _ensure_state(self) -> SyncState:
        st = self.sess.get(SyncState, self.peer_id)
        if not st:
            st = SyncState(
                peer_id=self.peer_id, last_pushed_change_id=0, last_pulled_change_id=0
            )
            self.sess.add(st)
            self.sess.commit()
        return st

    def pull_then_push(self, peer_transport, batch: int = 1000) -> Tuple[int, int]:
        # Set the current node_id for change logging
        set_current_node_id(self.node_id)
        try:
            st = self._ensure_state()
            pulled = 0
            while True:
                remote_changes = peer_transport.get_changes_since(
                    st.last_pulled_change_id, limit=batch, exclude_node_id=self.node_id
                )
                if not remote_changes:
                    break
                self.apply_remote_changes(remote_changes)
                self.sess.commit()
                st.last_pulled_change_id = remote_changes[-1]["id"]
                self.sess.add(st)
                self.sess.commit()
                peer_transport.ack(st.last_pulled_change_id)
                pulled += len(remote_changes)
            pushed = 0
            while True:
                out = self.local_changes_since(st.last_pushed_change_id, limit=batch)
                if not out:
                    break
                peer_transport.apply_changes(out)
                st.last_pushed_change_id = out[-1]["id"]
                self.sess.add(st)
                self.sess.commit()
                pushed += len(out)
            return pulled, pushed
        finally:
            # Clear the node_id context
            set_current_node_id(None)
