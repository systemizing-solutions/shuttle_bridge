from flask import Blueprint, request, jsonify

from data_shuttle_bridge.sql.typing_ import ChangePayload
from data_shuttle_bridge.sql.sync import SyncEngine


def sync_blueprint(engine_factory):
    bp = Blueprint("sync", __name__)

    @bp.get("/sync/changes")
    def get_changes():
        eng: SyncEngine = engine_factory()
        since_id = int(request.args.get("since_id", "0"))
        limit = int(request.args.get("limit", "1000"))
        exclude_node_id = request.args.get("exclude_node_id")
        changes = eng.remote_changes_since(
            since_id,
            limit=limit,
            exclude_node_id=exclude_node_id,
        )
        return jsonify({"changes": changes})

    @bp.post("/sync/apply")
    def apply():
        eng: SyncEngine = engine_factory()
        payload = request.get_json(force=True) or {}
        changes: list[ChangePayload] = payload.get("changes", [])
        eng.apply_remote_changes(changes)
        eng.sess.commit()
        return jsonify({"ok": True})

    @bp.post("/sync/ack")
    def ack():
        return jsonify({"ok": True})

    return bp
