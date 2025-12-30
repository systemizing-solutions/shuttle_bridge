from sqlalchemy.orm import Session

from shuttle_bridge import (
    SyncEngine,
    ConflictPolicy,
    ClientNodeManager,
    attach_change_hooks_for_models,
    build_schema,
    HttpPeerTransport,
    KSortedID,
)

from db import init_engine, get_engine, make_session
from models_sqlalchemy import Base, Customer, Order, set_id_generator

DB_URL = "sqlite:///local_client_sqlalchemy.db"
SERVER_BASE_URL = "http://127.0.0.1:5002"

init_engine(DB_URL)
Base.metadata.create_all(get_engine())

mgr = ClientNodeManager()
node_id = mgr.ensure_node_id(SERVER_BASE_URL)

gen = KSortedID(node_id=node_id)
set_id_generator(lambda: gen())

models = [Customer, Order]
attach_change_hooks_for_models(models)
SCHEMA = build_schema(models)


def main():
    with make_session() as sess:
        engine = SyncEngine(
            session=sess,
            peer_id="remote-server",
            schema=SCHEMA,
            policy=ConflictPolicy.LWW,
        )
        remote = HttpPeerTransport(SERVER_BASE_URL)
        pulled, pushed = engine.pull_then_push(remote)
        print(f"Sync finished. Pulled: {pulled}, Pushed: {pushed}")


if __name__ == "__main__":
    main()
