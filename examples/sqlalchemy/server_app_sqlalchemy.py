from flask import Flask, jsonify

from sqlmodel import SQLModel

from sqlalchemy import select as sa_select
from sqlalchemy.orm import Session

from data_shuttle_bridge import (
    attach_change_hooks_for_models,
    SyncEngine,
    ConflictPolicy,
    sync_blueprint,
    node_registry_blueprint,
    build_schema,
    set_id_generator,
    set_current_node_id,
    ChangeLog,
    SyncState,
)

from db import init_engine, get_engine, make_session
from models_sqlalchemy import Base, Customer, Order

app = Flask(__name__)

# Set up the global ID generator for the server
set_id_generator("server-node")

DB_URL = "sqlite:///remote_server_sqlalchemy.db"
init_engine(DB_URL)
# Create both custom tables and data_shuttle_bridge tables
Base.metadata.create_all(get_engine())
SQLModel.metadata.create_all(get_engine())

models = [Customer, Order]
attach_change_hooks_for_models(models)
SCHEMA = build_schema(models)


def engine_factory():
    sess: Session = make_session()
    return SyncEngine(
        session=sess,
        peer_id="client-peer",
        schema=SCHEMA,
        policy=ConflictPolicy.LWW,
        node_id="server-node",  # Server's unique node identifier
    )


app.register_blueprint(sync_blueprint(engine_factory))
app.register_blueprint(node_registry_blueprint(make_session))


@app.post("/demo/seed")
def seed():
    set_current_node_id("server-node")
    try:
        with make_session() as s:
            c = Customer(name="Alice", email="alice@example.com")
            s.add(c)
            s.flush()
            s.add(Order(customer_id=c.id, status="new", total_cents=1299))
            s.commit()
        return jsonify({"ok": True})
    finally:
        set_current_node_id(None)


@app.get("/demo/customers")
def list_customers():
    with make_session() as s:
        rows = list(s.execute(sa_select(Customer)).scalars())
        return jsonify([{"id": r.id, "name": r.name, "email": r.email} for r in rows])


if __name__ == "__main__":
    app.run(debug=True, port=5002)
