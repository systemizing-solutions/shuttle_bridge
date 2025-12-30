from flask import Flask, jsonify
from sqlmodel import select

from shuttle_bridge import (
    attach_change_hooks_for_models,
    SyncEngine,
    ConflictPolicy,
    sync_blueprint,
    node_registry_blueprint,
    build_schema,
    set_current_node_id,
    set_id_generator,
)

from db import init_engine, create_all, make_session
from models_sqlmodel import Customer, Order


app = Flask(__name__)

# Set up the global ID generator for the server
set_id_generator("server-node")

DB_URL = "sqlite:///remote_server_sqlmodel.db"
init_engine(DB_URL)
create_all()

models = [Customer, Order]
attach_change_hooks_for_models(models)
SCHEMA = build_schema(models)


def engine_factory():
    sess = make_session()
    return SyncEngine(
        session=sess,
        peer_id="client-peer",
        schema=SCHEMA,
        policy=ConflictPolicy.LWW,
        node_id="server-node",
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
        rows = s.exec(select(Customer)).all()
        return jsonify([{"id": r.id, "name": r.name, "email": r.email} for r in rows])


if __name__ == "__main__":
    app.run(debug=True, port=5001)
