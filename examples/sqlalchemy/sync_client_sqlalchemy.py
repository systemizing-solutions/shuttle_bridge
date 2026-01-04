import random

from sqlmodel import SQLModel

from sqlalchemy import select
from sqlalchemy.orm import Session

from data_shuttle_bridge import (
    SyncEngine,
    ConflictPolicy,
    ClientNodeManager,
    attach_change_hooks_for_models,
    build_schema,
    HttpPeerTransport,
    set_id_generator,
    ChangeLog,
    SyncState,
)

from db import init_engine, get_engine, make_session
from models_sqlalchemy import Base, Customer, Order

DB_URL = "sqlite:///local_client_sqlalchemy.db"
SERVER_BASE_URL = "http://127.0.0.1:5002"

init_engine(DB_URL)
# Create both custom tables and data_shuttle_bridge tables
Base.metadata.create_all(get_engine())
SQLModel.metadata.create_all(get_engine())

mgr = ClientNodeManager()
node_id = mgr.ensure_node_id(SERVER_BASE_URL)

# Set up the global ID generator with this client's node_id
set_id_generator(node_id)

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
            node_id=str(node_id),  # THIS IS CRITICAL - enables watermarking
        )
        remote = HttpPeerTransport(SERVER_BASE_URL)

        # Pull from server first to get any existing data
        pulled, pushed = engine.pull_then_push(remote)
        print(f"Sync finished. Pulled: {pulled}, Pushed: {pushed}")

        # Then check if we have customers, if not create one
        # Use exec() for pure SQLAlchemy models with declarative_base
        result = sess.exec(select(Customer))
        customer = result.scalars().first()
        if not customer:
            customer = Customer(name="John Doe", email="john@example.com")
            sess.add(customer)
            sess.commit()
            print(f"Created customer: {customer.name} (ID: {customer.id})")

        # Always add a new order with random values
        statuses = ["new", "pending", "shipped", "delivered", "cancelled"]
        order = Order(
            customer_id=customer.id,
            status=random.choice(statuses),
            total_cents=random.randint(100, 100000),
        )
        sess.add(order)
        sess.commit()
        print(
            f"Created order (ID: {order.id}) for customer {customer.name} - Status: {order.status}, Total: ${order.total_cents/100:.2f}"
        )

        # Sync again to push the new order
        pulled, pushed = engine.pull_then_push(remote)
        print(f"Second sync finished. Pulled: {pulled}, Pushed: {pushed}")


if __name__ == "__main__":
    main()
