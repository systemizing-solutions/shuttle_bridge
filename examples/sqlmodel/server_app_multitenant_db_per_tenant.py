# Database-per-tenant

from flask import Flask, request, g
from sqlmodel import SQLModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from data_shuttle_bridge import (
    tenant_sync_blueprint_db_per_tenant,
    build_schema,
    attach_change_hooks_for_models,
)

from models_sqlmodel import Customer, Order

app = Flask(__name__)

# Pretend we have three per-tenant engines:
ENGINES = {
    "default": create_engine("sqlite:///remote_mt_default.db", future=True),
    "tenant_a": create_engine("sqlite:///remote_mt_tenant_a.db", future=True),
    "tenant_b": create_engine("sqlite:///remote_mt_tenant_b.db", future=True),
}
for eng in ENGINES.values():
    SQLModel.metadata.create_all(eng)


def session_factory_for_tenant(tenant: str) -> Session:
    engine = ENGINES.get(tenant) or ENGINES["default"]
    return sessionmaker(bind=engine, class_=Session, expire_on_commit=False)()


models = [Customer, Order]
attach_change_hooks_for_models(models)  # one-time per-process

bp = tenant_sync_blueprint_db_per_tenant(
    session_factory_for_tenant=session_factory_for_tenant,
    models=models,
    peer_id_namer=lambda t: f"client-peer:{t}",
)
app.register_blueprint(bp)


@app.before_request
def resolve_tenant():
    t = request.args.get("tenant")
    if t:
        g.tenant = t


if __name__ == "__main__":
    app.run(debug=True, port=5003)
