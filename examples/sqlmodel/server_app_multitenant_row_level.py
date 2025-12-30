# Row-level tenancy (shared DB with tenant columns)

from flask import Flask, request, g
from sqlmodel import SQLModel, create_engine, Session
from sqlalchemy.orm import sessionmaker

from shuttle_bridge import tenant_sync_blueprint_row_level, ChangeLogMT, SyncStateMT

from models_sqlmodel import Customer, Order

app = Flask(__name__)

engine = create_engine("sqlite:///remote_mt_single.db", future=True)
SQLModel.metadata.create_all(
    engine
)  # creates your app tables + ChangeLogMT/SyncStateMT once imported
SessionLocal = sessionmaker(bind=engine, class_=Session, expire_on_commit=False)


def session_factory() -> Session:
    return SessionLocal()


def tenant_resolver() -> str:
    return request.args.get("tenant") or getattr(g, "tenant", "default")


bp = tenant_sync_blueprint_row_level(
    session_factory=session_factory,
    models=[Customer, Order],
    tenant_resolver=tenant_resolver,
)
app.register_blueprint(bp)

if __name__ == "__main__":
    app.run(debug=True, port=5004)
