from sqlmodel import SQLModel, create_engine, Session
from sqlalchemy.orm import sessionmaker

_engine = None
_SessionLocal = None


def init_engine(url: str):
    global _engine, _SessionLocal
    _engine = create_engine(url, echo=False, future=True)
    _SessionLocal = sessionmaker(
        bind=_engine,
        class_=Session,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )


def get_engine():
    assert _engine is not None, "Call init_engine first"
    return _engine


def make_session() -> Session:
    assert _SessionLocal is not None, "Call init_engine first"
    return _SessionLocal()


def create_all():
    SQLModel.metadata.create_all(get_engine())
