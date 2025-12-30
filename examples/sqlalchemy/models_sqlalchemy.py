from typing import Optional

from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, BigInteger, String, Integer, ForeignKey

from shuttle_bridge.mixins import SyncRowSAMixin
from shuttle_bridge.ids import KSortedID

Base = declarative_base()

_id_gen = KSortedID(node_id=0)


def next_id() -> int:
    return _id_gen()


def set_id_generator(gen_callable):
    global _id_gen, next_id
    _id_gen = gen_callable

    def _next():
        return _id_gen()

    next_id = _next


class Customer(SyncRowSAMixin, Base):
    __tablename__ = "customers"
    id = Column(BigInteger, primary_key=True, default=next_id)
    name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=True)
    orders = relationship(
        "Order", back_populates="customer", cascade="all, delete-orphan"
    )


class Order(SyncRowSAMixin, Base):
    __tablename__ = "orders"
    id = Column(BigInteger, primary_key=True, default=next_id)
    customer_id = Column(
        BigInteger, ForeignKey("customers.id", ondelete="CASCADE"), nullable=False
    )
    status = Column(String(50), nullable=False, default="new")
    total_cents = Column(Integer, nullable=False, default=0)
    customer = relationship("Customer", back_populates="orders")
