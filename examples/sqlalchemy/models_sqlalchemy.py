from typing import Optional

from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, String, Integer, ForeignKey

from data_shuttle_bridge.mixins import SyncRowSAMixin

Base = declarative_base()


class Customer(SyncRowSAMixin, Base):
    __tablename__ = "customers"
    # id is automatically provided by SyncRowSAMixin with auto-generation!
    name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=True)
    orders = relationship(
        "Order", back_populates="customer", cascade="all, delete-orphan"
    )


class Order(SyncRowSAMixin, Base):
    __tablename__ = "orders"
    # id is automatically provided by SyncRowSAMixin with auto-generation!
    customer_id = Column(
        Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=False
    )
    status = Column(String(50), nullable=False, default="new")
    total_cents = Column(Integer, nullable=False, default=0)
    customer = relationship("Customer", back_populates="orders")
