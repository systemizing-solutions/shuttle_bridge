from typing import Optional, List

from sqlmodel import SQLModel, Field, Relationship, Column as SQLModelColumn
from sqlalchemy import String, Integer, ForeignKey

from shuttle_bridge.mixins import SyncRowSQLModelMixin


class Customer(SyncRowSQLModelMixin, SQLModel, table=True):
    __tablename__ = "customers"
    # id is automatically provided by SyncRowSQLModelMixin with auto-generation!
    name: str = Field(
        sa_column=SQLModelColumn(
            String(255),
            nullable=False,
        )
    )
    email: Optional[str] = Field(
        sa_column=SQLModelColumn(
            String(255),
            default=None,
            nullable=True,
        )
    )
    orders: List["Order"] = Relationship(back_populates="customer")


class Order(SyncRowSQLModelMixin, SQLModel, table=True):
    __tablename__ = "orders"
    # id is automatically provided by SyncRowSQLModelMixin with auto-generation!
    customer_id: int = Field(
        sa_column=SQLModelColumn(
            ForeignKey("customers.id", ondelete="CASCADE"),
            nullable=False,
        ),
    )
    status: str = Field(
        sa_column=SQLModelColumn(
            String(50),
            default="new",
        )
    )
    total_cents: int = Field(
        sa_column=SQLModelColumn(
            Integer,
            default=0,
        )
    )
    customer: Optional[Customer] = Relationship(back_populates="orders")
