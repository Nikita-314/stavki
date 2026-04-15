from __future__ import annotations

from decimal import Decimal

from sqlalchemy import Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.mixins import IntIdMixin, TimestampMixin


class BalanceSnapshot(Base, IntIdMixin, TimestampMixin):
    __tablename__ = "balance_snapshots"

    base_amount: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    label: Mapped[str | None] = mapped_column(String(), nullable=True)

