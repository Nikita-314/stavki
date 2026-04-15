from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Enum as SAEnum, ForeignKey, Numeric, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.enums import BetResult
from app.db.base import Base
from app.db.models.mixins import IntIdMixin, TimestampMixin

if TYPE_CHECKING:
    from app.db.models.signal import Signal


class Settlement(Base, IntIdMixin, TimestampMixin):
    __tablename__ = "settlements"

    signal_id: Mapped[int] = mapped_column(
        ForeignKey("signals.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )

    result: Mapped[BetResult] = mapped_column(
        SAEnum(BetResult, name="bet_result"),
        nullable=False,
        default=BetResult.UNKNOWN,
        server_default=BetResult.UNKNOWN.value,
    )

    profit_loss: Mapped[Decimal] = mapped_column(
        Numeric(12, 2),
        nullable=False,
        default=Decimal("0"),
        server_default="0",
    )

    settled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    result_details: Mapped[str | None] = mapped_column(Text, nullable=True)

    bankroll_before: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    bankroll_after: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)

    signal: Mapped["Signal"] = relationship(back_populates="settlement")

