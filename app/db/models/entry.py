from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, Enum as SAEnum, ForeignKey, Numeric, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.enums import EntryStatus
from app.db.base import Base
from app.db.models.mixins import IntIdMixin, TimestampMixin

if TYPE_CHECKING:
    from app.db.models.signal import Signal


class Entry(Base, IntIdMixin, TimestampMixin):
    __tablename__ = "entries"

    signal_id: Mapped[int] = mapped_column(ForeignKey("signals.id", ondelete="CASCADE"), nullable=False)

    status: Mapped[EntryStatus] = mapped_column(
        SAEnum(EntryStatus, name="entry_status"),
        nullable=False,
        default=EntryStatus.PENDING,
        server_default=EntryStatus.PENDING.value,
    )

    entered_odds: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    stake_amount: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    entered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    is_manual: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default="true",
    )

    was_found_in_bookmaker: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    missed_reason: Mapped[str | None] = mapped_column(nullable=True)
    delay_seconds: Mapped[int | None] = mapped_column(nullable=True)

    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    signal: Mapped["Signal"] = relationship(back_populates="entries")

