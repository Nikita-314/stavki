from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Enum as SAEnum, ForeignKey, JSON, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.enums import FailureCategory
from app.db.base import Base
from app.db.models.mixins import IntIdMixin, TimestampMixin

if TYPE_CHECKING:
    from app.db.models.signal import Signal


class FailureReview(Base, IntIdMixin, TimestampMixin):
    __tablename__ = "failure_reviews"

    signal_id: Mapped[int] = mapped_column(ForeignKey("signals.id", ondelete="CASCADE"), nullable=False)

    category: Mapped[FailureCategory] = mapped_column(
        SAEnum(FailureCategory, name="failure_category"),
        nullable=False,
        default=FailureCategory.UNKNOWN,
        server_default=FailureCategory.UNKNOWN.value,
    )

    auto_reason: Mapped[str | None] = mapped_column(nullable=True)
    manual_reason: Mapped[str | None] = mapped_column(nullable=True)
    failure_tags_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    signal: Mapped["Signal"] = relationship(back_populates="failure_reviews")

