from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, ForeignKey, JSON, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.db.models.mixins import IntIdMixin

if TYPE_CHECKING:
    from app.db.models.signal import Signal


class PredictionLog(Base, IntIdMixin):
    __tablename__ = "prediction_logs"

    signal_id: Mapped[int] = mapped_column(ForeignKey("signals.id", ondelete="CASCADE"), nullable=False)

    feature_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    raw_model_output_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    explanation_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    signal: Mapped["Signal"] = relationship(back_populates="prediction_logs")

