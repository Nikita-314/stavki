from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Enum as SAEnum, JSON, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.enums import SportType
from app.db.base import Base
from app.db.models.mixins import IntIdMixin, TimestampMixin


class ModelVersion(Base, IntIdMixin, TimestampMixin):
    __tablename__ = "model_versions"

    sport: Mapped[SportType] = mapped_column(
        SAEnum(SportType, name="sport_type"),
        nullable=False,
    )

    model_key: Mapped[str] = mapped_column(nullable=False)
    version_name: Mapped[str] = mapped_column(nullable=False)

    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )

    training_dataset_name: Mapped[str | None] = mapped_column(nullable=True)
    metrics_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    training_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    training_finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    deployed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

