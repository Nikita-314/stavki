from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, Enum as SAEnum, Numeric, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.enums import BookmakerType, SignalStatus, SportType
from app.db.base import Base
from app.db.models.mixins import IntIdMixin, TimestampMixin

if TYPE_CHECKING:
    from app.db.models.entry import Entry
    from app.db.models.failure_review import FailureReview
    from app.db.models.prediction_log import PredictionLog
    from app.db.models.settlement import Settlement


class Signal(Base, IntIdMixin, TimestampMixin):
    __tablename__ = "signals"

    sport: Mapped[SportType] = mapped_column(
        SAEnum(SportType, name="sport_type"),
        nullable=False,
    )
    bookmaker: Mapped[BookmakerType] = mapped_column(
        SAEnum(BookmakerType, name="bookmaker_type"),
        nullable=False,
    )

    event_external_id: Mapped[str | None] = mapped_column(nullable=True)
    tournament_name: Mapped[str] = mapped_column(nullable=False)
    match_name: Mapped[str] = mapped_column(nullable=False)
    home_team: Mapped[str] = mapped_column(nullable=False)
    away_team: Mapped[str] = mapped_column(nullable=False)

    market_type: Mapped[str] = mapped_column(nullable=False)
    market_label: Mapped[str] = mapped_column(nullable=False)
    selection: Mapped[str] = mapped_column(nullable=False)

    odds_at_signal: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    min_entry_odds: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)

    predicted_prob: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    implied_prob: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    edge: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)

    model_name: Mapped[str | None] = mapped_column(nullable=True)
    model_version_name: Mapped[str | None] = mapped_column(nullable=True)
    signal_score: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)

    status: Mapped[SignalStatus] = mapped_column(
        SAEnum(SignalStatus, name="signal_status"),
        nullable=False,
        default=SignalStatus.NEW,
        server_default=SignalStatus.NEW.value,
    )

    section_name: Mapped[str | None] = mapped_column(nullable=True)
    subsection_name: Mapped[str | None] = mapped_column(nullable=True)
    search_hint: Mapped[str | None] = mapped_column(nullable=True)

    is_live: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )

    event_start_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    signaled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    entries: Mapped[list["Entry"]] = relationship(
        back_populates="signal",
        cascade="all, delete-orphan",
    )
    settlement: Mapped["Settlement | None"] = relationship(
        back_populates="signal",
        cascade="all, delete-orphan",
        uselist=False,
    )
    failure_reviews: Mapped[list["FailureReview"]] = relationship(
        back_populates="signal",
        cascade="all, delete-orphan",
    )
    prediction_logs: Mapped[list["PredictionLog"]] = relationship(
        back_populates="signal",
        cascade="all, delete-orphan",
    )

