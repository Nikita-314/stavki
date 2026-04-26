from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import JSON, Boolean, DateTime, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.mixins import IntIdMixin, TimestampMixin


class FootballLiveProbabilityIdea(Base, IntIdMixin, TimestampMixin):
    __tablename__ = "football_live_probability_ideas"

    event_id: Mapped[str] = mapped_column(String, nullable=False)
    fixture_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    match_name: Mapped[str] = mapped_column(String, nullable=False)
    home_team: Mapped[str | None] = mapped_column(String, nullable=True)
    away_team: Mapped[str | None] = mapped_column(String, nullable=True)
    minute: Mapped[int | None] = mapped_column(Integer, nullable=True)
    score_home: Mapped[int | None] = mapped_column(Integer, nullable=True)
    score_away: Mapped[int | None] = mapped_column(Integer, nullable=True)

    market: Mapped[str] = mapped_column(String, nullable=False)  # 1x2 | match_total_over | team_total_over
    selection: Mapped[str] = mapped_column(String, nullable=False)
    line: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), nullable=True)
    odds: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)

    implied_probability: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    model_probability: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    value_edge: Mapped[Decimal | None] = mapped_column(Numeric(10, 6), nullable=True)
    confidence_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    risk_level: Mapped[str] = mapped_column(String, nullable=False)
    api_intelligence_available: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )
    reasons_json: Mapped[list | None] = mapped_column(JSON, nullable=True)
    missing_data_json: Mapped[list | None] = mapped_column(JSON, nullable=True)

    result: Mapped[str | None] = mapped_column(String, nullable=True)  # WIN | LOSE | VOID | UNKNOWN
    profit_loss: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    settled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    final_score_home: Mapped[int | None] = mapped_column(Integer, nullable=True)
    final_score_away: Mapped[int | None] = mapped_column(Integer, nullable=True)
    settlement_note: Mapped[str | None] = mapped_column(Text, nullable=True)
