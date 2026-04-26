from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import JSON, Boolean, DateTime, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.mixins import IntIdMixin, TimestampMixin


class FootballLiveRankerIdea(Base, IntIdMixin, TimestampMixin):
    __tablename__ = "football_live_ranker_ideas"

    preview_run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event_id: Mapped[str] = mapped_column(String, nullable=False)
    fixture_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    event_start_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    match_name: Mapped[str] = mapped_column(String, nullable=False)
    home_team: Mapped[str | None] = mapped_column(String, nullable=True)
    away_team: Mapped[str | None] = mapped_column(String, nullable=True)
    minute: Mapped[int | None] = mapped_column(Integer, nullable=True)
    score_home: Mapped[int | None] = mapped_column(Integer, nullable=True)
    score_away: Mapped[int | None] = mapped_column(Integer, nullable=True)

    market: Mapped[str] = mapped_column(String, nullable=False)  # match_total_over | team_total_over | 1x2
    selection: Mapped[str] = mapped_column(String, nullable=False)
    line: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), nullable=True)
    odds: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    goals_needed_to_win: Mapped[int | None] = mapped_column(Integer, nullable=True)
    team_side: Mapped[str | None] = mapped_column(String, nullable=True)  # home | away
    selection_side: Mapped[str | None] = mapped_column(String, nullable=True)  # home | away | draw

    bucket: Mapped[str] = mapped_column(String, nullable=False)  # eligible | watchlist
    risk_level: Mapped[str] = mapped_column(String, nullable=False)  # low | medium | high
    api_intelligence_available: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )

    result: Mapped[str | None] = mapped_column(String, nullable=True)  # WIN | LOSE | VOID | UNKNOWN
    profit_loss: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
    settled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    final_score_home: Mapped[int | None] = mapped_column(Integer, nullable=True)
    final_score_away: Mapped[int | None] = mapped_column(Integer, nullable=True)
    result_payload_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    settlement_note: Mapped[str | None] = mapped_column(Text, nullable=True)
