"""Initial tables for signals analytics.

Revision ID: 0001_initial_tables
Revises: 
Create Date: 2026-04-15
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0001_initial_tables"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enums
    sport_type = postgresql.ENUM("CS2", "DOTA2", "FOOTBALL", name="sport_type")
    bookmaker_type = postgresql.ENUM("FONBET", "WINLINE", "BETBOOM", name="bookmaker_type")
    signal_status = postgresql.ENUM("NEW", "SENT", "ENTERED", "MISSED", "SETTLED", "CANCELED", name="signal_status")
    entry_status = postgresql.ENUM("PENDING", "ENTERED", "SKIPPED", "REJECTED", name="entry_status")
    bet_result = postgresql.ENUM("WIN", "LOSE", "VOID", "UNKNOWN", name="bet_result")
    failure_category = postgresql.ENUM(
        "MODEL_ERROR",
        "EXECUTION_ERROR",
        "MARKET_UNAVAILABLE",
        "LINE_MOVEMENT",
        "VARIANCE",
        "DATA_ISSUE",
        "UNKNOWN",
        name="failure_category",
    )

    bind = op.get_bind()
    sport_type.create(bind, checkfirst=True)
    bookmaker_type.create(bind, checkfirst=True)
    signal_status.create(bind, checkfirst=True)
    entry_status.create(bind, checkfirst=True)
    bet_result.create(bind, checkfirst=True)
    failure_category.create(bind, checkfirst=True)

    # Tables
    op.create_table(
        "model_versions",
        sa.Column("sport", sa.Enum("CS2", "DOTA2", "FOOTBALL", name="sport_type"), nullable=False),
        sa.Column("model_key", sa.String(), nullable=False),
        sa.Column("version_name", sa.String(), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("training_dataset_name", sa.String(), nullable=True),
        sa.Column("metrics_json", sa.JSON(), nullable=True),
        sa.Column("training_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("training_finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deployed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_model_versions")),
    )

    op.create_table(
        "signals",
        sa.Column("sport", sa.Enum("CS2", "DOTA2", "FOOTBALL", name="sport_type"), nullable=False),
        sa.Column("bookmaker", sa.Enum("FONBET", "WINLINE", "BETBOOM", name="bookmaker_type"), nullable=False),
        sa.Column("event_external_id", sa.String(), nullable=True),
        sa.Column("tournament_name", sa.String(), nullable=False),
        sa.Column("match_name", sa.String(), nullable=False),
        sa.Column("home_team", sa.String(), nullable=False),
        sa.Column("away_team", sa.String(), nullable=False),
        sa.Column("market_type", sa.String(), nullable=False),
        sa.Column("market_label", sa.String(), nullable=False),
        sa.Column("selection", sa.String(), nullable=False),
        sa.Column("odds_at_signal", sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column("min_entry_odds", sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column("predicted_prob", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("implied_prob", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("edge", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("model_name", sa.String(), nullable=True),
        sa.Column("model_version_name", sa.String(), nullable=True),
        sa.Column("signal_score", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column(
            "status",
            sa.Enum("NEW", "SENT", "ENTERED", "MISSED", "SETTLED", "CANCELED", name="signal_status"),
            server_default=sa.text("'NEW'"),
            nullable=False,
        ),
        sa.Column("section_name", sa.String(), nullable=True),
        sa.Column("subsection_name", sa.String(), nullable=True),
        sa.Column("search_hint", sa.String(), nullable=True),
        sa.Column("is_live", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("event_start_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("signaled_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_signals")),
    )

    op.create_table(
        "entries",
        sa.Column("signal_id", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.Enum("PENDING", "ENTERED", "SKIPPED", "REJECTED", name="entry_status"),
            server_default=sa.text("'PENDING'"),
            nullable=False,
        ),
        sa.Column("entered_odds", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("stake_amount", sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column("entered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_manual", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("was_found_in_bookmaker", sa.Boolean(), nullable=True),
        sa.Column("missed_reason", sa.String(), nullable=True),
        sa.Column("delay_seconds", sa.Integer(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["signal_id"], ["signals.id"], ondelete="CASCADE", name=op.f("fk_entries_signal_id_signals")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_entries")),
    )

    op.create_table(
        "failure_reviews",
        sa.Column("signal_id", sa.Integer(), nullable=False),
        sa.Column(
            "category",
            sa.Enum(
                "MODEL_ERROR",
                "EXECUTION_ERROR",
                "MARKET_UNAVAILABLE",
                "LINE_MOVEMENT",
                "VARIANCE",
                "DATA_ISSUE",
                "UNKNOWN",
                name="failure_category",
            ),
            server_default=sa.text("'UNKNOWN'"),
            nullable=False,
        ),
        sa.Column("auto_reason", sa.String(), nullable=True),
        sa.Column("manual_reason", sa.String(), nullable=True),
        sa.Column("failure_tags_json", sa.JSON(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["signal_id"], ["signals.id"], ondelete="CASCADE", name=op.f("fk_failure_reviews_signal_id_signals")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_failure_reviews")),
    )

    op.create_table(
        "prediction_logs",
        sa.Column("signal_id", sa.Integer(), nullable=False),
        sa.Column("feature_snapshot_json", sa.JSON(), nullable=False),
        sa.Column("raw_model_output_json", sa.JSON(), nullable=True),
        sa.Column("explanation_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["signal_id"], ["signals.id"], ondelete="CASCADE", name=op.f("fk_prediction_logs_signal_id_signals")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_prediction_logs")),
    )

    op.create_table(
        "settlements",
        sa.Column("signal_id", sa.Integer(), nullable=False),
        sa.Column(
            "result",
            sa.Enum("WIN", "LOSE", "VOID", "UNKNOWN", name="bet_result"),
            server_default=sa.text("'UNKNOWN'"),
            nullable=False,
        ),
        sa.Column("profit_loss", sa.Numeric(precision=12, scale=2), server_default=sa.text("0"), nullable=False),
        sa.Column("settled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result_details", sa.Text(), nullable=True),
        sa.Column("bankroll_before", sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column("bankroll_after", sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["signal_id"], ["signals.id"], ondelete="CASCADE", name=op.f("fk_settlements_signal_id_signals")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_settlements")),
        sa.UniqueConstraint("signal_id", name=op.f("uq_settlements_signal_id")),
    )


def downgrade() -> None:
    op.drop_table("settlements")
    op.drop_table("prediction_logs")
    op.drop_table("failure_reviews")
    op.drop_table("entries")
    op.drop_table("signals")
    op.drop_table("model_versions")

    bind = op.get_bind()
    postgresql.ENUM(name="failure_category").drop(bind, checkfirst=True)
    postgresql.ENUM(name="bet_result").drop(bind, checkfirst=True)
    postgresql.ENUM(name="entry_status").drop(bind, checkfirst=True)
    postgresql.ENUM(name="signal_status").drop(bind, checkfirst=True)
    postgresql.ENUM(name="bookmaker_type").drop(bind, checkfirst=True)
    postgresql.ENUM(name="sport_type").drop(bind, checkfirst=True)

