"""Add football_live_probability_ideas table.

Revision ID: 0004_prob_ideas
Revises: 0003_football_live_ranker_ideas
Create Date: 2026-04-26
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0004_prob_ideas"
down_revision = "0003_football_live_ranker_ideas"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "football_live_probability_ideas",
        sa.Column("event_id", sa.String(), nullable=False),
        sa.Column("fixture_id", sa.Integer(), nullable=True),
        sa.Column("match_name", sa.String(), nullable=False),
        sa.Column("home_team", sa.String(), nullable=True),
        sa.Column("away_team", sa.String(), nullable=True),
        sa.Column("minute", sa.Integer(), nullable=True),
        sa.Column("score_home", sa.Integer(), nullable=True),
        sa.Column("score_away", sa.Integer(), nullable=True),
        sa.Column("market", sa.String(), nullable=False),
        sa.Column("selection", sa.String(), nullable=False),
        sa.Column("line", sa.Numeric(precision=10, scale=3), nullable=True),
        sa.Column("odds", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("implied_probability", sa.Numeric(precision=10, scale=6), nullable=True),
        sa.Column("model_probability", sa.Numeric(precision=10, scale=6), nullable=True),
        sa.Column("value_edge", sa.Numeric(precision=10, scale=6), nullable=True),
        sa.Column("confidence_score", sa.Integer(), nullable=True),
        sa.Column("risk_level", sa.String(), nullable=False),
        sa.Column("api_intelligence_available", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("reasons_json", sa.JSON(), nullable=True),
        sa.Column("missing_data_json", sa.JSON(), nullable=True),
        sa.Column("result", sa.String(), nullable=True),
        sa.Column("profit_loss", sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column("settled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("final_score_home", sa.Integer(), nullable=True),
        sa.Column("final_score_away", sa.Integer(), nullable=True),
        sa.Column("settlement_note", sa.Text(), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_football_live_probability_ideas")),
    )
    op.create_index(
        op.f("ix_football_live_probability_ideas_event_id"),
        "football_live_probability_ideas",
        ["event_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_football_live_probability_ideas_result"),
        "football_live_probability_ideas",
        ["result"],
        unique=False,
    )
    op.create_index(
        op.f("ix_football_live_probability_ideas_market"),
        "football_live_probability_ideas",
        ["market"],
        unique=False,
    )
    op.create_index(
        op.f("ix_football_live_probability_ideas_created_at"),
        "football_live_probability_ideas",
        ["created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_football_live_probability_ideas_created_at"), table_name="football_live_probability_ideas")
    op.drop_index(op.f("ix_football_live_probability_ideas_market"), table_name="football_live_probability_ideas")
    op.drop_index(op.f("ix_football_live_probability_ideas_result"), table_name="football_live_probability_ideas")
    op.drop_index(op.f("ix_football_live_probability_ideas_event_id"), table_name="football_live_probability_ideas")
    op.drop_table("football_live_probability_ideas")
