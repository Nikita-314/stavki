"""Add football_live_ranker_ideas table.

Revision ID: 0003_football_live_ranker_ideas
Revises: 0002_balance_snapshots
Create Date: 2026-04-26
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0003_football_live_ranker_ideas"
down_revision = "0002_balance_snapshots"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "football_live_ranker_ideas",
        sa.Column("preview_run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_id", sa.String(), nullable=False),
        sa.Column("fixture_id", sa.Integer(), nullable=True),
        sa.Column("event_start_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.Column("goals_needed_to_win", sa.Integer(), nullable=True),
        sa.Column("team_side", sa.String(), nullable=True),
        sa.Column("selection_side", sa.String(), nullable=True),
        sa.Column("bucket", sa.String(), nullable=False),
        sa.Column("risk_level", sa.String(), nullable=False),
        sa.Column("api_intelligence_available", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("result", sa.String(), nullable=True),
        sa.Column("profit_loss", sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column("settled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("final_score_home", sa.Integer(), nullable=True),
        sa.Column("final_score_away", sa.Integer(), nullable=True),
        sa.Column("result_payload_json", sa.JSON(), nullable=True),
        sa.Column("settlement_note", sa.Text(), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_football_live_ranker_ideas")),
    )
    op.create_index(op.f("ix_football_live_ranker_ideas_event_id"), "football_live_ranker_ideas", ["event_id"], unique=False)
    op.create_index(op.f("ix_football_live_ranker_ideas_preview_run_at"), "football_live_ranker_ideas", ["preview_run_at"], unique=False)
    op.create_index(op.f("ix_football_live_ranker_ideas_result"), "football_live_ranker_ideas", ["result"], unique=False)
    op.create_index(op.f("ix_football_live_ranker_ideas_bucket"), "football_live_ranker_ideas", ["bucket"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_football_live_ranker_ideas_bucket"), table_name="football_live_ranker_ideas")
    op.drop_index(op.f("ix_football_live_ranker_ideas_result"), table_name="football_live_ranker_ideas")
    op.drop_index(op.f("ix_football_live_ranker_ideas_preview_run_at"), table_name="football_live_ranker_ideas")
    op.drop_index(op.f("ix_football_live_ranker_ideas_event_id"), table_name="football_live_ranker_ideas")
    op.drop_table("football_live_ranker_ideas")
