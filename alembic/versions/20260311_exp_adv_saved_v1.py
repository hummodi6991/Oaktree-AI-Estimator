"""expansion advisor saved searches v1

Revision ID: 20260311_exp_adv_saved_v1
Revises: 20260310_exp_adv_v2
Create Date: 2026-03-11
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "20260311_exp_adv_saved_v1"
down_revision = "20260310_exp_adv_v2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "expansion_saved_search",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("search_id", sa.String(length=36), sa.ForeignKey("expansion_search.id", ondelete="CASCADE"), nullable=False, unique=True),
        sa.Column("title", sa.String(length=256), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="draft"),
        sa.Column("selected_candidate_ids", JSONB, nullable=True),
        sa.Column("filters_json", JSONB, nullable=True),
        sa.Column("ui_state_json", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.execute("CREATE INDEX ix_exp_saved_search_created_at_desc ON expansion_saved_search (created_at DESC)")
    op.create_index("ix_exp_saved_search_status", "expansion_saved_search", ["status"])


def downgrade() -> None:
    op.drop_index("ix_exp_saved_search_status", table_name="expansion_saved_search")
    op.drop_index("ix_exp_saved_search_created_at_desc", table_name="expansion_saved_search")
    op.drop_table("expansion_saved_search")
