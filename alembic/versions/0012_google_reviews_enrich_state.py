"""add google_reviews_enrich_state table for resumable enrichment

Revision ID: 0012_google_reviews_enrich_state
Revises: 0011_google_reviews_columns
Create Date: 2026-03-05
"""

from alembic import op
import sqlalchemy as sa


revision = "0012_google_reviews_enrich_state"
down_revision = "0011_google_reviews_columns"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "google_reviews_enrich_state",
        sa.Column("id", sa.Integer, primary_key=True, server_default="1"),
        sa.Column("last_cursor", sa.Text, nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    # Seed the singleton row
    op.execute(
        "INSERT INTO google_reviews_enrich_state (id, last_cursor) "
        "VALUES (1, NULL) ON CONFLICT (id) DO NOTHING"
    )


def downgrade() -> None:
    op.drop_table("google_reviews_enrich_state")
