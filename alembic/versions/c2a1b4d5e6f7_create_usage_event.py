"""create usage_event table

Revision ID: c2a1b4d5e6f7
Revises: b1d2c3e4f5a6
Create Date: 2025-02-07 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "c2a1b4d5e6f7"
down_revision = "4cff77cdbd28"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "usage_event",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("user_id", sa.Text(), nullable=True),
        sa.Column(
            "is_admin",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("event_name", sa.Text(), nullable=True),
        sa.Column("method", sa.Text(), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=False),
        sa.Column("estimate_id", sa.Text(), nullable=True),
        sa.Column("meta", postgresql.JSONB(), nullable=True),
        schema="public",
    )
    op.create_index(
        "ix_usage_event_ts",
        "usage_event",
        ["ts"],
        schema="public",
    )
    op.create_index(
        "ix_usage_event_user_ts",
        "usage_event",
        ["user_id", "ts"],
        schema="public",
    )
    op.create_index(
        "ix_usage_event_event_ts",
        "usage_event",
        ["event_name", "ts"],
        schema="public",
    )
    op.create_index(
        "ix_usage_event_path_ts",
        "usage_event",
        ["path", "ts"],
        schema="public",
    )


def downgrade() -> None:
    op.drop_index("ix_usage_event_path_ts", table_name="usage_event", schema="public")
    op.drop_index("ix_usage_event_event_ts", table_name="usage_event", schema="public")
    op.drop_index("ix_usage_event_user_ts", table_name="usage_event", schema="public")
    op.drop_index("ix_usage_event_ts", table_name="usage_event", schema="public")
    op.drop_table("usage_event", schema="public")
