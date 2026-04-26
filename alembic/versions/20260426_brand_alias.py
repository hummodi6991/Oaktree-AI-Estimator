"""Create brand_alias table for canonical chain mapping.

revision: 20260426_brand_alias
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic
revision = "20260426_brand_alias"
down_revision = "20260425_memo_prompt_version"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "brand_alias",
        sa.Column("alias_key", sa.String(256), primary_key=True),
        sa.Column("canonical_brand_id", sa.String(64), nullable=False),
        sa.Column("display_name_en", sa.String(256)),
        sa.Column("display_name_ar", sa.String(256)),
        sa.Column("notes", sa.Text),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_brand_alias_canonical_brand_id",
        "brand_alias",
        ["canonical_brand_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_brand_alias_canonical_brand_id", table_name="brand_alias")
    op.drop_table("brand_alias")
