"""expansion advisor v1 branches cannibalization compare

Revision ID: 20260310_exp_adv_v1
Revises: 20260310_exp_adv_v0
Create Date: 2026-03-10
"""

from alembic import op
import sqlalchemy as sa


revision = "20260310_exp_adv_v1"
down_revision = "20260310_exp_adv_v0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "expansion_branch",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "search_id",
            sa.String(length=36),
            sa.ForeignKey("expansion_search.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=256), nullable=True),
        sa.Column("lat", sa.Numeric(10, 7), nullable=False),
        sa.Column("lon", sa.Numeric(10, 7), nullable=False),
        sa.Column("district", sa.String(length=128), nullable=True),
        sa.Column("source", sa.String(length=32), nullable=False, server_default="manual"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_expansion_branch_search_id", "expansion_branch", ["search_id"])

    op.add_column("expansion_candidate", sa.Column("district", sa.String(length=128), nullable=True))
    op.add_column("expansion_candidate", sa.Column("cannibalization_score", sa.Numeric(6, 2), nullable=True))
    op.add_column(
        "expansion_candidate",
        sa.Column("distance_to_nearest_branch_m", sa.Numeric(12, 2), nullable=True),
    )
    op.add_column("expansion_candidate", sa.Column("compare_rank", sa.Integer(), nullable=True))

    op.create_index(
        "ix_expansion_candidate_search_id_compare_rank",
        "expansion_candidate",
        ["search_id", "compare_rank"],
    )
    op.create_index(
        "ix_expansion_candidate_search_id_district",
        "expansion_candidate",
        ["search_id", "district"],
    )


def downgrade() -> None:
    op.drop_index("ix_expansion_candidate_search_id_district", table_name="expansion_candidate")
    op.drop_index("ix_expansion_candidate_search_id_compare_rank", table_name="expansion_candidate")
    op.drop_column("expansion_candidate", "compare_rank")
    op.drop_column("expansion_candidate", "distance_to_nearest_branch_m")
    op.drop_column("expansion_candidate", "cannibalization_score")
    op.drop_column("expansion_candidate", "district")

    op.drop_index("ix_expansion_branch_search_id", table_name="expansion_branch")
    op.drop_table("expansion_branch")
