"""merge cu/exp_adv and geography heads

Revision ID: 20260330_merge_cu_exp_adv
Revises: 20260330_merge_cu_cols_and_exp_adv, 20260322_ea_geog_gist
Create Date: 2026-03-30
"""

from alembic import op

revision = "20260330_merge_cu_exp_adv"
down_revision = ("20260330_merge_cu_cols_and_exp_adv", "20260322_ea_geog_gist")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
