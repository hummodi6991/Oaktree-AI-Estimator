"""merge alembic heads after expansion advisor fixes

Revision ID: e7f8a9b0c1d2
Revises: 20260314_exp_adv_v61_outputs, d4e5f6a1b2c3
Create Date: 2026-03-15
"""

from alembic import op

revision = "e7f8a9b0c1d2"
down_revision = ("20260314_exp_adv_v61_outputs", "d4e5f6a1b2c3")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
