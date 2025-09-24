"""initial"""

from alembic import op

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("-- placeholder; add tables in later PRs")


def downgrade() -> None:
    op.execute("-- placeholder")
