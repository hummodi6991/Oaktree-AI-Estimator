"""merge heads

Revision ID: a9b6cdbd0831
Revises: b1d2c3e4f5a6, c2a1b4d5e6f7
Create Date: 2026-01-22 06:39:24.026138

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a9b6cdbd0831'
down_revision: Union[str, Sequence[str], None] = ('b1d2c3e4f5a6', 'c2a1b4d5e6f7')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
