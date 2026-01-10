"""merge alembic heads

Revision ID: 4cff77cdbd28
Revises: d2c3b4a5e6f7, e2b7c9d4a1f0
Create Date: 2026-01-10 14:08:50.754718

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4cff77cdbd28'
down_revision: Union[str, Sequence[str], None] = ('d2c3b4a5e6f7', 'e2b7c9d4a1f0')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
