"""Create land_use_stat table"""

from alembic import op
import sqlalchemy as sa


revision = "0005_land_use_stat"
down_revision = "0004_external_features"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "land_use_stat",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("city", sa.String(length=64), nullable=False),
        sa.Column("sub_municipality", sa.String(length=128)),
        sa.Column("category", sa.String(length=128)),
        sa.Column("metric", sa.String(length=64)),
        sa.Column("unit", sa.String(length=32)),
        sa.Column("value", sa.Numeric(18, 4)),
        sa.Column("source_url", sa.String(length=512)),
    )


def downgrade():
    op.drop_table("land_use_stat")
