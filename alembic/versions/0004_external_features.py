"""external features table"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0004_external_features"
down_revision = "0003_market_ind_est"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "external_feature",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("layer_name", sa.String(length=128), nullable=False),
        sa.Column("feature_type", sa.String(length=16), nullable=False),
        sa.Column(
            "geometry",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "properties",
            postgresql.JSONB(astext_type=sa.Text()),
        ),
        sa.Column("source", sa.String(length=256)),
    )
    op.create_index(
        "ix_external_feature_layer",
        "external_feature",
        ["layer_name"],
    )


def downgrade():
    op.drop_index("ix_external_feature_layer", table_name="external_feature")
    op.drop_table("external_feature")
