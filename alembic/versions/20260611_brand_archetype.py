"""Add ``brand_archetype`` to ``expansion_brand_profile``.

Brand archetypes decouple the weight-profile choice from service_model:
``delivery_led | street_flagship | neighborhood_local | balanced``. The
API resolves the archetype server-side (explicit user value, else legacy
non-default ``expansion_goal``, else seeded from ``service_model``) and
persists the RESOLVED value here so prewarm / DB-read memo paths agree
with the search-time scoring path (findings §1.4.5).

Nullable, no backfill: legacy rows resolve at read time via the same
resolution helper (``app.services.expansion_advisor.resolve_brand_archetype``).

Revision ID: 20260611_brand_archetype
Revises: 20260526_cl_rega_license
Create Date: 2026-06-11
"""

from alembic import op
import sqlalchemy as sa


revision = "20260611_brand_archetype"
down_revision = "20260526_cl_rega_license"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "expansion_brand_profile",
        sa.Column("brand_archetype", sa.String(length=32), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("expansion_brand_profile", "brand_archetype")
