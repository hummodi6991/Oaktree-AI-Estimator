"""delivery source record and ingest run tables

Revision ID: 0014_delivery_source_tables
Revises: 0013_restaurant_heatmap_cache
Create Date: 2026-03-08
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB


revision = "0014_delivery_source_tables"
down_revision = "0013_restaurant_heatmap_cache"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ---- delivery_ingest_run ----
    op.create_table(
        "delivery_ingest_run",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("platform", sa.String(32), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=text("now()"),
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column(
            "status",
            sa.String(16),
            nullable=False,
            server_default=text("'running'"),
        ),
        sa.Column("rows_scraped", sa.Integer, server_default=text("0")),
        sa.Column("rows_parsed", sa.Integer, server_default=text("0")),
        sa.Column("rows_inserted", sa.Integer, server_default=text("0")),
        sa.Column("rows_updated", sa.Integer, server_default=text("0")),
        sa.Column("rows_skipped", sa.Integer, server_default=text("0")),
        sa.Column("rows_matched", sa.Integer, server_default=text("0")),
        sa.Column("error_summary", JSONB),
    )
    op.create_index(
        "ix_dir_platform_started",
        "delivery_ingest_run",
        ["platform", "started_at"],
    )

    # ---- delivery_source_record ----
    op.create_table(
        "delivery_source_record",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("platform", sa.String(32), nullable=False),
        sa.Column("source_listing_id", sa.String(256)),
        sa.Column("source_url", sa.Text),
        sa.Column(
            "scraped_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=text("now()"),
        ),
        sa.Column(
            "city", sa.String(64), nullable=False, server_default=text("'riyadh'")
        ),
        # Location
        sa.Column("district_text", sa.String(256)),
        sa.Column("area_text", sa.String(256)),
        sa.Column("address_raw", sa.Text),
        sa.Column("lat", sa.Numeric(10, 7)),
        sa.Column("lon", sa.Numeric(10, 7)),
        sa.Column("geocode_method", sa.String(32), server_default=text("'none'")),
        sa.Column("location_confidence", sa.Float, server_default=text("0.0")),
        # Restaurant identity
        sa.Column("restaurant_name_raw", sa.String(512)),
        sa.Column("restaurant_name_normalized", sa.String(512)),
        sa.Column("brand_raw", sa.String(256)),
        sa.Column("branch_raw", sa.String(256)),
        # Category / cuisine
        sa.Column("cuisine_raw", sa.String(256)),
        sa.Column("category_raw", sa.String(256)),
        sa.Column("category_confidence", sa.Float, server_default=text("0.0")),
        # Pricing & ratings
        sa.Column("price_band_raw", sa.String(32)),
        sa.Column("rating", sa.Numeric(3, 2)),
        sa.Column("rating_count", sa.Integer),
        # Delivery details
        sa.Column("delivery_time_min", sa.Integer),
        sa.Column("delivery_fee", sa.Numeric(8, 2)),
        sa.Column("minimum_order", sa.Numeric(8, 2)),
        # Status
        sa.Column("promo_text", sa.Text),
        sa.Column("availability_text", sa.String(256)),
        sa.Column("is_open_now_raw", sa.Boolean),
        # Contact
        sa.Column("phone_raw", sa.String(64)),
        sa.Column("website_raw", sa.Text),
        sa.Column("menu_url", sa.Text),
        # Raw payload
        sa.Column("raw_payload", JSONB),
        # Ingest tracking
        sa.Column("ingest_run_id", sa.Integer),
        sa.Column("parse_confidence", sa.Float, server_default=text("0.0")),
        # Entity resolution
        sa.Column(
            "entity_resolution_status",
            sa.String(16),
            server_default=text("'pending'"),
        ),
        sa.Column("matched_restaurant_poi_id", sa.String(128)),
        sa.Column("matched_entity_confidence", sa.Float),
    )

    op.create_index("ix_dsr_platform", "delivery_source_record", ["platform"])
    op.create_index(
        "ix_dsr_platform_scraped",
        "delivery_source_record",
        ["platform", "scraped_at"],
    )
    op.create_index(
        "ix_dsr_name_normalized",
        "delivery_source_record",
        ["restaurant_name_normalized"],
    )
    op.create_index("ix_dsr_district", "delivery_source_record", ["district_text"])
    op.create_index(
        "ix_dsr_resolution_status",
        "delivery_source_record",
        ["entity_resolution_status"],
    )
    op.create_index(
        "ix_dsr_matched_poi",
        "delivery_source_record",
        ["matched_restaurant_poi_id"],
    )
    op.create_index("ix_dsr_ingest_run", "delivery_source_record", ["ingest_run_id"])
    op.create_index("ix_dsr_brand", "delivery_source_record", ["brand_raw"])


def downgrade() -> None:
    op.drop_index("ix_dsr_brand", table_name="delivery_source_record")
    op.drop_index("ix_dsr_ingest_run", table_name="delivery_source_record")
    op.drop_index("ix_dsr_matched_poi", table_name="delivery_source_record")
    op.drop_index("ix_dsr_resolution_status", table_name="delivery_source_record")
    op.drop_index("ix_dsr_district", table_name="delivery_source_record")
    op.drop_index("ix_dsr_name_normalized", table_name="delivery_source_record")
    op.drop_index("ix_dsr_platform_scraped", table_name="delivery_source_record")
    op.drop_index("ix_dsr_platform", table_name="delivery_source_record")
    op.drop_table("delivery_source_record")
    op.drop_index("ix_dir_platform_started", table_name="delivery_ingest_run")
    op.drop_table("delivery_ingest_run")
