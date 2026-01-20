"""Widen source_type columns."""

from __future__ import annotations

from typing import Optional

from alembic import op
import sqlalchemy as sa


revision = "b1d2c3e4f5a6"
down_revision = "a1b2c3d4e5f6"  # current head from `alembic heads`
branch_labels = None
depends_on = None


def _column_length(column_type: sa.types.TypeEngine) -> Optional[int]:
    return getattr(column_type, "length", None)


def _maybe_widen(table: str, column: str, target_length: int, nullable: bool) -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    columns = {col["name"]: col for col in inspector.get_columns(table)}
    column_info = columns.get(column)
    if not column_info:
        return

    current_type = column_info["type"]
    current_length = _column_length(current_type)
    if current_length is None or current_length >= target_length:
        return

    op.alter_column(
        table,
        column,
        type_=sa.String(length=target_length),
        existing_type=current_type,
        existing_nullable=column_info["nullable"],
    )


def _maybe_shrink(table: str, column: str, target_length: int, nullable: bool) -> None:
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    columns = {col["name"]: col for col in inspector.get_columns(table)}
    column_info = columns.get(column)
    if not column_info:
        return

    current_type = column_info["type"]
    current_length = _column_length(current_type)
    if current_length is None or current_length <= target_length:
        return

    op.alter_column(
        table,
        column,
        type_=sa.String(length=target_length),
        existing_type=current_type,
        existing_nullable=nullable,
    )


def upgrade() -> None:
    _maybe_widen("estimate_line", "source_type", 64, nullable=True)
    _maybe_widen("assumption_ledger", "source_type", 64, nullable=False)


def downgrade() -> None:
    raise RuntimeError("Downgrade not supported: source_type may exceed 16 chars.")
