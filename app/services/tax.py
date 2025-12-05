from __future__ import annotations

from typing import Any, Dict, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.tables import TaxRule


def latest_tax_rule(db: Session, tax_type: str = "RETT") -> Optional[TaxRule]:
    """
    Return the latest rule for a given tax_type.

    For now we just take the highest rule_id for that tax_type.
    """
    return (
        db.query(TaxRule)
        .filter(func.lower(TaxRule.tax_type) == tax_type.lower())
        .order_by(TaxRule.rule_id.desc(), TaxRule.id.desc())
        .first()
    )


def latest_tax_rate(db: Session, tax_type: str = "RETT") -> dict[str, Any] | None:
    """Return the highest rule_id for this tax_type with rate + metadata."""

    row = latest_tax_rule(db, tax_type=tax_type)
    if not row:
        return None
    return {
        "tax_type": row.tax_type,
        "rule_id": row.rule_id,
        "rate": float(row.rate),
        "base_type": row.base_type,
        "payer_default": row.payer_default,
        "exemptions": row.exemptions,
        "notes": row.notes,
    }
