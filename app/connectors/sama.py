from typing import Iterable, Dict

from app.connectors.open_data import safe_get_json
from app.core.config import settings

OPEN_JSON = settings.SAMA_OPEN_JSON


def fetch_rates() -> Iterable[Dict]:
    if not OPEN_JSON:
        return []
    data = safe_get_json(OPEN_JSON)
    for r in data.get("records", []):
        fields = r.get("fields", {})
        if "date" in fields and "rr" in fields:  # adapt to schema
            yield {
                "date": str(fields["date"])[:10],
                "tenor": "overnight",
                "rate_type": "SAMA_base",
                "value": float(fields["rr"]),
                "source_url": OPEN_JSON,
            }
