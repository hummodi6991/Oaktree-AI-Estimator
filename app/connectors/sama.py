from typing import Iterable, Dict

from app.connectors.open_data import safe_get_json

# Option A: if you adopt an open-data mirror that exposes JSON with date/value (e.g., Opendatasoft/KAPSARC)
OPEN_JSON = None  # e.g., "https://data.kapsarc.org/api/records/1.0/search/?dataset=interest-rates-and-sama-average-bills&rows=5000"


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
