from typing import Iterable, Dict
import csv
import io

from app.connectors.open_data import safe_get_bytes
from app.core.config import settings

CSV_URL = settings.GASTAT_CCI_CSV_URL


def fetch_cci_rows() -> Iterable[Dict]:
    if not CSV_URL:
        return []
    raw = safe_get_bytes(CSV_URL)
    text = raw.decode("utf-8", errors="ignore")
    rd = csv.DictReader(io.StringIO(text))
    for r in rd:
        yield {"month": r.get("month"), "cci_index": r.get("cci_index"), "source_url": CSV_URL}
