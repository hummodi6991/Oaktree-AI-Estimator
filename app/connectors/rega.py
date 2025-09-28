from typing import Iterable, Dict

# Many indicator pages provide CSV/Excel exports; wire their direct file URLs once identified.
CSV_URLS: list[str] = []  # add one or more allowed CSV export links


def fetch_market_indicators() -> Iterable[Dict]:
    from app.connectors.open_data import safe_get_bytes
    import csv
    import io

    for url in CSV_URLS:
        raw = safe_get_bytes(url)
        text = raw.decode("utf-8", errors="ignore")
        rd = csv.DictReader(io.StringIO(text))
        for r in rd:
            # normalize your chosen schema
            yield {
                "date": r.get("date") or r.get("month") or r.get("period"),
                "city": r.get("city") or r.get("region"),
                "asset_type": r.get("asset_type") or "residential",
                "indicator_type": r.get("indicator_type") or "sale_price_per_m2",
                "value": r.get("value"),
                "unit": r.get("unit") or "SAR/m2",
                "source_url": url,
            }
