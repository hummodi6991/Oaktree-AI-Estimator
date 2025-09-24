from fastapi import APIRouter, Query

router = APIRouter(tags=["indices"])

CCI_SAMPLE = [
    {
        "month": "2025-06-01",
        "sector": "construction",
        "cci_index": 108.9,
        "source_url": "https://example-cci",
    }
]

RATES_SAMPLE = [
    {
        "date": "2025-06-01",
        "tenor": "overnight",
        "rate_type": "SAMA_base",
        "value": 6.00,
        "source_url": "https://example-sama",
    },
    {
        "date": "2025-06-01",
        "tenor": "1M",
        "rate_type": "SAIBOR",
        "value": 6.1,
        "source_url": "https://example-sama",
    },
]


@router.get("/indices/cci")
def get_cci(month: str | None = Query(default=None)) -> dict[str, list[dict]]:
    items = CCI_SAMPLE
    if month:
        items = [record for record in items if record["month"][0:7] == month[0:7]]
    return {"items": items}


@router.get("/indices/rates")
def get_rates(date_str: str | None = Query(default=None)) -> dict[str, list[dict]]:
    items = RATES_SAMPLE
    if date_str:
        items = [record for record in items if record["date"] == date_str]
    return {"items": items}
