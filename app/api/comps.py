from fastapi import APIRouter, Query

router = APIRouter(tags=["comps"])

EXAMPLE_COMPS = [
    {
        "id": "C-001",
        "date": "2025-06-15",
        "city": "Riyadh",
        "district": "Al Olaya",
        "asset_type": "land",
        "net_area_m2": 1500,
        "price_per_m2": 2800,
        "source": "rega_indicator",
        "source_url": "https://example-rega",
    }
]


@router.get("/comps")
def get_comps(
    city: str | None = Query(default=None),
    type: str | None = Query(default=None),
    since: str | None = Query(default=None),
) -> dict[str, list[dict]]:
    items = EXAMPLE_COMPS
    if city:
        items = [record for record in items if record["city"].lower() == city.lower()]
    if type:
        items = [record for record in items if record["asset_type"].lower() == type.lower()]
    return {"items": items}
