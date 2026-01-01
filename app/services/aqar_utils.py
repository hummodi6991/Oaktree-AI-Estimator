from __future__ import annotations


def norm_city_for_aqar(city: str | None) -> str | None:
    """
    Normalize city names for Aqar data sources.

    - Strip whitespace.
    - Map common English names to their Arabic equivalents used in Aqar.
    - Otherwise return the stripped city unchanged.
    """
    if city is None:
        return None

    city_stripped = city.strip()
    city_key = city_stripped.lower()
    mappings = {
        "riyadh": "الرياض",
        "ar riyadh": "الرياض",
        "ar-riyadh": "الرياض",
        "jeddah": "جدة",
        "dammam": "الدمام",
    }
    return mappings.get(city_key, city_stripped)
