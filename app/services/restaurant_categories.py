"""
Restaurant category taxonomy and normalization.

Maps categories from various sources (Overture, OSM, delivery platforms)
to a unified set of restaurant categories used in the scoring engine.
"""

from __future__ import annotations

CATEGORIES: dict[str, dict[str, str]] = {
    "burger": {"en": "Burger", "ar": "برجر"},
    "pizza": {"en": "Pizza", "ar": "بيتزا"},
    "chicken": {"en": "Chicken", "ar": "دجاج"},
    "traditional": {"en": "Traditional / Arabic", "ar": "أكل شعبي / عربي"},
    "asian": {"en": "Asian", "ar": "آسيوي"},
    "indian": {"en": "Indian", "ar": "هندي"},
    "turkish": {"en": "Turkish", "ar": "تركي"},
    "japanese_sushi": {"en": "Japanese / Sushi", "ar": "ياباني / سوشي"},
    "seafood": {"en": "Seafood", "ar": "مأكولات بحرية"},
    "coffee_bakery": {"en": "Coffee & Bakery", "ar": "قهوة ومخبوزات"},
    "desserts": {"en": "Desserts & Sweets", "ar": "حلويات"},
    "healthy": {"en": "Healthy / Salads", "ar": "صحي / سلطات"},
    "cloud_kitchen": {"en": "Cloud Kitchen", "ar": "مطبخ سحابي"},
    "international": {"en": "International", "ar": "عالمي"},
}

# Maps raw source keywords/tags → normalized category.
# Checked in order; first match wins.
_KEYWORD_MAP: list[tuple[list[str], str]] = [
    (["burger", "hamburger"], "burger"),
    (["pizza", "pizzeria"], "pizza"),
    (["chicken", "broasted", "fried_chicken", "wings"], "chicken"),
    (
        [
            "arabic", "middle_eastern", "saudi", "lebanese", "syrian",
            "egyptian", "yemeni", "kabsa", "mandi", "shawarma",
            "falafel", "traditional", "شعبي", "عربي", "كبسة", "مندي",
        ],
        "traditional",
    ),
    # Specific Asian sub-categories — checked before generic "asian"
    (
        [
            "indian", "biryani", "tandoori", "curry", "masala", "dosa",
            "naan", "tikka", "هندي",
        ],
        "indian",
    ),
    (
        [
            "turkish", "kebab", "lahmacun", "pide", "iskender",
            "doner", "baklava_turkish", "تركي",
        ],
        "turkish",
    ),
    (
        ["japanese", "sushi", "ramen", "tempura", "udon", "ياباني", "سوشي"],
        "japanese_sushi",
    ),
    (
        [
            "chinese", "korean", "thai", "vietnamese", "asian",
            "noodle", "wok", "dim_sum", "pho",
        ],
        "asian",
    ),
    (["fish", "seafood", "shrimp", "سمك", "بحري"], "seafood"),
    (
        ["cafe", "coffee", "bakery", "قهوة"],
        "coffee_bakery",
    ),
    (
        [
            "dessert", "pastry", "ice_cream", "chocolate", "donut",
            "kunafa", "sweets", "candy", "gelato", "حلويات", "كنافة",
        ],
        "desserts",
    ),
    (["salad", "healthy", "vegan", "vegetarian", "poke", "bowl"], "healthy"),
    (["cloud_kitchen", "ghost_kitchen", "delivery_only", "مطبخ_سحابي"], "cloud_kitchen"),
]


def normalize_category(raw: str | None) -> str:
    """
    Map a raw category/cuisine string to a normalized category key.
    Returns 'international' as fallback.
    """
    if not raw:
        return "international"
    lower = raw.lower().replace("-", "_").replace(" ", "_")
    for keywords, cat in _KEYWORD_MAP:
        for kw in keywords:
            if kw in lower:
                return cat
    return "international"


def normalize_osm_cuisine(cuisine_tag: str | None) -> str:
    """Normalize an OSM ``cuisine=`` tag value."""
    if not cuisine_tag:
        return "international"
    # OSM uses semicolons for multi-value: "burger;pizza" → take first
    first = cuisine_tag.split(";")[0].strip()
    return normalize_category(first)


def normalize_overture_taxonomy(taxonomy_path: str | None) -> str:
    """
    Normalize an Overture Maps taxonomy path like
    ``restaurant > asian_restaurant > chinese_restaurant``.
    """
    if not taxonomy_path:
        return "international"
    # Use the most specific (last) segment
    parts = [p.strip() for p in taxonomy_path.split(">")]
    return normalize_category(parts[-1] if parts else taxonomy_path)


def list_categories() -> list[dict[str, str]]:
    """Return all categories with display names."""
    return [
        {"key": key, "name_en": val["en"], "name_ar": val["ar"]}
        for key, val in CATEGORIES.items()
    ]
