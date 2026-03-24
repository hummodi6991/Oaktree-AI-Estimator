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
    "shawarma": {"en": "Shawarma", "ar": "شاورما"},
    "grills": {"en": "Grills / Kebab", "ar": "مشويات"},
    "traditional": {"en": "Traditional / Arabic", "ar": "أكل شعبي / عربي"},
    "japanese": {"en": "Japanese / Sushi", "ar": "ياباني / سوشي"},
    "chinese": {"en": "Chinese", "ar": "صيني"},
    "indian": {"en": "Indian", "ar": "هندي"},
    "korean": {"en": "Korean", "ar": "كوري"},
    "thai": {"en": "Thai", "ar": "تايلندي"},
    "italian": {"en": "Italian", "ar": "إيطالي"},
    "asian": {"en": "Asian", "ar": "آسيوي"},
    "seafood": {"en": "Seafood", "ar": "مأكولات بحرية"},
    "coffee": {"en": "Coffee / Café", "ar": "قهوة / كافيه"},
    "bakery": {"en": "Bakery", "ar": "مخبوزات"},
    "dessert": {"en": "Dessert", "ar": "حلويات"},
    "juice": {"en": "Juice / Smoothie", "ar": "عصائر"},
    "coffee_bakery": {"en": "Coffee & Bakery", "ar": "قهوة ومخبوزات"},
    "sandwich": {"en": "Sandwich", "ar": "سندويش"},
    "healthy": {"en": "Healthy / Salads", "ar": "صحي / سلطات"},
    "breakfast": {"en": "Breakfast", "ar": "فطور"},
    "international": {"en": "International", "ar": "عالمي"},
}

# Maps raw source keywords/tags → normalized category.
# Checked in order; first match wins.
# Maps raw source keywords/tags → normalized category.
# Checked in order; first match wins.  Specific categories MUST come before
# broad fallbacks so that Arabic cuisine_raw values like "حلى, مشروبات"
# resolve to "dessert" rather than falling through to "traditional".
_KEYWORD_MAP: list[tuple[list[str], str]] = [
    # ── Specific categories (checked first) ──
    (["burger", "hamburger", "برجر", "هامبرغر"], "burger"),
    (["pizza", "pizzeria", "بيتزا", "بيتسا"], "pizza"),
    (["chicken", "broasted", "fried_chicken", "wings", "دجاج", "بروستد", "فراخ"], "chicken"),
    (["shawarma", "شاورما", "شاورمة"], "shawarma"),
    (["sushi", "سوشي", "japanese", "ياباني", "ramen"], "japanese"),
    (["chinese", "صيني", "wok", "noodle"], "chinese"),
    (["indian", "هندي", "biryani", "برياني", "mandi", "مندي"], "indian"),
    (["korean", "كوري"], "korean"),
    (["thai", "تايلندي", "vietnamese", "فيتنامي"], "thai"),
    (["italian", "إيطالي", "pasta", "باستا"], "italian"),
    (["grills", "مشويات", "مشاوي", "kebab", "كباب", "kabsa", "كبسة"], "grills"),
    (["fish", "seafood", "shrimp", "سمك", "بحري", "مأكولات بحرية"], "seafood"),
    (["cafe", "coffee", "قهوة", "كافيه"], "coffee"),
    (["bakery", "مخبز", "مخابز", "معجنات", "pastry"], "bakery"),
    (["dessert", "حلويات", "حلى", "ice_cream", "chocolate", "donut"], "dessert"),
    (["juice", "عصير", "عصائر", "smoothie"], "juice"),
    (["sandwich", "سندويش", "سندوتش"], "sandwich"),
    (["salad", "healthy", "vegan", "vegetarian", "poke", "bowl", "صحي"], "healthy"),
    (["breakfast", "فطور", "إفطار"], "breakfast"),
    # ── Broad fallbacks (only if no specific match above) ──
    (
        [
            "arabic", "middle_eastern", "saudi", "lebanese", "syrian",
            "egyptian", "yemeni", "traditional", "falafel",
            "شعبي", "عربي",
        ],
        "traditional",
    ),
    (["asian", "آسيوي"], "asian"),
]


# Maps a specific category to the broader delivery buckets it belongs to.
# Used by delivery scoring to match candidates against delivery market data
# at both specific and broad granularity levels.
_CATEGORY_TO_DELIVERY_BUCKETS: dict[str, list[str]] = {
    "burger": ["burger", "international"],
    "pizza": ["pizza", "international"],
    "chicken": ["chicken", "international", "traditional"],
    "shawarma": ["shawarma", "traditional"],
    "coffee": ["coffee", "coffee_bakery"],
    "bakery": ["bakery", "coffee_bakery"],
    "dessert": ["dessert", "coffee_bakery"],
    "juice": ["juice", "coffee_bakery"],
    "japanese": ["japanese", "asian", "international"],
    "chinese": ["chinese", "asian", "international"],
    "indian": ["indian", "international"],
    "korean": ["korean", "asian", "international"],
    "thai": ["thai", "asian", "international"],
    "italian": ["italian", "international"],
    "grills": ["grills", "traditional"],
    "sandwich": ["sandwich", "international", "traditional"],
    "healthy": ["healthy", "international"],
    "breakfast": ["breakfast", "coffee_bakery", "traditional"],
    "seafood": ["seafood"],
    "asian": ["asian", "international"],
    "international": ["international"],
    "traditional": ["traditional"],
    "coffee_bakery": ["coffee_bakery", "coffee", "bakery", "dessert", "juice"],
}


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
