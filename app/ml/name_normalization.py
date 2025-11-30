from __future__ import annotations
from typing import Optional
import re

# Strip Arabic diacritics (just in case some sources have them)
_AR_DIACRITICS = re.compile(
    r"[\u0610-\u061A\u064B-\u065F\u06D6-\u06DC\u06DF-\u06E8\u06EA-\u06ED]"
)

# Hard aliases where OSM naming and comps naming differ.
# You can extend this over time as you discover more mismatches.
_OSM_DISTRICT_ALIASES: dict[str, str] = {
    # Arabic "حي ..." → bare name
    "حي العارض": "العارض",
    "حي الياسمين": "الياسمين",
    "حي النرجس": "النرجس",
    "حي الازدهار": "الازدهار",
    "حي حطين": "حطين",
    "حي الملز": "الملز",
    "حي العليا": "العليا",

    # English spellings → canonical lower-case (mainly for Kaggle-style names)
    "al olaya": "al olaya",
    "olaya": "al olaya",
    "al malaz": "al malaz",
    "malaz": "al malaz",
    "hittin": "hittin",
    "hitten": "hittin",
    "al yasmin": "al yasmin",
    "al yasmeen": "al yasmin",
    "al narjis": "al narjis",
    "al narjes": "al narjis",
}


def _basic_norm(value: Optional[str]) -> str:
    """Trim, remove tatweel & diacritics, and collapse whitespace."""
    if not value:
        return ""
    s = value.strip()
    s = s.replace("ـ", "")  # tatweel
    s = _AR_DIACRITICS.sub("", s)
    # Normalise various dashes / punctuation to spaces
    for ch in ("-", "–", "_"):
        s = s.replace(ch, " ")
    # Collapse repeated spaces
    s = re.sub(r"\s+", " ", s)
    return s


def norm_city(city: Optional[str]) -> str:
    """Canonical city name used by the hedonic model."""
    s = _basic_norm(city)

    if not s:
        return ""

    # If it's Latin letters, just lower-case
    if re.search(r"[A-Za-z]", s):
        s = s.lower()

    # Normalise Riyadh variants (Arabic vs English)
    if s in {"riyadh", "ar riyadh", "al riyadh", "مدينة الرياض", "الرياض"}:
        return "riyadh"

    return s


def norm_district(city: Optional[str], district: Optional[str]) -> str:
    """
    Canonical district name. This is applied both at training time and
    at prediction time so that OSM / Kaggle / Rega names line up.
    """
    s = _basic_norm(district)

    if not s:
        return ""

    # Drop trailing city mention, e.g. "حي النرجس، الرياض"
    for sep in ("،", ","):
        if sep in s:
            s = s.split(sep, 1)[0].strip()

    # Drop leading Arabic "حي"/"حى"
    for prefix in ("حي ", "حى "):
        if s.startswith(prefix):
            s = s[len(prefix):].strip()

    # Second round of whitespace squash (in case we removed things)
    s = re.sub(r"\s+", " ", s)

    # For Latin names, lower-case for stability
    if re.search(r"[A-Za-z]", s):
        key = s.lower()
    else:
        key = s

    # Apply alias map if we have a match
    if key in _OSM_DISTRICT_ALIASES:
        return _OSM_DISTRICT_ALIASES[key]

    return key
