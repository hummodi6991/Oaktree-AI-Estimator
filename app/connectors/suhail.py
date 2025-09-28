from typing import Iterable, Dict

from app.core.config import settings

API_URL = settings.SUHAIL_API_URL
API_KEY = settings.SUHAIL_API_KEY

def fetch_comps(query: Dict) -> Iterable[Dict]:
    """
    Placeholder for licensed Suhail feed. Return rows conforming to sale_comp or rent_comp schema.
    """
    return []
