import json
import logging
import os
from fastapi import Depends, Header, HTTPException

MODE = os.getenv("AUTH_MODE", "disabled")  # disabled | api_key | oidc
API_KEY = os.getenv("API_KEY")
API_KEYS_JSON = os.getenv("API_KEYS_JSON")
ADMIN_API_KEYS_JSON = os.getenv("ADMIN_API_KEYS_JSON")

LOGGER = logging.getLogger(__name__)


def _load_keys(raw_value: str | None) -> dict[str, str] | None:
    if not raw_value:
        return None
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        LOGGER.warning("Invalid API key JSON provided; ignoring.")
        return None
    if not isinstance(parsed, dict):
        LOGGER.warning("API key JSON must be an object; ignoring.")
        return None
    return {
        key: value
        for key, value in parsed.items()
        if isinstance(key, str) and isinstance(value, str)
    }


async def require(
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None),
):
    if MODE == "disabled":
        return {"sub": "anonymous", "mode": MODE}
    if MODE == "api_key":
        key = (x_api_key or (authorization or "").replace("Bearer ", "").strip())
        admin_keys = _load_keys(ADMIN_API_KEYS_JSON)
        user_keys = _load_keys(API_KEYS_JSON)
        if admin_keys:
            for admin_id, admin_key in admin_keys.items():
                if key == admin_key:
                    return {"sub": admin_id, "is_admin": True, "mode": MODE}
        if user_keys:
            for user_id, user_key in user_keys.items():
                if key == user_key:
                    return {"sub": user_id, "is_admin": False, "mode": MODE}
        if not user_keys:
            if not API_KEY or key != API_KEY:
                raise HTTPException(status_code=401, detail="Unauthorized")
            return {"sub": "api-key", "is_admin": False, "mode": MODE}
        raise HTTPException(status_code=401, detail="Unauthorized")
    if MODE == "oidc":
        # Placeholder: validate JWT (iss/aud) via jwks; keep it simple for now per roadmap
        # Raise until wired to Entra ID in staging
        raise HTTPException(status_code=501, detail="OIDC not yet configured")


async def require_admin(payload: dict = Depends(require)) -> dict:
    if payload.get("is_admin") is not True:
        raise HTTPException(status_code=403, detail="Forbidden")
    return payload
