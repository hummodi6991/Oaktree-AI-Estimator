import json
import logging
import os
from fastapi import Depends, Header, HTTPException

LOGGER = logging.getLogger(__name__)


def get_mode() -> str:
    """Return the current AUTH_MODE, read fresh from env each time."""
    return os.getenv("AUTH_MODE", "disabled")


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
    mode = get_mode()
    if mode == "disabled":
        return {"sub": "anonymous", "mode": mode}
    if mode == "api_key":
        key = (x_api_key or (authorization or "").replace("Bearer ", "").strip())
        admin_keys = _load_keys(os.getenv("ADMIN_API_KEYS_JSON"))
        user_keys = _load_keys(os.getenv("API_KEYS_JSON"))
        if admin_keys:
            for admin_id, admin_key in admin_keys.items():
                if key == admin_key:
                    return {"sub": admin_id, "is_admin": True, "mode": mode}
        if user_keys:
            for user_id, user_key in user_keys.items():
                if key == user_key:
                    return {"sub": user_id, "is_admin": False, "mode": mode}
        if not user_keys:
            api_key = os.getenv("API_KEY")
            if not api_key or key != api_key:
                raise HTTPException(status_code=401, detail="Unauthorized")
            return {"sub": "api-key", "is_admin": False, "mode": mode}
        raise HTTPException(status_code=401, detail="Unauthorized")
    if mode == "oidc":
        # Placeholder: validate JWT (iss/aud) via jwks; keep it simple for now per roadmap
        # Raise until wired to Entra ID in staging
        raise HTTPException(status_code=501, detail="OIDC not yet configured")


async def require_admin(payload: dict = Depends(require)) -> dict:
    if payload.get("is_admin") is not True:
        raise HTTPException(status_code=403, detail="Forbidden")
    return payload
