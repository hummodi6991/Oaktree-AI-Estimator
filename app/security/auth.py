import os
from fastapi import Header, HTTPException

MODE = os.getenv("AUTH_MODE", "disabled")  # disabled | api_key | oidc
API_KEY = os.getenv("API_KEY")


async def require(
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None),
):
    if MODE == "disabled":
        return {"sub": "anonymous", "mode": MODE}
    if MODE == "api_key":
        key = (x_api_key or (authorization or "").replace("Bearer ", "").strip())
        if not API_KEY or key != API_KEY:
            raise HTTPException(status_code=401, detail="Unauthorized")
        return {"sub": "api-key", "mode": MODE}
    if MODE == "oidc":
        # Placeholder: validate JWT (iss/aud) via jwks; keep it simple for now per roadmap
        # Raise until wired to Entra ID in staging
        raise HTTPException(status_code=501, detail="OIDC not yet configured")
