from fastapi import Depends, HTTPException, Request

from app.security import auth


async def set_auth_context(
    request: Request, payload: dict = Depends(auth.require)
) -> dict:
    request.state.auth = payload
    return payload


async def require_admin_context(
    payload: dict = Depends(set_auth_context),
) -> dict:
    if payload.get("is_admin") is not True:
        raise HTTPException(status_code=403, detail="Forbidden")
    return payload
