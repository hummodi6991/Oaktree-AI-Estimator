from fastapi import APIRouter, Depends

from app.security.auth import require_admin

router = APIRouter(
    prefix="/admin/usage",
    tags=["admin"],
    dependencies=[Depends(require_admin)],
)


@router.get("/summary")
def usage_summary(since: str | None = None) -> dict:
    return {
        "since": since,
        "totals": {
            "active_users": 0,
            "requests": 0,
            "estimates": 0,
            "pdf_exports": 0,
            "error_rate": 0.0,
        },
    }


@router.get("/users")
def usage_users(since: str | None = None) -> dict:
    return {"since": since, "items": []}


@router.get("/user/{user_id}")
def usage_user(user_id: str, since: str | None = None) -> dict:
    return {"user_id": user_id, "since": since, "timeline": [], "metrics": {}}
