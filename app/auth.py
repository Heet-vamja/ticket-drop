from fastapi import Header, HTTPException, Query

from .config import ADMIN_KEY


def require_admin(x_admin_key: str | None = Header(None), key: str | None = Query(None)) -> None:
    """Header for fetch(), query param for EventSource (which can't set headers)."""
    if (x_admin_key or key) != ADMIN_KEY:
        raise HTTPException(403, "admin key required")
