from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse

from src.config import settings
from src.local_auth import get_session_username


# Determine whether a request path should bypass local authentication checks.
def _is_public_path(path: str) -> bool:
    if path in {"/login", "/auth/login", "/auth/session", "/health", "/favicon.ico"}:
        return True
    if path.startswith("/static/"):
        return True
    return False


# Register local auth middleware that guards app pages and API routes.
def register_local_auth_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def local_auth_middleware(request: Request, call_next):
        if not settings.enable_local_auth:
            return await call_next(request)

        path = request.url.path
        if _is_public_path(path):
            return await call_next(request)

        token = request.cookies.get(settings.local_auth_cookie_name)
        username = get_session_username(token)
        if username:
            request.state.auth_user = username
            return await call_next(request)

        if path == "/":
            return RedirectResponse(url="/login", status_code=302)
        return JSONResponse(status_code=401, content={"detail": "Authentication required."})
