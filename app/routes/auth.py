from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.config import settings
from src.local_auth import create_session, delete_session, get_session_username, validate_credentials

router = APIRouter()


class LoginRequest(BaseModel):
    username: str
    password: str


# Return current authentication state for login page and frontend checks.
@router.get("/auth/session")
def auth_session(request: Request) -> dict:
    if not settings.enable_local_auth:
        return {"authenticated": True, "username": "anonymous", "auth_enabled": False}
    token = request.cookies.get(settings.local_auth_cookie_name)
    username = get_session_username(token)
    return {
        "authenticated": bool(username),
        "username": username or "",
        "auth_enabled": True,
    }


# Validate local credentials and issue session cookie.
@router.post("/auth/login")
def auth_login(req: LoginRequest) -> JSONResponse:
    if not settings.enable_local_auth:
        return JSONResponse({"ok": True, "auth_enabled": False})
    if not validate_credentials(req.username.strip(), req.password):
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    token = create_session(req.username.strip())
    resp = JSONResponse({"ok": True, "username": req.username.strip(), "auth_enabled": True})
    resp.set_cookie(
        key=settings.local_auth_cookie_name,
        value=token,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=max(60, settings.local_auth_session_hours * 3600),
        path="/",
    )
    return resp


# Clear session cookie and remove the corresponding local session.
@router.post("/auth/logout")
def auth_logout(request: Request) -> JSONResponse:
    if settings.enable_local_auth:
        token = request.cookies.get(settings.local_auth_cookie_name)
        delete_session(token)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(key=settings.local_auth_cookie_name, path="/")
    return resp
