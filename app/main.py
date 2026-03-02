from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.lifecycle import lifespan
from app.routes import admin, auth, chat, pipeline, search, system
from src.config import settings
from src.local_auth import get_session_username


app = FastAPI(title="Video Semantic Search API", version="0.1.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")


# Determine whether the request path bypasses local authentication.
def _is_public_path(path: str) -> bool:
    if path in {"/login", "/auth/login", "/auth/session", "/health", "/favicon.ico"}:
        return True
    if path.startswith("/static/"):
        return True
    return False


@app.middleware("http")
# Enforce local session auth for app pages and API routes except allowlisted paths.
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


app.include_router(system.router)
app.include_router(auth.router)
app.include_router(admin.router)
app.include_router(pipeline.router)
app.include_router(search.router)
app.include_router(chat.router)
