from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse

from app.lifecycle import get_provision_state
from src.config import settings

router = APIRouter()


# Serve the frontend single-page app.
@router.get("/")
def index() -> FileResponse:
    return FileResponse("app/static/index.html")


# Serve local login page when local auth is enabled.
@router.get("/login")
def login() -> FileResponse:
    return FileResponse("app/static/login.html")


# Serve site favicon.
@router.get("/favicon.ico")
def favicon() -> FileResponse:
    return FileResponse("app/static/favicon.svg")


# Return service health plus current provision status snapshot.
@router.get("/health")
def health(request: Request) -> dict:
    state = get_provision_state(request.app)
    return {
        "status": "ok",
        "provision": state,
    }


# Return UI runtime configuration values.
@router.get("/ui-config")
def ui_config() -> dict:
    return {
        "search_jump_preroll_seconds": settings.search_jump_preroll_seconds,
    }
