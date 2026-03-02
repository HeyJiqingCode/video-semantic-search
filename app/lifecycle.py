from __future__ import annotations

from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI

from src.config import settings
from src.pipeline import ensure_containers
from src.provision import provision_search_resources

logger = logging.getLogger("uvicorn.error")


# Build a default provision state before any startup tasks run.
def default_provision_state() -> dict:
    return {
        "enabled": settings.auto_provision_on_startup,
        "done": False,
        "error": "",
        "details": {},
    }


# Read current provision state from app state with a safe default.
def get_provision_state(app: FastAPI) -> dict:
    return getattr(app.state, "provision_state", default_provision_state())


# Update app-level provision state and return the stored snapshot.
def set_provision_state(
    app: FastAPI,
    *,
    done: bool,
    error: str,
    details: dict,
) -> dict:
    state = {
        "enabled": settings.auto_provision_on_startup,
        "done": done,
        "error": error,
        "details": details,
    }
    app.state.provision_state = state
    return state


@asynccontextmanager
# Run startup auto-provisioning and persist status for health/admin routes.
async def lifespan(app: FastAPI):
    app.state.provision_state = default_provision_state()
    if settings.auto_provision_on_startup:
        try:
            logger.info("[Provision] startup auto-provision begin")
            storage_details = ensure_containers()
            search_details = provision_search_resources()
            state = set_provision_state(
                app,
                done=True,
                error="",
                details={
                    "storage": storage_details,
                    "search": search_details,
                },
            )
            logger.info("[Provision] startup auto-provision completed details=%s", state["details"])
        except Exception as exc:
            set_provision_state(app, done=False, error=str(exc), details={})
            logger.exception("[Provision] startup auto-provision failed: %s", exc)
            if settings.auto_provision_fail_fast:
                raise
    yield
