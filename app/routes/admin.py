from __future__ import annotations

import logging
from collections.abc import Callable

from fastapi import APIRouter, HTTPException, Request

from app.lifecycle import set_provision_state
from src.pipeline import ensure_containers
from src.provision import provision_search_resources, rebuild_search_resources

router = APIRouter()
logger = logging.getLogger("uvicorn.error")


# Run a provision-like action and sync shared provision state.
def _run_provision_action(
    request: Request,
    action_label: str,
    action: Callable[[], dict[str, str]],
) -> dict:
    logger.info("[Provision] manual %s begin", action_label)
    storage_details = ensure_containers()
    search_details = action()
    details = {
        "storage": storage_details,
        "search": search_details,
    }
    set_provision_state(request.app, done=True, error="", details=details)
    logger.info("[Provision] manual %s completed details=%s", action_label, details)
    return {"status": "ok", "details": details}


# Provision storage and search resources without deleting existing index.
@router.post("/admin/provision")
def provision(request: Request) -> dict:
    try:
        return _run_provision_action(request, "provision", provision_search_resources)
    except Exception as exc:
        set_provision_state(request.app, done=False, error=str(exc), details={})
        logger.exception("[Provision] manual provision failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# Rebuild search resources by recreating index and dependent artifacts.
@router.post("/admin/rebuild")
def rebuild(request: Request) -> dict:
    try:
        return _run_provision_action(request, "rebuild", rebuild_search_resources)
    except Exception as exc:
        set_provision_state(request.app, done=False, error=str(exc), details={})
        logger.exception("[Provision] manual rebuild failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
