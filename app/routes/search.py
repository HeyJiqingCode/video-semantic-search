from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.query import list_indexed_videos, search_segments

router = APIRouter()
logger = logging.getLogger("uvicorn.error")


class SearchRequest(BaseModel):
    query: str
    top: int | None = None
    video_id: str | None = None


# Execute hybrid+semantic search for a query optionally scoped to one video.
@router.post("/search")
def search(req: SearchRequest) -> dict:
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="query is required")
    try:
        return search_segments(req.query, top=req.top, video_id=req.video_id)
    except Exception as exc:
        logger.exception("[Search] search failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# Return distinct video list currently available in the index.
@router.get("/videos")
def videos() -> dict:
    try:
        return {"videos": list_indexed_videos()}
    except Exception as exc:
        logger.exception("[Search] list videos failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
