from __future__ import annotations

import json
import logging
from collections.abc import Iterator

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src.llm import chat_with_video, chat_with_video_stream, summarize_video

router = APIRouter()
logger = logging.getLogger("uvicorn.error")


class ChatRequest(BaseModel):
    video_id: str
    message: str


# Validate and normalize required string input.
def _require_non_empty(value: str, field_name: str) -> str:
    normalized = (value or "").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail=f"{field_name} is required")
    return normalized


# Yield NDJSON chat events while preserving server-side errors.
def _iter_chat_stream(video_id: str, message: str) -> Iterator[str]:
    try:
        for delta in chat_with_video_stream(video_id, message):
            yield json.dumps({"type": "delta", "text": delta}, ensure_ascii=False) + "\n"
        yield json.dumps({"type": "done"}, ensure_ascii=False) + "\n"
    except Exception as exc:
        logger.exception("[Chat] stream failed: %s", exc)
        yield json.dumps({"type": "error", "message": str(exc)}, ensure_ascii=False) + "\n"


# Summarize a selected video's indexed timeline chunks.
@router.get("/video-summary")
def video_summary(video_id: str, refresh: bool = False) -> dict:
    video_id = _require_non_empty(video_id, "video_id")
    try:
        return summarize_video(video_id, force_refresh=refresh)
    except Exception as exc:
        logger.exception("[Summary] summary failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# Return a single non-streaming answer grounded in the selected video context.
@router.post("/chat")
def chat(req: ChatRequest) -> dict:
    video_id = _require_non_empty(req.video_id, "video_id")
    message = _require_non_empty(req.message, "message")
    try:
        return chat_with_video(video_id, message)
    except Exception as exc:
        logger.exception("[Chat] chat failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# Stream a chat answer as NDJSON deltas for incremental UI rendering.
@router.post("/chat/stream")
def chat_stream(req: ChatRequest) -> StreamingResponse:
    video_id = _require_non_empty(req.video_id, "video_id")
    message = _require_non_empty(req.message, "message")
    return StreamingResponse(_iter_chat_stream(video_id, message), media_type="application/x-ndjson")
