from __future__ import annotations

from collections.abc import Generator
from typing import Any

from openai import NotFoundError, OpenAI

from src.config import settings
from src.query import get_video_chunks

_summary_cache: dict[str, str] = {}
_MAX_CONTEXT_CHUNKS = 120
_MAX_SUMMARY_CACHE_ITEMS = 64
_SUMMARY_INSTRUCTIONS = (
    "You are a technical video summarizer. "
    "Summarize the video in concise English with 3-5 bullet points. "
    "Where possible, cite approximate time ranges in mm:ss format."
)
_CHAT_INSTRUCTIONS = (
    "You are a video assistant. Answer only from the provided video context. "
    "If the context is insufficient, say so explicitly. "
    "Keep the answer concise and include one or more supporting time ranges in mm:ss when possible."
)


# Build OpenAI client using configured endpoint and deployment settings.
def _llm_client() -> OpenAI:
    api_key = settings.chat_model_api_key
    base_url = settings.chat_model_endpoint
    if not api_key or not base_url or not settings.chat_model_deployment:
        raise RuntimeError(
            "Chat model config missing. Please set CHAT_MODEL_ENDPOINT, CHAT_MODEL_DEPLOYMENT, CHAT_MODEL_API_KEY."
        )
    endpoint = base_url.strip().rstrip("/")
    if endpoint.endswith("/openai/v1"):
        normalized_base_url = endpoint + "/"
    elif endpoint.endswith("/openai"):
        normalized_base_url = endpoint + "/v1/"
    else:
        normalized_base_url = endpoint + "/openai/v1/"
    return OpenAI(api_key=api_key, base_url=normalized_base_url)


# Build timeline context text fed to summary/chat models.
def _format_timeline_context(
    video_id: str,
    max_chunks: int = _MAX_CONTEXT_CHUNKS,
) -> tuple[str, list[dict[str, Any]]]:
    chunks = get_video_chunks(video_id, max_chunks=max_chunks)
    if not chunks:
        return "", []

    lines: list[str] = []
    for i, c in enumerate(chunks, start=1):
        start = int(c.get("start_ms") or 0)
        end = int(c.get("end_ms") or 0)
        text = (c.get("content") or "").strip()
        if not text:
            continue
        lines.append(f"[{i}] {start}-{end}ms: {text}")
    return "\n".join(lines), chunks


# Build reusable kwargs for Responses API calls.
def _responses_kwargs(input_text: str, instructions: str) -> dict[str, Any]:
    return {
        "model": settings.chat_model_deployment,
        "input": input_text,
        "reasoning": {"effort": "medium", "summary": "auto"},
        "text": {"verbosity": "low"},
        "instructions": instructions,
    }


# Build grounded prompt text for question answering on one video.
def _build_chat_input(video_id: str, context: str, user_message: str) -> str:
    return (
        f"Video ID: {video_id}\n\n"
        f"Video Context:\n{context}\n\n"
        f"User Question:\n{user_message}"
    )


# Execute one non-streaming Responses API call and return output text.
def _create_response_text(client: OpenAI, input_text: str, instructions: str) -> str:
    response = client.responses.create(**_responses_kwargs(input_text, instructions))
    return (response.output_text or "").strip()


# Stream response deltas from Responses API for incremental UI rendering.
def _iter_response_text_stream(
    client: OpenAI,
    input_text: str,
    instructions: str,
) -> Generator[str, None, None]:
    stream = client.responses.create(
        stream=True,
        **_responses_kwargs(input_text, instructions),
    )
    for event in stream:
        etype = getattr(event, "type", "") or ""
        if etype == "response.output_text.delta":
            delta = getattr(event, "delta", "") or ""
            if delta:
                yield delta


# Store summary in bounded in-memory cache to prevent unbounded growth.
def _cache_summary(video_id: str, summary: str) -> None:
    if video_id not in _summary_cache and len(_summary_cache) >= _MAX_SUMMARY_CACHE_ITEMS:
        oldest_key = next(iter(_summary_cache))
        _summary_cache.pop(oldest_key, None)
    _summary_cache[video_id] = summary


# Generate and optionally cache concise summary for one video.
def summarize_video(video_id: str, force_refresh: bool = False) -> dict[str, Any]:
    if not force_refresh and video_id in _summary_cache:
        return {"video_id": video_id, "summary": _summary_cache[video_id], "cached": True}

    context, chunks = _format_timeline_context(video_id, max_chunks=_MAX_CONTEXT_CHUNKS)
    if not context:
        return {"video_id": video_id, "summary": "No indexed chunks found for this video.", "cached": False}

    client = _llm_client()
    try:
        summary = _create_response_text(
            client=client,
            input_text=f"Video ID: {video_id}\n\nTimeline Chunks:\n{context}",
            instructions=_SUMMARY_INSTRUCTIONS,
        )
    except NotFoundError as exc:
        raise RuntimeError(
            "Chat model resource not found (404). "
            "Please verify CHAT_MODEL_ENDPOINT and CHAT_MODEL_DEPLOYMENT."
        ) from exc
    summary = summary or "Summary generation returned empty output."
    _cache_summary(video_id, summary)
    return {"video_id": video_id, "summary": summary, "cached": False, "chunk_count": len(chunks)}


# Generate one non-streaming answer grounded in selected video chunks.
def chat_with_video(video_id: str, user_message: str) -> dict[str, Any]:
    context, chunks = _format_timeline_context(video_id, max_chunks=_MAX_CONTEXT_CHUNKS)
    if not context:
        return {"video_id": video_id, "answer": "No indexed chunks found for this video.", "used_chunks": 0}

    client = _llm_client()
    input_text = _build_chat_input(video_id, context, user_message)
    try:
        answer = _create_response_text(
            client=client,
            input_text=input_text,
            instructions=_CHAT_INSTRUCTIONS,
        )
    except NotFoundError as exc:
        raise RuntimeError(
            "Chat model resource not found (404). "
            "Please verify CHAT_MODEL_ENDPOINT and CHAT_MODEL_DEPLOYMENT."
        ) from exc
    return {
        "video_id": video_id,
        "answer": answer,
        "used_chunks": len(chunks),
    }


# Stream answer tokens grounded in selected video chunks.
def chat_with_video_stream(video_id: str, user_message: str) -> Generator[str, None, None]:
    context, _ = _format_timeline_context(video_id, max_chunks=_MAX_CONTEXT_CHUNKS)
    if not context:
        yield "No indexed chunks found for this video."
        return

    client = _llm_client()
    input_text = _build_chat_input(video_id, context, user_message)

    try:
        yield from _iter_response_text_stream(
            client=client,
            input_text=input_text,
            instructions=_CHAT_INSTRUCTIONS,
        )
    except NotFoundError as exc:
        raise RuntimeError(
            "Chat model resource not found (404). "
            "Please verify CHAT_MODEL_ENDPOINT and CHAT_MODEL_DEPLOYMENT."
        ) from exc
