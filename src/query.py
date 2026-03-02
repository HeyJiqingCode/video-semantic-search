from __future__ import annotations

import json
import logging
import re
import urllib.error
import urllib.request
from typing import Any

from src.config import settings
from src.pipeline import blob_url

_EN_STOPWORDS = {
    "the", "a", "an", "and", "or", "to", "of", "in", "on", "for", "with", "from",
    "is", "are", "be", "by", "at", "as", "it", "that", "this", "what", "how", "when",
    "where", "which", "before", "after", "then", "than", "if", "do", "does", "did",
    "can", "could", "should", "would", "will", "about", "into", "over", "under",
}
_MAX_REFINEMENT_SHIFT_MS = 20000
_MAX_REFINEMENT_RATIO = 0.35
_SEARCH_SELECT_FIELDS = (
    "chunk_id,video_id,start_ms,end_ms,content,chunk_language,"
    "dominant_language,source_path,video_url"
)
logger = logging.getLogger("uvicorn.error")


# Execute one Azure AI Search documents/search request.
def _search_api(payload: dict[str, Any]) -> dict[str, Any]:
    url = (
        f"{settings.search_endpoint}/indexes/{settings.search_index_name}/docs/search"
        f"?api-version={settings.search_api_version}"
    )
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "api-key": settings.search_admin_key,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        detail = raw.strip() or str(exc)
        raise RuntimeError(f"Azure AI Search request failed ({exc.code}): {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Azure AI Search request network error: {exc}") from exc


# Build hybrid query payload with optional video-level filter.
def _hybrid_payload(query: str, top: int, video_id: str | None = None) -> dict[str, Any]:
    payload = {
        "search": query,
        "vectorQueries": [
            {
                "kind": "text",
                "text": query,
                "fields": "content_vector",
                "k": settings.candidate_k,
            }
        ],
        "queryType": "semantic",
        "semanticConfiguration": "sem-default",
        "top": top,
        "select": _SEARCH_SELECT_FIELDS,
    }
    if video_id:
        escaped = video_id.replace("'", "''")
        payload["filter"] = f"video_id eq '{escaped}'"
    return payload


# Extract meaningful CN/EN query tokens for anchor matching.
def _extract_query_tokens(query: str) -> list[str]:
    q = (query or "").lower()
    english = [
        w for w in re.findall(r"[a-z0-9]{2,}", q)
        if w not in _EN_STOPWORDS
    ]
    chinese_words = re.findall(r"[\u4e00-\u9fff]{2,}", q)
    chinese_bigrams: list[str] = []
    for w in chinese_words:
        if len(w) == 2:
            chinese_bigrams.append(w)
        else:
            chinese_bigrams.extend(w[i:i + 2] for i in range(len(w) - 1))
    tokens = english + chinese_words + chinese_bigrams
    # 去重并按长度倒序，优先用更有信息量的 token
    return sorted(set(tokens), key=len, reverse=True)


# Locate best anchor position in content for intra-chunk jump refinement.
def _find_best_anchor_char_index(query: str, content: str) -> tuple[int | None, bool, int]:
    if not query or not content:
        return None, False, 0
    text = content.lower()
    q = query.lower().strip()
    if not q:
        return None, False, 0

    exact_idx = text.find(q)
    if exact_idx >= 0:
        return exact_idx, True, 999

    tokens = _extract_query_tokens(q)
    if not tokens:
        return None, False, 0

    # 按句子/子句做轻量定位，选 token 命中权重最高的子句
    best_score = 0
    best_start = None
    best_match_count = 0
    for m in re.finditer(r"[^.!?。！？\n]+", text):
        seg = m.group(0)
        score = 0
        matched = 0
        for t in tokens:
            if t in seg:
                score += max(1, len(t))
                matched += 1
        if score > best_score or (score == best_score and matched > best_match_count):
            best_score = score
            best_start = m.start()
            best_match_count = matched

    if best_start is not None and best_score > 0:
        return best_start, False, best_match_count

    # 兜底：找第一个较长 token 的首次出现
    fallback_match_count = 0
    for t in tokens:
        if len(t) < 3:
            continue
        idx = text.find(t)
        if idx >= 0:
            fallback_match_count = 1
            return idx, False, fallback_match_count
    return None, False, 0


# Estimate refined jump timestamp inside a chunk for better playback start.
def _estimate_jump_start_ms(query: str, start_ms: int, end_ms: int, content: str) -> int:
    if not settings.enable_intra_chunk_jump:
        return start_ms
    if end_ms <= start_ms:
        return start_ms
    if not query or not content:
        return start_ms

    duration = end_ms - start_ms
    if duration <= 1500:
        return start_ms

    anchor, is_exact_match, matched_terms = _find_best_anchor_char_index(query, content)
    if anchor is None:
        return start_ms

    # 非精确匹配时，至少有 2 个词命中才允许段内偏移，避免长段“飞跳”
    if not is_exact_match and matched_terms < 2:
        return start_ms

    text_len = max(1, len(content))
    ratio = anchor / text_len
    # 命中太靠前时，保持原始段首，避免误偏移
    if ratio < 0.03:
        return start_ms

    jump = start_ms + int(duration * ratio)
    if not is_exact_match:
        # 保守偏移：最多偏移整个段时长的 35%，且不超过 20s
        max_shift = min(int(duration * _MAX_REFINEMENT_RATIO), _MAX_REFINEMENT_SHIFT_MS)
        jump = min(jump, start_ms + max_shift)
    # 避免跳到段尾导致错过内容
    upper = max(start_ms, end_ms - 500)
    return max(start_ms, min(jump, upper))


# Generate fresh runtime video URL from source path when possible.
def _resolve_runtime_video_url(source_path: str, stored_url: str) -> str:
    if not source_path:
        return stored_url
    try:
        return blob_url(settings.raw_video_container, source_path)
    except Exception as exc:
        logger.warning(
            "[Search] failed to refresh video SAS from source_path=%s, fallback to stored_url, error=%s",
            source_path,
            exc,
        )
        return stored_url


# Normalize search hits into API-facing result dictionaries.
def _normalize_hits(data: dict[str, Any], query: str | None = None) -> list[dict[str, Any]]:
    hits = data.get("value", [])
    normalized: list[dict[str, Any]] = []
    for h in hits:
        start_ms = int(h.get("start_ms") or 0)
        end_ms = int(h.get("end_ms") or start_ms)
        content = h.get("content") or ""
        source_path = h.get("source_path") or ""
        jump_start_ms = _estimate_jump_start_ms(
            query or "",
            start_ms,
            end_ms,
            content,
        )
        video_url = _resolve_runtime_video_url(source_path, h.get("video_url") or "")
        normalized.append(
            {
                "chunk_id": h.get("chunk_id"),
                "video_id": h.get("video_id"),
                "start_ms": start_ms,
                "end_ms": end_ms,
                "jump_start_ms": jump_start_ms,
                "content": content,
                "chunk_language": h.get("chunk_language"),
                "dominant_language": h.get("dominant_language"),
                "source_path": source_path,
                "video_url": video_url,
                "score": h.get("@search.score", 0.0),
                "reranker_score": h.get("@search.rerankerScore", 0.0),
            }
        )
    return normalized


# Run search and return ranked segment matches with confidence signal.
def search_segments(query: str, top: int | None = None, video_id: str | None = None) -> dict[str, Any]:
    top = top or settings.top_k
    merged = _normalize_hits(_search_api(_hybrid_payload(query, top, video_id=video_id)), query=query)

    confidence = 0.0
    if merged:
        confidence = float(merged[0].get("reranker_score") or merged[0].get("score") or 0.0)
    return {
        "query": query,
        "confidence": confidence,
        "video_filter": video_id,
        "results": merged,
    }


# Return distinct indexed videos with refreshed playable URLs.
def list_indexed_videos(max_docs: int = 1000) -> list[dict[str, Any]]:
    payload = {
        "search": "*",
        "top": max_docs,
        "select": "video_id,video_url,source_path",
    }
    data = _search_api(payload)
    out: dict[str, dict[str, Any]] = {}
    for row in data.get("value", []):
        vid = row.get("video_id")
        if not vid:
            continue
        if vid not in out:
            source_path = row.get("source_path") or ""
            stored_url = row.get("video_url") or ""
            out[vid] = {
                "video_id": vid,
                "video_url": _resolve_runtime_video_url(source_path, stored_url),
                "source_path": source_path,
            }
    return sorted(out.values(), key=lambda x: x["video_id"])


# Return timeline chunks of one video ordered by start timestamp.
def get_video_chunks(video_id: str, max_chunks: int = 120) -> list[dict[str, Any]]:
    escaped = video_id.replace("'", "''")
    payload = {
        "search": "*",
        "filter": f"video_id eq '{escaped}'",
        "top": max_chunks,
        "orderby": "start_ms asc",
        "select": _SEARCH_SELECT_FIELDS,
    }
    return _normalize_hits(_search_api(payload))
