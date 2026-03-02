from __future__ import annotations

import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from typing import Iterable

from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas

from src.config import settings
from src.provision import run_indexer

logger = logging.getLogger("uvicorn.error")


@dataclass
class ChunkDoc:
    chunk_id: str
    video_id: str
    start_ms: int
    end_ms: int
    content: str
    chunk_language: str
    dominant_language: str
    source_path: str
    video_url: str
    metadata: dict[str, Any]


class ContentUnderstandingClient:
    # Initialize client config from environment-backed settings.
    def __init__(self) -> None:
        self.endpoint = settings.cu_endpoint
        self.api_key = settings.cu_api_key
        self.api_version = settings.cu_api_version
        self.analyzer_id = settings.cu_analyzer_id

    # Build HTTP headers for Content Understanding API calls.
    def _headers(self) -> dict[str, str]:
        return {
            "Ocp-Apim-Subscription-Key": self.api_key,
            "Content-Type": "application/json",
        }

    # Build analyze request body according to API version contract.
    def _build_analyze_body(self, file_url: str) -> dict[str, Any]:
        # GA (2025-11-01+) requires: {"inputs":[{"url":"..."}]}
        # Older previews accepted: {"url":"..."}.
        version_date = (self.api_version or "")[:10]
        if version_date >= "2025-11-01":
            return {"inputs": [{"url": file_url}]}
        return {"url": file_url}

    # Normalize operation location/header payload into a full poll URL.
    def _normalize_operation_location(self, operation_ref: str) -> str:
        op = (operation_ref or "").strip()
        if not op:
            return op
        if op.startswith("http://") or op.startswith("https://"):
            return op
        return (
            f"{self.endpoint}/contentunderstanding/analyzerResults/"
            f"{urllib.parse.quote(op)}?api-version={urllib.parse.quote(self.api_version)}"
        )

    # Submit an analyze request and return operation tracking URL/id.
    def begin_analyze_from_url(self, file_url: str) -> str:
        url = (
            f"{self.endpoint}/contentunderstanding/analyzers/"
            f"{urllib.parse.quote(self.analyzer_id)}:analyze"
            f"?api-version={urllib.parse.quote(self.api_version)}"
        )
        body = json.dumps(self._build_analyze_body(file_url)).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers=self._headers(), method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                op = resp.headers.get("Operation-Location")
                if not op:
                    payload = json.loads(resp.read().decode("utf-8"))
                    op = payload.get("id") or payload.get("operationLocation")
                if not op:
                    raise RuntimeError("Content Understanding analyze did not return operation location.")
                return self._normalize_operation_location(op)
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            detail = raw.strip() or str(exc)
            raise RuntimeError(f"Content Understanding analyze request failed ({exc.code}): {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Content Understanding analyze request network error: {exc}") from exc

    # Poll analyze operation until completion or timeout.
    def poll_result(self, operation_location: str, timeout_sec: int = 1200) -> dict[str, Any]:
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            req = urllib.request.Request(
                operation_location,
                headers={"Ocp-Apim-Subscription-Key": self.api_key},
                method="GET",
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                detail = raw.strip() or str(exc)
                raise RuntimeError(f"Content Understanding poll request failed ({exc.code}): {detail}") from exc
            except urllib.error.URLError as exc:
                raise RuntimeError(f"Content Understanding poll request network error: {exc}") from exc

            status = (payload.get("status") or "").lower()
            if status in {"succeeded", "failed", "canceled"}:
                if status != "succeeded":
                    raise RuntimeError(f"Content Understanding analysis failed: {payload}")
                return payload
            time.sleep(3)
        raise TimeoutError("Content Understanding poll timeout.")


# Build a BlobServiceClient from the configured storage connection string.
def _blob_client() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(settings.storage_connection_string)


# Convert arbitrary video IDs into Azure AI Search-safe document keys.
def _safe_doc_key(text: str) -> str:
    # Azure AI Search key allows letters, digits, underscore, dash, equal sign.
    cleaned = re.sub(r"[^A-Za-z0-9_\-=]", "_", text)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "video"


# Extract and clean markdown transcript fallback text from CU content.
def _extract_markdown_text(content: dict[str, Any]) -> str:
    raw = content.get("markdown") or ""
    if not isinstance(raw, str):
        return ""
    cleaned = raw
    # Remove common VTT/code-fence noise from markdown transcript output.
    cleaned = cleaned.replace("```", " ")
    cleaned = re.sub(r"\bWEBVTT\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\d{2}:\d{2}\.\d{3}\s*-->\s*\d{2}:\d{2}\.\d{3}", " ", cleaned)
    cleaned = re.sub(r"#\s*Video:.*?(?=Transcript|$)", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


# Extract transcript text by concatenating CU phrase-level entries.
def _extract_transcript_text(content: dict[str, Any]) -> str:
    phrases = content.get("transcriptPhrases") or []
    texts: list[str] = []
    for p in phrases:
        t = p.get("text")
        if isinstance(t, str) and t.strip():
            texts.append(t.strip())
    joined = " ".join(texts)
    joined = re.sub(r"\s+", " ", joined).strip()
    return joined


# Detect chunk language from CU phrase metadata.
def _extract_language(content: dict[str, Any], default_lang: str) -> str:
    phrases = content.get("transcriptPhrases") or []
    for p in phrases:
        loc = p.get("locale")
        if loc:
            return loc
    return default_lang


# Convert CU raw result payload into normalized chunk documents.
def chunks_from_cu_result(
    result: dict[str, Any],
    video_id: str,
    source_path: str,
    video_url: str,
) -> list[ChunkDoc]:
    payload = result.get("result", result)
    api_version = payload.get("apiVersion", "")
    analyzer_id = payload.get("analyzerId", "")
    contents = payload.get("contents") or []

    raw_chunks: list[dict[str, Any]] = []
    for c in contents:
        if c.get("kind") != "audioVisual":
            continue
        transcript_text = _extract_transcript_text(c)
        text = transcript_text or _extract_markdown_text(c)
        if not text:
            continue
        start_ms = int(c.get("startTimeMs", 0))
        end_ms = int(c.get("endTimeMs", 0))
        lang = _extract_language(c, "unknown")
        raw_chunks.append(
            {
                "start_ms": start_ms,
                "end_ms": end_ms,
                "content": text,
                "chunk_language": lang,
            }
        )

    if not raw_chunks:
        return []

    dominant = max(
        (
            (c["chunk_language"], max(1, c["end_ms"] - c["start_ms"]))
            for c in raw_chunks
        ),
        key=lambda item: item[1],
    )[0]

    out: list[ChunkDoc] = []
    safe_video_key = _safe_doc_key(video_id)
    for idx, c in enumerate(raw_chunks, start=1):
        out.append(
            ChunkDoc(
                chunk_id=f"{safe_video_key}-{idx}",
                video_id=video_id,
                start_ms=c["start_ms"],
                end_ms=c["end_ms"],
                content=c["content"],
                chunk_language=c["chunk_language"],
                dominant_language=dominant,
                source_path=source_path,
                video_url=video_url,
                metadata={
                    "apiVersion": api_version,
                    "analyzerId": analyzer_id,
                },
            )
        )
    return out


# Ensure required Blob containers exist for raw videos and search docs.
def ensure_containers() -> dict[str, str]:
    client = _blob_client()
    result: dict[str, str] = {}
    for container in [settings.raw_video_container, settings.search_docs_container]:
        c = client.get_container_client(container)
        if not c.exists():
            c.create_container()
            result[container] = "created"
            logger.info("[Provision][Storage] container=%s action=created", container)
        else:
            result[container] = "skipped(existing)"
            logger.info("[Provision][Storage] container=%s action=skipped(existing)", container)
    return result


# Write chunk documents as one JSONL blob consumed by search indexer.
def upload_search_docs_jsonl(video_id: str, docs: Iterable[ChunkDoc]) -> str:
    client = _blob_client()
    container = client.get_container_client(settings.search_docs_container)
    blob_name = f"{video_id}/latest.jsonl"
    lines: list[str] = []
    for d in docs:
        lines.append(
            json.dumps(
                {
                    "chunk_id": d.chunk_id,
                    "video_id": d.video_id,
                    "start_ms": d.start_ms,
                    "end_ms": d.end_ms,
                    "content": d.content,
                    "chunk_language": d.chunk_language,
                    "dominant_language": d.dominant_language,
                    "source_path": d.source_path,
                    "video_url": d.video_url,
                    "metadata_json": json.dumps(d.metadata, ensure_ascii=False),
                },
                ensure_ascii=False,
            )
        )
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    container.upload_blob(blob_name, payload, overwrite=True)
    return blob_name


# Generate a playable/readable blob URL with short-lived SAS when possible.
def blob_url(container: str, blob_name: str) -> str:
    svc = _blob_client()
    client = svc.get_blob_client(container, blob_name)

    account_name = svc.account_name
    credential = getattr(svc, "credential", None)
    account_key = getattr(credential, "account_key", None)

    # CU needs to fetch the media from URL. For private containers, use short-lived SAS.
    if account_name and account_key:
        sas = generate_blob_sas(
            account_name=account_name,
            container_name=container,
            blob_name=blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc).replace(microsecond=0) + timedelta(hours=2),
        )
        return f"{client.url}?{sas}"

    logger.warning(
        "[Storage] account key unavailable, returning plain blob URL for container=%s blob=%s",
        container,
        blob_name,
    )
    return client.url


# Upload a local video file to raw video container and return blob name.
def upload_local_video(local_path: str | Path, target_blob_name: str | None = None) -> str:
    path = Path(local_path)
    if not target_blob_name:
        target_blob_name = path.name
    client = _blob_client().get_blob_client(settings.raw_video_container, target_blob_name)
    with path.open("rb") as f:
        client.upload_blob(f, overwrite=True)
    return target_blob_name


# Run full processing for an existing blob: CU analyze -> chunks -> indexer.
def process_video_blob(blob_name: str) -> dict[str, Any]:
    video_url = blob_url(settings.raw_video_container, blob_name)
    cu = ContentUnderstandingClient()
    op = cu.begin_analyze_from_url(video_url)
    result = cu.poll_result(op)

    video_id = Path(blob_name).stem
    chunks = chunks_from_cu_result(
        result=result,
        video_id=video_id,
        source_path=blob_name,
        video_url=video_url,
    )
    if not chunks:
        return {
            "video_id": video_id,
            "chunk_count": 0,
            "status": "no_chunks",
            "video_url": video_url,
            "blob_name": blob_name,
        }

    jsonl_blob = upload_search_docs_jsonl(video_id, chunks)
    run_indexer()
    return {
        "video_id": video_id,
        "chunk_count": len(chunks),
        "search_docs_blob": jsonl_blob,
        "status": "indexed",
        "video_url": video_url,
        "blob_name": blob_name,
    }


# Upload a local file and immediately process it through the full pipeline.
def process_uploaded_file(local_path: str, original_filename: str | None = None) -> dict[str, Any]:
    target_blob_name = None
    if original_filename:
        clean_name = Path(original_filename).name.strip()
        if clean_name:
            target_blob_name = clean_name
    blob_name = upload_local_video(local_path, target_blob_name=target_blob_name)
    return process_video_blob(blob_name)
