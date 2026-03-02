from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    enable_local_auth: bool = os.getenv("ENABLE_LOCAL_AUTH", "true").strip().lower() == "true"
    local_auth_username: str = os.getenv("LOCAL_AUTH_USERNAME", "admin")
    local_auth_password: str = os.getenv("LOCAL_AUTH_PASSWORD", "admin123")
    local_auth_session_hours: int = int(os.getenv("LOCAL_AUTH_SESSION_HOURS", "24"))
    local_auth_cookie_name: str = os.getenv("LOCAL_AUTH_COOKIE_NAME", "video_auth_session")

    storage_connection_string: str = os.getenv("STORAGE_CONNECTION_STRING", "")
    raw_video_container: str = os.getenv("RAW_VIDEO_CONTAINER", "raw-videos")
    search_docs_container: str = os.getenv("SEARCH_DOCS_CONTAINER", "search-docs")

    cu_endpoint: str = os.getenv("CU_ENDPOINT", "").rstrip("/")
    cu_api_key: str = os.getenv("CU_API_KEY", "")
    cu_api_version: str = os.getenv("CU_API_VERSION", "2025-11-01")
    cu_analyzer_id: str = os.getenv("CU_ANALYZER_ID", "prebuilt-videoSearch")

    search_endpoint: str = os.getenv("SEARCH_ENDPOINT", "").rstrip("/")
    search_admin_key: str = os.getenv("SEARCH_ADMIN_KEY", "")
    search_api_version: str = os.getenv("SEARCH_API_VERSION", "2025-09-01")
    search_index_name: str = os.getenv("SEARCH_INDEX_NAME", "video-chunks-index")
    search_datasource_name: str = os.getenv("SEARCH_DATASOURCE_NAME", "video-chunks-ds")
    search_skillset_name: str = os.getenv("SEARCH_SKILLSET_NAME", "video-chunks-skillset")
    search_indexer_name: str = os.getenv("SEARCH_INDEXER_NAME", "video-chunks-indexer")

    aoai_endpoint: str = os.getenv("AOAI_ENDPOINT", "").rstrip("/")
    aoai_api_key: str = os.getenv("AOAI_API_KEY", "")
    aoai_embedding_deployment: str = os.getenv("AOAI_EMBEDDING_DEPLOYMENT", "")
    aoai_embedding_model_name: str = os.getenv("AOAI_EMBEDDING_MODEL_NAME", "text-embedding-3-small")
    aoai_embedding_dimensions: int = int(os.getenv("AOAI_EMBEDDING_DIMENSIONS", "1536"))
    chat_model_endpoint: str = os.getenv("CHAT_MODEL_ENDPOINT", "").rstrip("/")
    chat_model_deployment: str = os.getenv("CHAT_MODEL_DEPLOYMENT", "")
    chat_model_api_key: str = os.getenv("CHAT_MODEL_API_KEY", os.getenv("AOAI_API_KEY", ""))

    top_k: int = int(os.getenv("TOP_K", "5"))
    candidate_k: int = int(os.getenv("CANDIDATE_K", "50"))
    enable_intra_chunk_jump: bool = (
        os.getenv("ENABLE_INTRA_CHUNK_JUMP", "true").strip().lower() == "true"
    )
    search_jump_preroll_seconds: float = float(os.getenv("SEARCH_JUMP_PREROLL_SECONDS", "0"))
    auto_provision_on_startup: bool = (
        os.getenv("AUTO_PROVISION_ON_STARTUP", "true").strip().lower() == "true"
    )
    auto_provision_fail_fast: bool = (
        os.getenv("AUTO_PROVISION_FAIL_FAST", "false").strip().lower() == "true"
    )

    soft_delete_column_name: str = os.getenv("SOFT_DELETE_COLUMN_NAME", "").strip()
    soft_delete_marker_value: str = os.getenv("SOFT_DELETE_MARKER_VALUE", "true").strip()


settings = Settings()
