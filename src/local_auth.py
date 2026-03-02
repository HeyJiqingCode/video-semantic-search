from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hmac
import secrets

from src.config import settings

_sessions: dict[str, tuple[str, datetime]] = {}


# Remove expired in-memory sessions to keep state bounded.
def _cleanup_expired_sessions() -> None:
    now = datetime.now(timezone.utc)
    expired = [token for token, (_, expires_at) in _sessions.items() if expires_at <= now]
    for token in expired:
        _sessions.pop(token, None)


# Validate submitted username/password against configured local credentials.
def validate_credentials(username: str, password: str) -> bool:
    if not settings.enable_local_auth:
        return True
    return (
        hmac.compare_digest(username or "", settings.local_auth_username)
        and hmac.compare_digest(password or "", settings.local_auth_password)
    )


# Create a new authenticated session and return its opaque token.
def create_session(username: str) -> str:
    _cleanup_expired_sessions()
    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=settings.local_auth_session_hours)
    _sessions[token] = (username, expires_at)
    return token


# Resolve username from session token if session exists and is not expired.
def get_session_username(token: str | None) -> str | None:
    if not token:
        return None
    _cleanup_expired_sessions()
    row = _sessions.get(token)
    if not row:
        return None
    username, expires_at = row
    if expires_at <= datetime.now(timezone.utc):
        _sessions.pop(token, None)
        return None
    return username


# Remove one session token from local in-memory store.
def delete_session(token: str | None) -> None:
    if token:
        _sessions.pop(token, None)
