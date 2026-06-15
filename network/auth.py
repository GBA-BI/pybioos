import ast
import base64
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from bioos.config import Config


DEFAULT_PASSPORT_EXPIRES_IN = 3600
PASSPORT_REFRESH_MARGIN = 60


class StaticPassportProvider:
    def __init__(self, token: str):
        if not token:
            raise ValueError("passport token is required")
        self.token = token

    def get_token(self, force_refresh: bool = False) -> str:
        return self.token


class BioOSBridgePassportProvider:
    """Exchange BioOS AK/SK credentials for a short-lived Network passport."""

    TOKEN_KEYS = ("AAIPassport", "Passport", "Token", "AccessToken", "access_token", "token")
    EXPIRES_AT_KEYS = ("ExpiresAt", "ExpiredAt", "ExpireTime", "ExpiredTime", "expires_at", "expired_at")
    EXPIRES_IN_KEYS = ("ExpiresIn", "ExpireIn", "expires_in", "expire_in")

    def __init__(
        self,
        expires_in: int = DEFAULT_PASSPORT_EXPIRES_IN,
        refresh_margin: int = PASSPORT_REFRESH_MARGIN,
    ):
        self.expires_in = int(expires_in)
        self.refresh_margin = int(refresh_margin)
        self._token = None
        self._expires_at = 0.0
        self._cache_key = None

    def get_token(self, force_refresh: bool = False) -> str:
        cache_key = self._current_cache_key()
        now = time.time()
        if (
            not force_refresh
            and self._token
            and self._cache_key == cache_key
            and now < self._expires_at - self.refresh_margin
        ):
            return self._token

        params = {"ExpiresIn": self.expires_in} if self.expires_in else {}
        try:
            payload = Config.service().get_repository_passport(params)
        except Exception as exc:
            raise _repository_passport_error(exc) from exc
        token = self._extract_token(payload)
        self._token = token
        self._cache_key = cache_key
        self._expires_at = self._extract_expires_at(payload, now)
        return token

    def _current_cache_key(self):
        login_info = Config.login_info()
        return login_info.endpoint, login_info.region, login_info.access_key

    def _extract_token(self, payload: Dict[str, Any]) -> str:
        if not isinstance(payload, dict):
            raise RuntimeError("GetRepositoryPassport returned an invalid response.")
        for key in self.TOKEN_KEYS:
            value = payload.get(key)
            if value:
                return str(value)
        raise RuntimeError(
            "GetRepositoryPassport did not return a passport token. "
            f"Expected one of: {', '.join(self.TOKEN_KEYS)}."
        )

    def _extract_expires_at(self, payload: Dict[str, Any], now: float) -> float:
        for key in self.EXPIRES_AT_KEYS:
            value = payload.get(key)
            if value:
                parsed = _parse_expiry(value)
                if parsed:
                    return parsed
        for key in self.EXPIRES_IN_KEYS:
            value = payload.get(key)
            if value:
                try:
                    return now + int(value)
                except (TypeError, ValueError):
                    pass
        return now + self.expires_in


RepositoryPassportProvider = BioOSBridgePassportProvider


def passport_subject(provider, force_refresh: bool = False) -> str:
    token = provider.get_token(force_refresh=force_refresh)
    return passport_token_subject(token)


def passport_token_subject(token: str) -> str:
    if not token:
        raise RuntimeError("Network passport token is empty.")

    parts = str(token).split(".")
    if len(parts) < 2:
        raise RuntimeError("Network passport token does not contain a JWT payload.")

    try:
        payload_segment = parts[1]
        padding = "=" * ((4 - len(payload_segment) % 4) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_segment + padding))
    except Exception as exc:
        raise RuntimeError("Unable to decode Network passport user identity.") from exc

    subject = payload.get("sub")
    if not subject:
        raise RuntimeError("Network passport does not expose a user identity.")
    return str(subject)


def _parse_expiry(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        return timestamp

    if not isinstance(value, str):
        return None

    stripped = value.strip()
    if not stripped:
        return None
    try:
        return _parse_expiry(float(stripped))
    except ValueError:
        pass

    try:
        normalized = stripped.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except ValueError:
        return None


def _repository_passport_error(exc: Exception) -> RuntimeError:
    backend_error = _parse_backend_error(exc)
    if backend_error:
        code = backend_error.get("Code") or backend_error.get("code") or "Unknown"
        message = str(backend_error.get("Message") or backend_error.get("message") or str(exc)).rstrip(".")
        request_id = backend_error.get("RequestId")
        request_id_text = f" RequestId: {request_id}." if request_id else ""
        return RuntimeError(
            "Unable to obtain BioOS Network AAAI passport. "
            "Please make sure the current BioOS account is associated with a BioOS Network account. "
            f"Backend error: {code} - {message}.{request_id_text}"
        )
    return RuntimeError(
        "Unable to obtain BioOS Network AAAI passport. "
        "Please make sure the current BioOS account is associated with a BioOS Network account. "
        f"Original error: {exc}"
    )


def _parse_backend_error(exc: Exception) -> Optional[Dict[str, Any]]:
    text = str(exc)
    candidates = [text]
    if text.startswith("b'") or text.startswith('b"'):
        try:
            candidates.append(ast.literal_eval(text).decode("utf-8"))
        except Exception:
            pass
    for candidate in candidates:
        try:
            import json

            payload = json.loads(candidate)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        response_metadata = payload.get("ResponseMetadata")
        if isinstance(response_metadata, dict) and isinstance(response_metadata.get("Error"), dict):
            parsed = dict(response_metadata["Error"])
            if response_metadata.get("RequestId"):
                parsed["RequestId"] = response_metadata.get("RequestId")
            return parsed
        if isinstance(payload.get("Error"), dict):
            return payload["Error"]
    return None
