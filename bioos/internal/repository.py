import ast
import json
import os
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote, urlencode, urlparse

import requests
from volcengine.Credentials import Credentials
from volcengine.auth.SignerV4 import SignerV4
from volcengine.base.Request import Request

from bioos.config import Config
from bioos.errors import ConfigurationError


DEFAULT_PASSPORT_EXPIRES_IN = 3600
PASSPORT_REFRESH_MARGIN = 60
DEFAULT_REPOSITORY_ENDPOINT = "https://network.miracle.ac.cn"
DEFAULT_DRS_ENDPOINT = "http://imc-drs.miracle.ac.cn"


def resolve_repository_endpoint(endpoint: Optional[str] = None) -> str:
    return _resolve_endpoint(
        explicit=endpoint,
        env_name="BIOOS_REPOSITORY_ENDPOINT",
        config_names=("repository_endpoint", "repositoryaddr"),
        default_endpoint=DEFAULT_REPOSITORY_ENDPOINT,
    )


def resolve_drs_endpoint(endpoint: Optional[str] = None) -> str:
    return _resolve_endpoint(
        explicit=endpoint,
        env_name="BIOOS_DRS_ENDPOINT",
        config_names=("drs_endpoint", "drsaddr"),
        default_endpoint=DEFAULT_DRS_ENDPOINT,
    )


def _resolve_endpoint(explicit: Optional[str], env_name: str, config_names, default_endpoint: str) -> str:
    value = explicit or os.getenv(env_name)
    if not value:
        client_config = _load_client_config()
        for name in config_names:
            if client_config.get(name):
                value = client_config[name]
                break
    if not value:
        value = default_endpoint
    if not value:
        raise ConfigurationError(env_name)
    return value.rstrip("/")


def _load_client_config() -> Dict[str, Any]:
    try:
        from bioos.cli.config_store import load_client_config

        return load_client_config()
    except Exception:
        return {}


class RepositoryPassportProvider:
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


class RepositoryRestClient:
    def __init__(
        self,
        endpoint: str,
        passport_provider: Optional[RepositoryPassportProvider] = None,
        sign_requests: bool = True,
        service_name: str = "bio",
        region: Optional[str] = None,
        timeout: int = 60,
        session: Optional[requests.Session] = None,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.passport_provider = passport_provider or RepositoryPassportProvider()
        self.sign_requests = sign_requests
        self.service_name = service_name
        self.region = region
        self.timeout = timeout
        self.session = session or requests.Session()

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.request("GET", path, params=params)

    def post(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self.request("POST", path, params=params, body=body)

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        method = method.upper()
        body_text = json.dumps(body, ensure_ascii=False) if body is not None else ""
        query = _normalize_query_params(params or {})
        full_path = self._full_path(path)
        if self.sign_requests:
            query = self._signed_query(method, full_path, query, body_text)

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.passport_provider.get_token()}",
        }
        if body is not None:
            headers["Content-Type"] = "application/json"

        response = self.session.request(
            method,
            self._url(full_path, query),
            headers=headers,
            data=body_text if body is not None else None,
            timeout=self.timeout,
        )
        response.raise_for_status()
        if not response.content:
            return {}
        return response.json()

    def download_url(
        self,
        url: str,
        target_path: str,
        headers: Optional[Dict[str, str]] = None,
        chunk_size: int = 1024 * 1024,
    ) -> Dict[str, Any]:
        if not url:
            raise ValueError("download url is required")

        target = Path(target_path).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        response = self.session.request(
            "GET",
            url,
            headers=headers or {},
            stream=True,
            timeout=self.timeout,
        )
        response.raise_for_status()

        bytes_written = 0
        with target.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                handle.write(chunk)
                bytes_written += len(chunk)

        return {
            "target": str(target),
            "bytes_written": bytes_written,
        }

    def _signed_query(self, method: str, path: str, query: OrderedDict, body_text: str) -> OrderedDict:
        request = Request()
        request.set_shema(urlparse(self.endpoint).scheme)
        request.set_method(method)
        request.set_path(path)
        request.set_query(OrderedDict(query))
        request.body = body_text
        SignerV4.sign_url(request, self._credentials())
        return request.query

    def _credentials(self) -> Credentials:
        login_info = Config.login_info()
        if not login_info.access_key:
            raise ConfigurationError("ACCESS_KEY")
        if not login_info.secret_key:
            raise ConfigurationError("SECRET_KEY")
        region = self.region or login_info.region
        if not region:
            raise ConfigurationError("REGION")
        return Credentials(login_info.access_key, login_info.secret_key, self.service_name, region)

    def _full_path(self, path: str) -> str:
        parsed = urlparse(self.endpoint)
        base_path = parsed.path.rstrip("/")
        api_path = "/" + path.lstrip("/")
        return f"{base_path}{api_path}" if base_path else api_path

    def _url(self, path: str, query: OrderedDict) -> str:
        parsed = urlparse(self.endpoint)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        encoded_query = urlencode(query, doseq=True)
        if encoded_query:
            return f"{base_url}{path}?{encoded_query}"
        return f"{base_url}{path}"


def _normalize_query_params(params: Dict[str, Any]) -> OrderedDict:
    query = OrderedDict()
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            values = [str(item) for item in value if item is not None]
            if values:
                query[key] = values
        else:
            query[key] = str(value)
    return query


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


def quote_path_segment(value: str) -> str:
    return quote(str(value).strip(), safe="")


def _repository_passport_error(exc: Exception) -> RuntimeError:
    backend_error = _parse_backend_error(exc)
    if backend_error:
        code = backend_error.get("Code") or "Unknown"
        message = str(backend_error.get("Message") or str(exc)).rstrip(".")
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
    payload = _loads_possible_bytes_json(text)
    if not isinstance(payload, dict):
        return None

    metadata = payload.get("ResponseMetadata")
    if not isinstance(metadata, dict):
        return None

    error = metadata.get("Error")
    if not isinstance(error, dict):
        return None

    parsed = dict(error)
    if metadata.get("RequestId"):
        parsed["RequestId"] = metadata.get("RequestId")
    return parsed


def _loads_possible_bytes_json(text: str) -> Optional[Dict[str, Any]]:
    raw = text
    try:
        literal = ast.literal_eval(text)
        if isinstance(literal, bytes):
            raw = literal.decode("utf-8", errors="replace")
        elif isinstance(literal, str):
            raw = literal
    except (SyntaxError, ValueError):
        pass

    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(raw[start:end + 1])
            except ValueError:
                return None
    return None
