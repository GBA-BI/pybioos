from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from bioos.config import Config
from bioos.errors import ParameterError
from bioos.utils.common_tools import dict_str
from network.auth import BioOSBridgePassportProvider
from network.config import endpoint_from_drs_uri, normalize_endpoint, resolve_repository_endpoint
from network.internal.http import NetworkHttpClient, quote_path_segment


DEFAULT_DRS_ACCESS_ID = "https"


class DRSResource:
    def __init__(
        self,
        endpoint: Optional[str] = None,
        passport_provider: Optional[BioOSBridgePassportProvider] = None,
        fallback_endpoint: Optional[str] = None,
        repository_endpoint: Optional[str] = None,
    ):
        self.endpoint = normalize_drs_endpoint(endpoint) if endpoint else None
        self.fallback_endpoint = (
            normalize_drs_endpoint(fallback_endpoint) if fallback_endpoint else None
        )
        self.repository_endpoint = resolve_repository_endpoint(repository_endpoint)
        self.passport_provider = passport_provider or BioOSBridgePassportProvider()
        self._clients = {}

    def __repr__(self) -> str:
        info = {
            "endpoint": self.endpoint,
            "fallback_endpoint": self.fallback_endpoint,
            "repository_endpoint": self.repository_endpoint,
        }
        return f"DRSResource:\n{dict_str(info)}"

    def object(self, object_id: str) -> Dict[str, Any]:
        endpoint, normalized_id = self._resolve_endpoint_and_object_id(object_id)
        return self._client(endpoint).get(f"/ga4gh/drs/v1/objects/{quote_path_segment(normalized_id)}")

    def access(
        self,
        object_id: str,
        access_id: str = DEFAULT_DRS_ACCESS_ID,
    ) -> Dict[str, Any]:
        if not access_id:
            raise ParameterError("access_id")
        endpoint, normalized_id = self._resolve_endpoint_and_object_id(object_id)
        info = self._client(endpoint).get(f"/ga4gh/drs/v1/objects/{quote_path_segment(normalized_id)}")
        access = access_from_drs_object(info, access_id)
        if access:
            return access
        return self._client(endpoint).get(
            "/ga4gh/drs/v1/objects/"
            f"{quote_path_segment(normalized_id)}/access/{quote_path_segment(access_id)}"
        )

    def authorization(self, object_id: str) -> Dict[str, Any]:
        endpoint, normalized_id = self._resolve_endpoint_and_object_id(object_id)
        return self._client(endpoint).options(f"/ga4gh/drs/v1/objects/{quote_path_segment(normalized_id)}")

    def locate(self, drs_path: str, repository_endpoint: Optional[str] = None) -> Dict[str, Any]:
        if not drs_path:
            raise ParameterError("drs_path")
        value = str(drs_path).strip()
        if not value:
            raise ParameterError("drs_path")
        return Config.service().search_drs(
            {
                "RepositoryEndpoint": resolve_repository_endpoint(repository_endpoint or self.repository_endpoint),
                "DRSPath": value,
                "AAIPassport": self.passport_provider.get_token(),
            }
        )

    def download_object(
        self,
        object_id: str,
        target: str = ".",
        access_id: str = DEFAULT_DRS_ACCESS_ID,
        overwrite: bool = False,
        chunk_size: int = 1024 * 1024,
        object_info: Optional[Dict[str, Any]] = None,
        object_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not target:
            raise ParameterError("target")

        endpoint, normalized_id = self._resolve_endpoint_and_object_id(object_id)
        client = self._client(endpoint)
        info = object_info if object_info is not None else client.get(
            f"/ga4gh/drs/v1/objects/{quote_path_segment(normalized_id)}"
        )
        filename = safe_download_name(object_name or dict_get(info, "name") or normalized_id)
        target_path = resolve_download_target(target, filename)
        if target_path.exists() and not overwrite:
            raise FileExistsError(f"target already exists: {target_path}")

        access = access_from_drs_object(info, access_id)
        if not access:
            access = self.access(object_id, access_id=access_id)
        url = extract_access_url(access)
        headers = extract_access_headers(access)
        result = client.download_url(
            url,
            str(target_path),
            headers=headers,
            chunk_size=chunk_size,
        )
        result.update(
            {
                "success": True,
                "object_id": normalized_id,
                "access_id": access_id,
                "name": filename,
                "drs_endpoint": endpoint,
            }
        )
        return result

    def _resolve_endpoint_and_object_id(self, object_id: str):
        if not object_id:
            raise ParameterError("object_id")
        value = str(object_id).strip()
        endpoint = endpoint_from_drs_uri(value)
        normalized_id = normalize_drs_object_id(value)
        if not endpoint:
            endpoint = self.endpoint or self.fallback_endpoint
        if not endpoint:
            raise ParameterError(
                "object_id",
                "plain DRS object ids require a data library DRSHost or a drs://host/object URI",
            )
        return endpoint, normalized_id

    def _client(self, endpoint: str) -> NetworkHttpClient:
        if endpoint not in self._clients:
            self._clients[endpoint] = NetworkHttpClient(
                endpoint=endpoint,
                passport_provider=self.passport_provider,
                sign_requests=False,
            )
        return self._clients[endpoint]


def normalize_drs_object_id(object_id: str) -> str:
    if not object_id:
        raise ParameterError("object_id")
    value = str(object_id).strip()
    if value.startswith("drs://"):
        parsed = urlparse(value)
        value = parsed.path.strip("/") or parsed.netloc
    if not value:
        raise ParameterError("object_id")
    return value


def normalize_drs_endpoint(endpoint: str) -> str:
    value = normalize_endpoint(endpoint, default_scheme="http")
    for suffix in ("/ga4gh/drs/v1", "/ga4gh/drs"):
        if value.endswith(suffix):
            return value[: -len(suffix)] or value
    return value


def dict_get(value: Any, key: str):
    if isinstance(value, dict):
        return value.get(key)
    return None


def extract_access_url(access: Dict[str, Any]) -> str:
    if not isinstance(access, dict):
        raise RuntimeError("DRS access response is invalid.")
    candidate = access.get("url") or access.get("URL")
    if candidate:
        return candidate
    access_url = access.get("access_url") or access.get("AccessURL")
    if isinstance(access_url, dict):
        candidate = access_url.get("url") or access_url.get("URL")
        if candidate:
            return candidate
    raise RuntimeError("DRS access response did not contain a download URL.")


def extract_access_headers(access: Dict[str, Any]) -> Dict[str, str]:
    if not isinstance(access, dict):
        return {}
    raw_headers = access.get("headers") or access.get("Headers") or []
    if isinstance(raw_headers, dict):
        return {str(key): str(value) for key, value in raw_headers.items() if value is not None}
    headers = {}
    if isinstance(raw_headers, list):
        for item in raw_headers:
            if isinstance(item, dict):
                headers.update({str(key): str(value) for key, value in item.items() if value is not None})
                continue
            if isinstance(item, str) and ":" in item:
                key, value = item.split(":", 1)
                key = key.strip()
                if key:
                    headers[key] = value.strip()
    return headers


def access_from_drs_object(object_info: Dict[str, Any], access_id: str) -> Optional[Dict[str, Any]]:
    if not isinstance(object_info, dict):
        return None
    for method in object_info.get("access_methods") or []:
        if not isinstance(method, dict):
            continue
        method_access_id = method.get("access_id") or method.get("accessId") or method.get("type")
        if method_access_id != access_id:
            continue
        access_url = method.get("access_url") or method.get("accessURL") or method.get("AccessURL")
        if isinstance(access_url, dict) and (access_url.get("url") or access_url.get("URL")):
            return access_url
    return None


def resolve_download_target(target: str, filename: str) -> Path:
    target_path = Path(target).expanduser()
    if str(target).endswith("/") or target_path.exists() and target_path.is_dir():
        return target_path / filename
    return target_path


def safe_download_name(name: str) -> str:
    filename = Path(str(name)).name
    return filename or "download"
