import json
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote, urlencode, urlparse

import requests
from volcengine.Credentials import Credentials
from volcengine.auth.SignerV4 import SignerV4
from volcengine.base.Request import Request

from bioos.config import Config
from bioos.errors import ConfigurationError
from network.auth import BioOSBridgePassportProvider
from network.config import normalize_endpoint


class NetworkHttpClient:
    """HTTP client for BioOS Network Repository, Data Library, and DRS APIs."""

    def __init__(
        self,
        endpoint: str,
        passport_provider: Optional[BioOSBridgePassportProvider] = None,
        sign_requests: bool = False,
        service_name: str = "bio",
        region: Optional[str] = None,
        timeout: int = 60,
        session: Optional[requests.Session] = None,
    ):
        self.endpoint = normalize_endpoint(endpoint)
        self.passport_provider = passport_provider or BioOSBridgePassportProvider()
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

    def put(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self.request("PUT", path, params=params, body=body)

    def patch(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self.request("PATCH", path, params=params, body=body)

    def delete(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self.request("DELETE", path, params=params, body=body)

    def options(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.request("OPTIONS", path, params=params)

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


RepositoryRestClient = NetworkHttpClient


def quote_path_segment(value: str) -> str:
    return quote(str(value).strip(), safe="")


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
