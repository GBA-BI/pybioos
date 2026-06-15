from typing import Any, Dict, Iterable, Optional

from bioos.errors import NotFoundError, ParameterError
from bioos.utils.common_tools import dict_str
from network.auth import BioOSBridgePassportProvider
from network.config import normalize_endpoint, resolve_repository_endpoint
from network.internal.http import NetworkHttpClient, quote_path_segment
from network.resource.datasets import DataSet, DataSetResource
from network.resource.drs import DRSResource
from network.resource.payload import build_params, extract_records, payload_to_dataframe


DATA_LIBRARY_PARAM_ALIASES = {
    "ids": "id",
    "display_name": "displayName",
    "organization_id": "organizationID",
    "order_by": "orderBy",
}


class DataLibraryResource:
    def __init__(
        self,
        repository_endpoint: Optional[str] = None,
        repository_client: Optional[NetworkHttpClient] = None,
        passport_provider: Optional[BioOSBridgePassportProvider] = None,
    ):
        self.passport_provider = passport_provider or BioOSBridgePassportProvider()
        self.repository_endpoint = resolve_repository_endpoint(repository_endpoint)
        self.repository_client = repository_client or NetworkHttpClient(
            endpoint=self.repository_endpoint,
            passport_provider=self.passport_provider,
            sign_requests=True,
        )

    def __repr__(self) -> str:
        return f"DataLibraryResource:\n{dict_str({'repository_endpoint': self.repository_endpoint})}"

    def list(
        self,
        page: Optional[int] = None,
        size: Optional[int] = None,
        order_by: Optional[str] = None,
        ids: Optional[Iterable[str]] = None,
        display_name: Optional[Iterable[str]] = None,
        organization_id: Optional[Iterable[str]] = None,
        raw: bool = False,
        **filters,
    ):
        params = build_params(
            DATA_LIBRARY_PARAM_ALIASES,
            filters,
            page=page,
            size=size,
            order_by=order_by,
            ids=ids,
            display_name=display_name,
            organization_id=organization_id,
        )
        payload = self.repository_client.get("/api/repository/data_library", params=params)
        if raw:
            return payload
        return payload_to_dataframe(payload)

    def user(
        self,
        page: Optional[int] = None,
        size: Optional[int] = None,
        order_by: Optional[str] = None,
        raw: bool = False,
        **filters,
    ):
        params = build_params(
            DATA_LIBRARY_PARAM_ALIASES,
            filters,
            page=page,
            size=size,
            order_by=order_by,
        )
        payload = self.repository_client.get("/api/repository/data_library/user", params=params)
        if raw:
            return payload
        return payload_to_dataframe(payload)

    def get(self, data_library_id: str, raw: bool = False):
        if not data_library_id:
            raise ParameterError("data_library_id")
        payload = self.list(ids=[data_library_id], raw=True)
        if raw:
            return payload
        records = extract_records(payload)
        for record in records:
            if isinstance(record, dict) and record.get("id") == data_library_id:
                return record
        if records:
            return records[0]
        raise NotFoundError("DataLibrary", data_library_id)

    def data_library(self, data_library_id: str, record: Optional[Dict[str, Any]] = None):
        return DataLibrary(
            data_library_id=data_library_id,
            resource=self,
            record=record,
        )


class DataLibrary:
    def __init__(
        self,
        data_library_id: str,
        resource: Optional[DataLibraryResource] = None,
        record: Optional[Dict[str, Any]] = None,
    ):
        if not data_library_id:
            raise ParameterError("data_library_id")
        self.data_library_id = data_library_id
        self.resource = resource or DataLibraryResource()
        self._record = record
        self._data_site_client_instances = None

    def __repr__(self) -> str:
        info = {
            "data_library_id": self.data_library_id,
            "api_endpoint": self.api_endpoint,
            "web_endpoint": self.web_endpoint,
            "drs_host": self.drs_host,
        }
        return f"DataLibrary:\n{dict_str(info)}"

    @property
    def id(self) -> str:
        return self.data_library_id

    @property
    def api_endpoint(self) -> Optional[str]:
        return self._field("APIEndpoint")

    @property
    def drs_host(self) -> Optional[str]:
        return self._field("DRSHost")

    @property
    def web_endpoint(self) -> Optional[str]:
        return self._field("webEndpoint")

    @property
    def organization_id(self) -> Optional[str]:
        return self._field("organizationID")

    @property
    def datasets(self) -> DataSetResource:
        return DataSetResource(
            repository_endpoint=self.resource.repository_endpoint,
            data_library_id=self.data_library_id,
            data_site_clients=self._data_site_clients(),
            repository_client=self.resource.repository_client,
            drs_resource=self.drs,
            passport_provider=self.resource.passport_provider,
        )

    @property
    def drs(self) -> DRSResource:
        return DRSResource(
            endpoint=self.drs_host,
            repository_endpoint=self.resource.repository_endpoint,
            passport_provider=self.resource.passport_provider,
        )

    def get(self) -> Dict[str, Any]:
        return self._ensure_record()

    def dataset(self, data_set_id: str) -> DataSet:
        return self.datasets.data_set(data_set_id)

    def data_file_client(self) -> Optional[NetworkHttpClient]:
        return self._data_site_client()

    def _data_site_client(self) -> Optional[NetworkHttpClient]:
        clients = self._data_site_clients()
        return clients[0] if clients else None

    def _data_site_clients(self):
        if self._data_site_client_instances is not None:
            return self._data_site_client_instances

        endpoints = []
        for endpoint in (self.web_endpoint, self.api_endpoint):
            if not endpoint:
                continue
            normalized = normalize_data_site_endpoint(endpoint)
            if normalized not in endpoints:
                endpoints.append(normalized)

        self._data_site_client_instances = [
            NetworkHttpClient(
                endpoint=endpoint,
                passport_provider=self.resource.passport_provider,
                sign_requests=False,
            )
            for endpoint in endpoints
        ]
        return self._data_site_client_instances

    def _field(self, key: str) -> Optional[str]:
        record = self._ensure_record(required=False)
        if not isinstance(record, dict):
            return None
        value = record.get(key)
        if value is None:
            return None
        return str(value)

    def _ensure_record(self, required: bool = True):
        if self._record is None:
            try:
                self._record = self.resource.get(self.data_library_id)
            except Exception:
                if required:
                    raise
                return None
        return self._record


def data_library_path(data_library_id: str) -> str:
    return f"/api/repository/data_library/{quote_path_segment(data_library_id)}"


def normalize_data_site_endpoint(endpoint: str) -> str:
    value = normalize_endpoint(endpoint)
    suffix = "/api/data-library"
    if value.endswith(suffix):
        return value[: -len(suffix)] or value
    return value
