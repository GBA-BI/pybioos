from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from requests.exceptions import ConnectionError, SSLError, Timeout

from bioos.errors import NotFoundError, ParameterError
from bioos.utils.common_tools import dict_str
from network.auth import BioOSBridgePassportProvider
from network.config import resolve_repository_endpoint
from network.internal.http import NetworkHttpClient, quote_path_segment
from network.resource.drs import DEFAULT_DRS_ACCESS_ID, DRSResource, safe_download_name
from network.resource.payload import build_params, extract_ids, extract_records, payload_to_dataframe


DATA_SET_PARAM_ALIASES = {
    "order_by": "orderBy",
    "search_word": "searchWord",
    "data_library_id": "dataLibraryID",
    "ids": "id",
    "access_control": "accessControl",
    "project_data_type": "projectDataType",
    "user_id": "userID",
    "display_level": "displayLevel",
    "data_file_id": "dataFileID",
}

DATA_FILE_PARAM_ALIASES = {
    "data_library_id": "dataLibraryID",
    "order_by": "orderBy",
    "search_scope": "searchScope",
    "search_word": "searchWord",
    "time_search_scope": "timeSearchScope",
    "start_time": "startTime",
    "end_time": "endTime",
    "ids": "id",
    "file_type": "fileType",
}

DATA_SITE_FALLBACK_ERRORS = (ConnectionError, SSLError, Timeout)


class DataSetResource:
    """Data set catalogue.

    Without ``data_library_id`` this resource represents the Repository-level
    cross-library discovery catalogue. With a ``DataLibrary`` context it
    represents data sets under that data site.
    """

    def __init__(
        self,
        repository_endpoint: Optional[str] = None,
        data_library_id: Optional[str] = None,
        data_site_client: Optional[NetworkHttpClient] = None,
        data_site_clients: Optional[Iterable[NetworkHttpClient]] = None,
        repository_client: Optional[NetworkHttpClient] = None,
        drs_resource: Optional[DRSResource] = None,
        passport_provider: Optional[BioOSBridgePassportProvider] = None,
    ):
        provider = passport_provider or BioOSBridgePassportProvider()
        self.data_library_id = data_library_id
        self.repository_endpoint = resolve_repository_endpoint(repository_endpoint)
        self.repository_client = repository_client or NetworkHttpClient(
            endpoint=self.repository_endpoint,
            passport_provider=provider,
            sign_requests=True,
        )
        self.data_site_clients = self._normalize_data_site_clients(data_site_clients, data_site_client)
        self.data_site_client = self.data_site_clients[0] if self.data_site_clients else None
        self.drs = drs_resource or DRSResource(
            passport_provider=provider,
        )

    def __repr__(self) -> str:
        info = {
            "repository_endpoint": self.repository_endpoint,
            "data_library_id": self.data_library_id,
            "data_site_endpoints": [
                getattr(client, "endpoint", None)
                for client in self._data_site_client_candidates()
            ],
        }
        return f"DataSetResource:\n{dict_str(info)}"

    def list(
        self,
        page: Optional[int] = None,
        size: Optional[int] = None,
        order_by: Optional[str] = None,
        search_word: Optional[str] = None,
        data_library_id: Optional[str] = None,
        ids: Optional[Iterable[str]] = None,
        access_control: Optional[str] = None,
        project_data_type: Optional[Iterable[str]] = None,
        category: Optional[Iterable[str]] = None,
        user_id: Optional[str] = None,
        catalogue: Optional[Iterable[str]] = None,
        display_level: Optional[str] = None,
        group: Optional[str] = None,
        data_file_id: Optional[str] = None,
        raw: bool = False,
        **filters,
    ):
        library_id = data_library_id or getattr(self, "data_library_id", None)
        params = build_params(
            DATA_SET_PARAM_ALIASES,
            filters,
            page=page,
            size=size,
            order_by=order_by,
            search_word=search_word,
            data_library_id=None if self._use_data_site(library_id) else library_id,
            ids=ids,
            access_control=access_control,
            project_data_type=project_data_type,
            category=category,
            user_id=user_id,
            catalogue=catalogue,
            display_level=display_level,
            group=group,
            data_file_id=data_file_id,
        )
        if self._use_data_site(library_id):
            try:
                payload = self._data_site_get("/api/data-library/data_set", params=params)
            except DATA_SITE_FALLBACK_ERRORS:
                fallback_params = dict(params)
                fallback_params["dataLibraryID"] = library_id
                payload = self.repository_client.get("/api/repository/data_set", params=fallback_params)
        else:
            payload = self.repository_client.get("/api/repository/data_set", params=params)
        if raw:
            return payload
        return payload_to_dataframe(payload)

    def get(
        self,
        data_set_id: str,
        data_library_id: Optional[str] = None,
        display_level: str = "Full",
        raw: bool = False,
        **filters,
    ):
        if not data_set_id:
            raise ParameterError("data_set_id")

        payload = self.list(
            ids=[data_set_id],
            data_library_id=data_library_id,
            display_level=display_level,
            raw=True,
            **filters,
        )
        if raw:
            return payload

        records = extract_records(payload)
        for record in records:
            if isinstance(record, dict) and record.get("id") == data_set_id:
                return record
        if records:
            return records[0]
        raise NotFoundError("DataSet", data_set_id)

    def data_set(self, data_set_id: str, data_library_id: Optional[str] = None):
        return DataSet(
            data_set_id=data_set_id,
            data_library_id=data_library_id or self.data_library_id,
            resource=self,
        )

    def drs_object(self, object_id: str) -> Dict[str, Any]:
        return self.drs.object(object_id)

    def drs_access(
        self,
        object_id: str,
        access_id: str = DEFAULT_DRS_ACCESS_ID,
    ) -> Dict[str, Any]:
        return self.drs.access(object_id, access_id=access_id)

    def download_drs_object(
        self,
        object_id: str,
        target: str = ".",
        access_id: str = DEFAULT_DRS_ACCESS_ID,
        overwrite: bool = False,
        chunk_size: int = 1024 * 1024,
        object_info: Optional[Dict[str, Any]] = None,
        object_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.drs.download_object(
            object_id,
            target=target,
            access_id=access_id,
            overwrite=overwrite,
            chunk_size=chunk_size,
            object_info=object_info,
            object_name=object_name,
        )

    def _dataset_collection_client_and_path(self, library_id: Optional[str]):
        if self._use_data_site(library_id):
            return self.data_site_client, "/api/data-library/data_set"
        return self.repository_client, "/api/repository/data_set"

    def _data_file_client_and_path(self, data_set_id: str, library_id: Optional[str]):
        if self._use_data_site(library_id):
            return self.data_site_client, f"/api/data-library/data_set/{quote_path_segment(data_set_id)}/data_file"
        return self.repository_client, f"/api/repository/data_set/{quote_path_segment(data_set_id)}/data_file"

    def _data_file_ids_client_and_path(self, data_set_id: str, library_id: Optional[str]):
        client, path = self._data_file_client_and_path(data_set_id, library_id)
        return client, f"{path}/ids"

    def _use_data_site(self, library_id: Optional[str]) -> bool:
        return bool(
            self._data_site_client_candidates()
            and library_id
            and library_id == getattr(self, "data_library_id", None)
        )

    def _data_site_get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        last_error = None
        for client in self._data_site_client_candidates():
            try:
                return client.get(path, params=params)
            except DATA_SITE_FALLBACK_ERRORS as exc:
                last_error = exc
        if last_error:
            raise last_error
        raise RuntimeError("No data site endpoint is available.")

    def _data_site_client_candidates(self):
        clients = getattr(self, "data_site_clients", None)
        if clients is not None:
            return list(clients)
        client = getattr(self, "data_site_client", None)
        return [client] if client else []

    @staticmethod
    def _normalize_data_site_clients(data_site_clients, data_site_client):
        if data_site_clients is None:
            return [data_site_client] if data_site_client else []
        return [client for client in data_site_clients if client]


class DataSet:
    def __init__(
        self,
        data_set_id: str,
        data_library_id: Optional[str] = None,
        resource: Optional[DataSetResource] = None,
    ):
        if not data_set_id:
            raise ParameterError("data_set_id")
        self.data_set_id = data_set_id
        self.data_library_id = data_library_id
        self.resource = resource or DataSetResource(data_library_id=data_library_id)

    def __repr__(self) -> str:
        info = {
            "data_set_id": self.data_set_id,
            "data_library_id": self.data_library_id,
        }
        return f"DataSet:\n{dict_str(info)}"

    def get(self, display_level: str = "Full", raw: bool = False, **filters):
        return self.resource.get(
            self.data_set_id,
            data_library_id=self.data_library_id,
            display_level=display_level,
            raw=raw,
            **filters,
        )

    def files(
        self,
        data_library_id: Optional[str] = None,
        page: Optional[int] = None,
        size: Optional[int] = None,
        order_by: Optional[str] = None,
        search_scope: Optional[Iterable[str]] = None,
        search_word: Optional[str] = None,
        time_search_scope: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        ids: Optional[Iterable[str]] = None,
        file_type: Optional[Iterable[str]] = None,
        raw: bool = False,
        **filters,
    ):
        library_id = data_library_id or self.data_library_id or filters.pop("dataLibraryID", None)
        if not library_id:
            raise ParameterError("data_library_id")

        params = self._build_data_file_params(
            library_id=library_id,
            filters=filters,
            page=page,
            size=size,
            order_by=order_by,
            search_scope=search_scope,
            search_word=search_word,
            time_search_scope=time_search_scope,
            start_time=start_time,
            end_time=end_time,
            ids=ids,
            file_type=file_type,
        )
        data_site_path = f"/api/data-library/data_set/{quote_path_segment(self.data_set_id)}/data_file"
        if self.resource._use_data_site(library_id):
            try:
                payload = self.resource._data_site_get(data_site_path, params=params)
            except DATA_SITE_FALLBACK_ERRORS:
                fallback_params = dict(params)
                fallback_params["dataLibraryID"] = library_id
                payload = self.resource.repository_client.get(
                    f"/api/repository/data_set/{quote_path_segment(self.data_set_id)}/data_file",
                    params=fallback_params,
                )
        else:
            payload = self.resource.repository_client.get(
                f"/api/repository/data_set/{quote_path_segment(self.data_set_id)}/data_file",
                params=params,
            )
        if raw:
            return payload
        return payload_to_dataframe(payload)

    def file_ids(
        self,
        data_library_id: Optional[str] = None,
        search_scope: Optional[Iterable[str]] = None,
        search_word: Optional[str] = None,
        time_search_scope: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        ids: Optional[Iterable[str]] = None,
        file_type: Optional[Iterable[str]] = None,
        raw: bool = False,
        **filters,
    ):
        library_id = data_library_id or self.data_library_id or filters.pop("dataLibraryID", None)
        if not library_id:
            raise ParameterError("data_library_id")

        params = self._build_data_file_params(
            library_id=library_id,
            filters=filters,
            search_scope=search_scope,
            search_word=search_word,
            time_search_scope=time_search_scope,
            start_time=start_time,
            end_time=end_time,
            ids=ids,
            file_type=file_type,
        )
        data_site_path = f"/api/data-library/data_set/{quote_path_segment(self.data_set_id)}/data_file/ids"
        if self.resource._use_data_site(library_id):
            try:
                payload = self.resource._data_site_get(data_site_path, params=params)
            except DATA_SITE_FALLBACK_ERRORS:
                fallback_params = dict(params)
                fallback_params["dataLibraryID"] = library_id
                payload = self.resource.repository_client.get(
                    f"/api/repository/data_set/{quote_path_segment(self.data_set_id)}/data_file/ids",
                    params=fallback_params,
                )
        else:
            payload = self.resource.repository_client.get(
                f"/api/repository/data_set/{quote_path_segment(self.data_set_id)}/data_file/ids",
                params=params,
            )
        if raw:
            return payload
        return extract_ids(payload)

    def download_files(
        self,
        target: str,
        data_library_id: Optional[str] = None,
        access_id: str = DEFAULT_DRS_ACCESS_ID,
        overwrite: bool = False,
        continue_on_error: bool = False,
        page: Optional[int] = None,
        size: Optional[int] = None,
        order_by: Optional[str] = None,
        search_scope: Optional[Iterable[str]] = None,
        search_word: Optional[str] = None,
        time_search_scope: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        ids: Optional[Iterable[str]] = None,
        file_type: Optional[Iterable[str]] = None,
        **filters,
    ) -> Dict[str, Any]:
        if not target:
            raise ParameterError("target")

        payload = self.files(
            data_library_id=data_library_id,
            page=page,
            size=size,
            order_by=order_by,
            search_scope=search_scope,
            search_word=search_word,
            time_search_scope=time_search_scope,
            start_time=start_time,
            end_time=end_time,
            ids=ids,
            file_type=file_type,
            raw=True,
            **filters,
        )
        records = extract_records(payload)
        target_dir = Path(target).expanduser()
        target_dir.mkdir(parents=True, exist_ok=True)

        downloads = []
        failures = []
        for record in records:
            try:
                object_id = file_record_object_id(record)
                name = safe_download_name(dict_get(record, "name") or object_id)
                result = self.resource.download_drs_object(
                    object_id,
                    target=str(target_dir / name),
                    access_id=access_id,
                    overwrite=overwrite,
                    object_info=record if isinstance(record, dict) else None,
                    object_name=name,
                )
                downloads.append(result)
            except Exception as exc:
                failure = {
                    "record": record,
                    "error": str(exc),
                }
                failures.append(failure)
                if not continue_on_error:
                    raise

        return {
            "success": len(failures) == 0,
            "data_set_id": self.data_set_id,
            "data_library_id": data_library_id or self.data_library_id,
            "total": len(records),
            "downloaded_count": len(downloads),
            "failed_count": len(failures),
            "downloads": downloads,
            "failures": failures,
        }

    def drs_object(self, object_id: str) -> Dict[str, Any]:
        return self.resource.drs_object(object_id)

    def _build_data_file_params(self, library_id: str, filters: Dict[str, Any], **values) -> Dict[str, Any]:
        params = build_params(
            DATA_FILE_PARAM_ALIASES,
            filters,
            data_library_id=None if self.resource._use_data_site(library_id) else library_id,
            **values,
        )
        return params


def dict_get(value: Any, key: str):
    if isinstance(value, dict):
        return value.get(key)
    return None


def file_record_object_id(record: Any) -> str:
    if isinstance(record, dict):
        drs_url = record.get("drsURL") or record.get("drs_url") or record.get("DRSURL")
        if drs_url:
            return str(drs_url).strip()
        value = record.get("id") or record.get("ID")
        if value:
            return str(value).strip()
    raise ParameterError("object_id", "data file record does not include id or drsURL")
