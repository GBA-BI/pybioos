from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urlparse

import pandas as pd
from pandas import DataFrame

from bioos.errors import NotFoundError, ParameterError
from bioos.internal.repository import (
    RepositoryPassportProvider,
    RepositoryRestClient,
    quote_path_segment,
    resolve_drs_endpoint,
    resolve_repository_endpoint,
)
from bioos.utils.common_tools import SingletonType, dict_str


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

DEFAULT_DRS_ACCESS_ID = "https"


class DataSetResource(metaclass=SingletonType):
    def __init__(
        self,
        repository_endpoint: Optional[str] = None,
        drs_endpoint: Optional[str] = None,
        passport_provider: Optional[RepositoryPassportProvider] = None,
    ):
        provider = passport_provider or RepositoryPassportProvider()
        self.repository_endpoint = resolve_repository_endpoint(repository_endpoint)
        self.drs_endpoint = resolve_drs_endpoint(drs_endpoint)
        self.repository_client = RepositoryRestClient(
            endpoint=self.repository_endpoint,
            passport_provider=provider,
            sign_requests=True,
        )
        self.drs_client = RepositoryRestClient(
            endpoint=self.drs_endpoint,
            passport_provider=provider,
            sign_requests=False,
        )

    def __repr__(self) -> str:
        return f"DataSetResource:\n{dict_str({'repository_endpoint': self.repository_endpoint, 'drs_endpoint': self.drs_endpoint})}"

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
        """List BioOS Network data sets.

        Set ``raw=True`` to return the repository API response without
        converting result rows to a DataFrame.
        """
        params = _build_params(
            DATA_SET_PARAM_ALIASES,
            filters,
            page=page,
            size=size,
            order_by=order_by,
            search_word=search_word,
            data_library_id=data_library_id,
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
        payload = self.repository_client.get("/api/repository/data_set", params=params)
        if raw:
            return payload
        return _payload_to_dataframe(payload)

    def get(
        self,
        data_set_id: str,
        data_library_id: Optional[str] = None,
        display_level: str = "Full",
        raw: bool = False,
        **filters,
    ):
        """Get one BioOS Network data set by ID.

        The repository API exposes this through the list endpoint with an
        ``id`` filter, so the SDK keeps the user-facing operation as ``get``.
        Set ``raw=True`` to return the original list payload.
        """
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

        records = _extract_records(payload)
        for record in records:
            if isinstance(record, dict) and record.get("id") == data_set_id:
                return record
        if records:
            return records[0]
        raise NotFoundError("DataSet", data_set_id)

    def data_set(self, data_set_id: str, data_library_id: Optional[str] = None):
        return DataSet(
            data_set_id=data_set_id,
            data_library_id=data_library_id,
            resource=self,
        )

    def drs_object(self, object_id: str) -> Dict[str, Any]:
        object_id = _normalize_drs_object_id(object_id)
        return self.drs_client.get(f"/ga4gh/drs/v1/objects/{quote_path_segment(object_id)}")

    def drs_access(
        self,
        object_id: str,
        access_id: str = DEFAULT_DRS_ACCESS_ID,
    ) -> Dict[str, Any]:
        object_id = _normalize_drs_object_id(object_id)
        if not access_id:
            raise ParameterError("access_id")
        access = _access_from_drs_object(self.drs_object(object_id), access_id)
        if access:
            return access
        return self.drs_client.get(
            "/ga4gh/drs/v1/objects/"
            f"{quote_path_segment(object_id)}/access/{quote_path_segment(access_id)}"
        )

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
        object_id = _normalize_drs_object_id(object_id)
        if not target:
            raise ParameterError("target")

        info = object_info if object_info is not None else self.drs_object(object_id)
        filename = _safe_download_name(object_name or _dict_get(info, "name") or object_id)
        target_path = _resolve_download_target(target, filename)
        if target_path.exists() and not overwrite:
            raise FileExistsError(f"target already exists: {target_path}")

        access = _access_from_drs_object(info, access_id) or self.drs_access(object_id, access_id=access_id)
        url = _extract_access_url(access)
        headers = _extract_access_headers(access)
        result = self.drs_client.download_url(
            url,
            str(target_path),
            headers=headers,
            chunk_size=chunk_size,
        )
        result.update(
            {
                "success": True,
                "object_id": object_id,
                "access_id": access_id,
                "name": filename,
            }
        )
        return result


class DataSet(metaclass=SingletonType):
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
        self.resource = resource or DataSetResource()

    def __repr__(self) -> str:
        info = {
            "data_set_id": self.data_set_id,
            "data_library_id": self.data_library_id,
        }
        return f"DataSet:\n{dict_str(info)}"

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

        params = _build_data_file_params(
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
        payload = self.resource.repository_client.get(
            f"/api/repository/data_set/{quote_path_segment(self.data_set_id)}/data_file",
            params=params,
        )
        if raw:
            return payload
        return _payload_to_dataframe(payload)

    def drs_object(self, object_id: str) -> Dict[str, Any]:
        return self.resource.drs_object(object_id)

    def get(self, display_level: str = "Full", raw: bool = False, **filters):
        return self.resource.get(
            self.data_set_id,
            data_library_id=self.data_library_id,
            display_level=display_level,
            raw=raw,
            **filters,
        )

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

        params = _build_data_file_params(
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
        payload = self.resource.repository_client.get(
            f"/api/repository/data_set/{quote_path_segment(self.data_set_id)}/data_file/ids",
            params=params,
        )
        if raw:
            return payload
        return _extract_ids(payload)

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
        records = _extract_records(payload)
        target_dir = Path(target).expanduser()
        target_dir.mkdir(parents=True, exist_ok=True)

        downloads = []
        failures = []
        for record in records:
            try:
                object_id = _file_record_object_id(record)
                name = _safe_download_name(_dict_get(record, "name") or object_id)
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
            "total": len(records),
            "downloaded_count": len(downloads),
            "failed_count": len(failures),
            "downloads": downloads,
            "failures": failures,
        }


def _build_params(aliases: Dict[str, str], filters: Dict[str, Any], **values) -> Dict[str, Any]:
    params = {}
    for key, value in values.items():
        if value is not None:
            params[aliases.get(key, key)] = value
    for key, value in filters.items():
        if value is not None:
            params[aliases.get(key, key)] = value
    return params


def _build_data_file_params(library_id: str, filters: Dict[str, Any], **values) -> Dict[str, Any]:
    return _build_params(
        DATA_FILE_PARAM_ALIASES,
        filters,
        data_library_id=library_id,
        **values,
    )


def _payload_to_dataframe(payload: Any) -> DataFrame:
    records = _extract_records(payload)
    if not records:
        return pd.DataFrame()
    return pd.DataFrame.from_records(records)


def _extract_records(payload: Any):
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("Items", "items", "Data", "data", "Rows", "rows", "Results", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _extract_records(value)
            if nested:
                return nested
    result = payload.get("Result")
    if isinstance(result, (dict, list)):
        return _extract_records(result)
    return []


def _extract_ids(payload: Any):
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("ids", "IDs", "Items", "items", "Data", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            if value and isinstance(value[0], dict):
                return [item.get("id") or item.get("ID") for item in value if item.get("id") or item.get("ID")]
            return value
        if isinstance(value, dict):
            nested = _extract_ids(value)
            if nested:
                return nested
    result = payload.get("Result")
    if isinstance(result, (dict, list)):
        return _extract_ids(result)
    return []


def _normalize_drs_object_id(object_id: str) -> str:
    if not object_id:
        raise ParameterError("object_id")
    value = str(object_id).strip()
    if value.startswith("drs://"):
        parsed = urlparse(value)
        value = parsed.path.strip("/") or parsed.netloc
    if not value:
        raise ParameterError("object_id")
    return value


def _dict_get(value: Any, key: str):
    if isinstance(value, dict):
        return value.get(key)
    return None


def _extract_access_url(access: Dict[str, Any]) -> str:
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


def _extract_access_headers(access: Dict[str, Any]) -> Dict[str, str]:
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


def _access_from_drs_object(object_info: Dict[str, Any], access_id: str) -> Optional[Dict[str, Any]]:
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


def _resolve_download_target(target: str, filename: str) -> Path:
    target_path = Path(target).expanduser()
    if str(target).endswith("/") or target_path.exists() and target_path.is_dir():
        return target_path / filename
    return target_path


def _safe_download_name(name: str) -> str:
    filename = Path(str(name)).name
    return filename or "download"


def _file_record_object_id(record: Any) -> str:
    if isinstance(record, dict):
        drs_url = record.get("drsURL") or record.get("drs_url") or record.get("DRSURL")
        if drs_url:
            return _normalize_drs_object_id(drs_url)
        value = record.get("id") or record.get("ID")
        if value:
            return _normalize_drs_object_id(value)
    raise ParameterError("object_id", "data file record does not include id or drsURL")
