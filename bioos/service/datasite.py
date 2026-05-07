from __future__ import annotations

from typing import Any, Dict, Optional

from bioos.service.rest_base import RestClient


class DataSiteClient(RestClient):
    def list_applications(self, *, page: Optional[int] = None, size: Optional[int] = None) -> Any:
        params: Dict[str, Any] = {}
        if page is not None:
            params["page"] = page
        if size is not None:
            params["size"] = size
        return self.request("GET", "/api/data-library/application", params=params)

    def permit_application(self, application_id: str, task_id: str) -> Any:
        return self.request("PATCH", f"/api/data-library/application/{application_id}/permit/{task_id}")

    def reject_application(self, application_id: str, task_id: str) -> Any:
        return self.request("PATCH", f"/api/data-library/application/{application_id}/reject/{task_id}")

    def list_datasets(
        self,
        *,
        page: Optional[int] = None,
        size: Optional[int] = None,
        tab: Optional[str] = None,
        display_level: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> Any:
        params: Dict[str, Any] = {}
        if page is not None:
            params["page"] = page
        if size is not None:
            params["size"] = size
        if tab:
            params["tab"] = tab
        if display_level:
            params["displayLevel"] = display_level
        if order_by:
            params["orderBy"] = order_by
        return self.request("GET", "/api/data-library/data_set", params=params)

    def get_dataset(self, data_set_id: str) -> Any:
        return self.request("GET", "/api/data-library/data_set", params={"id": data_set_id})

    def create_dataset(self, payload: Dict[str, Any]) -> Any:
        return self.request("POST", "/api/data-library/data_set", json_body=payload)

    def apply_dataset(self, payload: Dict[str, Any]) -> Any:
        return self.request("POST", "/api/data-library/data_set/apply", json_body=payload)

    def update_dataset(self, data_set_id: str, payload: Dict[str, Any]) -> Any:
        return self.request("PUT", f"/api/data-library/data_set/{data_set_id}", json_body=payload)

    def update_dataset_config(self, data_set_id: str, payload: Dict[str, Any]) -> Any:
        return self.request("PATCH", f"/api/data-library/data_set/{data_set_id}/config", json_body=payload)

    def delete_dataset(self, data_set_id: str) -> Any:
        return self.request("DELETE", f"/api/data-library/data_set/{data_set_id}")

    def get_dataset_permission(self, data_set_id: str) -> Any:
        return self.request("GET", f"/api/data-library/data_set/{data_set_id}/permission")

    def release_dataset(self, data_set_id: str, payload: Optional[Dict[str, Any]] = None) -> Any:
        return self.request("PUT", f"/api/data-library/data_set/{data_set_id}/release", json_body=payload or {})

    def revoke_dataset(self, data_set_id: str, payload: Optional[Dict[str, Any]] = None) -> Any:
        return self.request("PUT", f"/api/data-library/data_set/{data_set_id}/revoke", json_body=payload or {})

    def get_dataset_archive_access(self, *, path: Optional[str] = None) -> Any:
        params: Dict[str, Any] = {}
        if path:
            params["path"] = path
        return self.request("GET", "/api/data-library/data_set/archive/tos_access", params=params)

    def check_dataset(self, **params: Any) -> Any:
        filtered = {key: value for key, value in params.items() if value is not None}
        return self.request("GET", "/api/data-library/data_set/check", params=filtered)

    def export_dataset(self, data_set_id: str, payload: Optional[Dict[str, Any]] = None) -> Any:
        return self.request("POST", f"/api/data-library/data_set/{data_set_id}/export", json_body=payload or {})

    def import_dataset(self, payload: Dict[str, Any]) -> Any:
        return self.request("POST", "/api/data-library/data_set/import", json_body=payload)

    def list_dataset_files(self, data_set_id: str, *, page: Optional[int] = None, size: Optional[int] = None, order_by: Optional[str] = None) -> Any:
        params: Dict[str, Any] = {}
        if page is not None:
            params["page"] = page
        if size is not None:
            params["size"] = size
        if order_by:
            params["orderBy"] = order_by
        return self.request("GET", f"/api/data-library/data_set/{data_set_id}/data_file", params=params)

    def upsert_dataset_files(self, data_set_id: str, payload: Dict[str, Any]) -> Any:
        return self.request("PUT", f"/api/data-library/data_set/{data_set_id}/data_file", json_body=payload)

    def delete_dataset_files(self, data_set_id: str, payload: Optional[Dict[str, Any]] = None) -> Any:
        return self.request("DELETE", f"/api/data-library/data_set/{data_set_id}/data_file", json_body=payload or {})

    def list_dataset_file_ids(self, data_set_id: str) -> Any:
        return self.request("GET", f"/api/data-library/data_set/{data_set_id}/data_file/ids")

    def list_files(self, *, page: Optional[int] = None, size: Optional[int] = None, order_by: Optional[str] = None) -> Any:
        params: Dict[str, Any] = {}
        if page is not None:
            params["page"] = page
        if size is not None:
            params["size"] = size
        if order_by:
            params["orderBy"] = order_by
        return self.request("GET", "/api/data-library/data_file", params=params)

    def list_file_types(self) -> Any:
        return self.request("GET", "/api/data-library/data_file/types")

    def list_schema_jobs(
        self,
        *,
        page: Optional[int] = None,
        size: Optional[int] = None,
        order_by: Optional[str] = None,
        scope: Optional[list[str]] = None,
        job_type: Optional[str] = None,
    ) -> Any:
        params: Dict[str, Any] = {}
        if page is not None:
            params["page"] = page
        if size is not None:
            params["size"] = size
        if order_by:
            params["orderBy"] = order_by
        if scope:
            params["scope"] = scope
        if job_type:
            params["type"] = job_type
        return self.request("GET", "/api/data-library/schema_job", params=params)

    def delete_schema_job(self, job_id: str) -> Any:
        return self.request("DELETE", f"/api/data-library/schema_job/{job_id}")

    def get_export_schema_jobs(
        self,
        *,
        page: Optional[int] = None,
        size: Optional[int] = None,
        order_by: Optional[str] = None,
        scope: Optional[list[str]] = None,
    ) -> Any:
        return self.list_schema_jobs(page=page, size=size, order_by=order_by, scope=scope, job_type="export")

    def get_import_schema_jobs(
        self,
        *,
        page: Optional[int] = None,
        size: Optional[int] = None,
        order_by: Optional[str] = None,
        scope: Optional[list[str]] = None,
    ) -> Any:
        return self.list_schema_jobs(page=page, size=size, order_by=order_by, scope=scope, job_type="import")

    def get_drs_object(self, object_id: str) -> Any:
        return self.request("GET", f"/ga4gh/drs/v1/objects/{object_id}")

    def post_drs_object(self, object_id: str, payload: Dict[str, Any]) -> Any:
        return self.request("POST", f"/ga4gh/drs/v1/objects/{object_id}", json_body=payload)

    def get_drs_access(self, object_id: str, access_id: str) -> Any:
        return self.request("GET", f"/ga4gh/drs/v1/objects/{object_id}/access/{access_id}")

    def post_drs_access(self, object_id: str, access_id: str, payload: Dict[str, Any]) -> Any:
        return self.request("POST", f"/ga4gh/drs/v1/objects/{object_id}/access/{access_id}", json_body=payload)
