from __future__ import annotations

from typing import Any, Dict, Optional

from bioos.service.rest_base import RestClient


class RepositoryClient(RestClient):
    def list_datasets(
        self,
        *,
        page: Optional[int] = None,
        size: Optional[int] = None,
        display_level: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> Any:
        params: Dict[str, Any] = {}
        if page is not None:
            params["page"] = page
        if size is not None:
            params["size"] = size
        if display_level:
            params["displayLevel"] = display_level
        if order_by:
            params["orderBy"] = order_by
        return self.request("GET", "/api/repository/data_set", params=params)

    def get_dataset(self, data_set_id: str) -> Any:
        return self.request("GET", "/api/repository/data_set", params={"id": data_set_id})

    def export_dataset(self, data_set_id: str, payload: Optional[Dict[str, Any]] = None) -> Any:
        return self.request("POST", f"/api/repository/data_set/{data_set_id}/export", json_body=payload or {})

    def import_dataset(self, payload: Dict[str, Any]) -> Any:
        return self.request("POST", "/api/repository/data_set/import", json_body=payload)

    def get_dataset_archive_access(self, *, path: Optional[str] = None) -> Any:
        params: Dict[str, Any] = {}
        if path:
            params["path"] = path
        return self.request("GET", "/api/repository/data_set/archive/tos_access", params=params)

    def list_dataset_files(
        self,
        data_set_id: str,
        *,
        page: Optional[int] = None,
        size: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> Any:
        params: Dict[str, Any] = {}
        if page is not None:
            params["page"] = page
        if size is not None:
            params["size"] = size
        if order_by:
            params["orderBy"] = order_by
        return self.request("GET", f"/api/repository/data_set/{data_set_id}/data_file", params=params)

    def list_dataset_file_ids(self, data_set_id: str) -> Any:
        return self.request("GET", f"/api/repository/data_set/{data_set_id}/data_file/ids")

    def list_dacs(
        self,
        *,
        page: Optional[int] = None,
        size: Optional[int] = None,
        limit: Optional[int] = None,
        scope: Optional[list[str]] = None,
    ) -> Any:
        params: Dict[str, Any] = {}
        if page is not None:
            params["page"] = page
        if size is not None:
            params["size"] = size
        if limit is not None:
            params["limit"] = limit
        if scope:
            params["scope"] = scope
        return self.request("GET", "/api/repository/dac", params=params)

    def list_applications(
        self,
        *,
        page: Optional[int] = None,
        size: Optional[int] = None,
        app_type: Optional[str] = None,
        field: Optional[str] = None,
        show_pending_approval: Optional[bool] = None,
    ) -> Any:
        params: Dict[str, Any] = {}
        if page is not None:
            params["page"] = page
        if size is not None:
            params["size"] = size
        if app_type:
            params["type"] = app_type
        if field:
            params["field"] = field
        if show_pending_approval is not None:
            params["showPendingApproval"] = str(show_pending_approval).lower()
        return self.request("GET", "/api/repository/application", params=params)

    def list_admins(self) -> Any:
        return self.request("GET", "/pylons/api/v1/admins")

    def get_organization_names(self, organization_id: str) -> Any:
        return self.request("GET", "/pylons/api/v1/organizationNames", params={"id": organization_id})

    def get_identity(self, identity_id: str) -> Any:
        return self.request("GET", "/pylons/api/v1/identities", params={"id": identity_id})

    def list_libraries(self) -> Any:
        return self.request("GET", "/api/repository/data_library")

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
        return self.request("GET", "/api/repository/schema_job", params=params)

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

    def get_datasite_pre_signed_url(self, filename: str) -> Any:
        return self.request("GET", "/api/repository/data_site/pre_signed_url", params={"filename": filename})

    def create_dac(self, payload: Dict[str, Any]) -> Any:
        return self.request("POST", "/api/repository/dac", json_body=payload)

    def update_dac(self, dac_id: str, payload: Dict[str, Any]) -> Any:
        return self.request("PUT", f"/api/repository/dac/{dac_id}", json_body=payload)

    def delete_dac(self, dac_id: str) -> Any:
        return self.request("DELETE", f"/api/repository/dac/{dac_id}")

    def check_dac(self, *, name: Optional[str] = None) -> Any:
        params: Dict[str, Any] = {}
        if name:
            params["name"] = name
        return self.request("GET", "/api/repository/dac/check", params=params)

    def list_dac_members(self, dac_id: str, *, page: Optional[int] = None, size: Optional[int] = None) -> Any:
        params: Dict[str, Any] = {}
        if page is not None:
            params["page"] = page
        if size is not None:
            params["size"] = size
        return self.request("GET", f"/api/repository/dac/{dac_id}/member", params=params)

    def upsert_dac_member(self, dac_id: str, payload: Dict[str, Any]) -> Any:
        return self.request("PUT", f"/api/repository/dac/{dac_id}/member", json_body=payload)

    def remove_dac_member(self, dac_id: str, payload: Dict[str, Any]) -> Any:
        return self.request("DELETE", f"/api/repository/dac/{dac_id}/member", json_body=payload)
