import ast
import json
from typing import Iterable, Optional

from bioos.config import Config
from bioos.utils.common_tools import SingletonType


class UsageResource(metaclass=SingletonType):
    VALID_ASSET_TYPES = {"WorkspaceVisit", "WorkflowUse"}
    VALID_RESOURCE_TYPES = {"cpu", "memory", "storage", "tos", "gpu"}

    def get_asset_usage_data(self, start_time: int, end_time: int, type_: str):
        params = {
            "StartTime": self._normalize_time(start_time, "start_time"),
            "EndTime": self._normalize_time(end_time, "end_time"),
            "Type": self._validate_asset_type(type_),
        }
        return self._call_service("get_asset_usage_data", params)

    def list_asset_usage(self, start_time: int, end_time: int, type_: str):
        params = {
            "StartTime": self._normalize_time(start_time, "start_time"),
            "EndTime": self._normalize_time(end_time, "end_time"),
            "Type": self._validate_asset_type(type_),
        }
        return self._call_service("list_asset_usage", params)

    def get_total_asset_usage(self, start_time: int, end_time: int, type_: str):
        params = {
            "StartTime": self._normalize_time(start_time, "start_time"),
            "EndTime": self._normalize_time(end_time, "end_time"),
            "Type": self._validate_asset_type(type_),
        }
        return self._call_service("get_total_asset_usage", params)

    def get_resource_usage_data(
        self,
        start_time: int,
        end_time: int,
        type_: str,
        sub_dimensions: Optional[Iterable[str]] = None,
    ):
        params = {
            "StartTime": self._normalize_time(start_time, "start_time"),
            "EndTime": self._normalize_time(end_time, "end_time"),
            "Type": self._validate_resource_type(type_),
        }
        normalized_sub_dimensions = self._normalize_sub_dimensions(sub_dimensions)
        if normalized_sub_dimensions:
            params["SubDimensions"] = normalized_sub_dimensions
        return self._call_service("get_resource_usage_data", params)

    def list_workspace_resource_usage(self, start_time: int, end_time: int):
        params = {
            "StartTime": self._normalize_time(start_time, "start_time"),
            "EndTime": self._normalize_time(end_time, "end_time"),
        }
        return self._call_service("list_workspace_resource_usage", params)

    def list_user_resource_usage(self, start_time: int, end_time: int):
        params = {
            "StartTime": self._normalize_time(start_time, "start_time"),
            "EndTime": self._normalize_time(end_time, "end_time"),
        }
        return self._call_service("list_user_resource_usage", params)

    def get_total_resource_usage(self, start_time: int, end_time: int):
        params = {
            "StartTime": self._normalize_time(start_time, "start_time"),
            "EndTime": self._normalize_time(end_time, "end_time"),
        }
        return self._call_service("get_total_resource_usage", params)

    def _call_service(self, method_name: str, params: dict):
        try:
            return getattr(Config.service(), method_name)(params)
        except Exception as exc:
            self._raise_friendly_usage_error(exc)

    def _raise_friendly_usage_error(self, exc: Exception):
        error_payload = self._parse_service_error(str(exc))
        if error_payload:
            error_code = error_payload.get("Code")
            error_message = error_payload.get("Message", "")
            if error_code == "ForbiddenErr" and "only account owner can access" in error_message.lower():
                raise PermissionError(
                    "This usage API is only available to the account owner. "
                    "Please use the main account AK/SK instead of a sub-account key."
                ) from exc
        raise exc

    def _parse_service_error(self, raw_message: str):
        payload = raw_message
        if raw_message.startswith(("b'", 'b"')):
            try:
                payload = ast.literal_eval(raw_message).decode("utf-8")
            except Exception:
                payload = raw_message
        try:
            data = json.loads(payload)
        except Exception:
            return None
        return data.get("ResponseMetadata", {}).get("Error")

    def _normalize_time(self, value: int, name: str) -> int:
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be an integer timestamp.") from exc

    def _validate_asset_type(self, type_: str) -> str:
        if type_ not in self.VALID_ASSET_TYPES:
            allowed = ", ".join(sorted(self.VALID_ASSET_TYPES))
            raise ValueError(f"Invalid asset usage type: {type_}. Allowed values: {allowed}.")
        return type_

    def _validate_resource_type(self, type_: str) -> str:
        if type_ not in self.VALID_RESOURCE_TYPES:
            allowed = ", ".join(sorted(self.VALID_RESOURCE_TYPES))
            raise ValueError(f"Invalid resource usage type: {type_}. Allowed values: {allowed}.")
        return type_

    def _normalize_sub_dimensions(self, values: Optional[Iterable[str]]):
        if values is None:
            return None
        if isinstance(values, str):
            values = [values]
        normalized = [str(value).strip() for value in values if str(value).strip()]
        return normalized or None
