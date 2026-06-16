from typing import Any, Dict

import pandas as pd
from pandas import DataFrame


def build_params(aliases: Dict[str, str], filters: Dict[str, Any], **values) -> Dict[str, Any]:
    params = {}
    for key, value in values.items():
        if value is not None:
            params[aliases.get(key, key)] = value
    for key, value in filters.items():
        if value is not None:
            params[aliases.get(key, key)] = value
    return params


def payload_to_dataframe(payload: Any) -> DataFrame:
    records = extract_records(payload)
    if not records:
        return pd.DataFrame()
    return pd.DataFrame.from_records(records)


def extract_records(payload: Any):
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
            nested = extract_records(value)
            if nested:
                return nested
    result = payload.get("Result")
    if isinstance(result, (dict, list)):
        return extract_records(result)
    return []


def extract_ids(payload: Any):
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
            nested = extract_ids(value)
            if nested:
                return nested
    result = payload.get("Result")
    if isinstance(result, (dict, list)):
        return extract_ids(result)
    return []
