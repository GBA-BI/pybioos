from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def dataframe_records(df: Any) -> List[Dict[str, Any]]:
    if df is None or getattr(df, "empty", True):
        return []
    return [
        {key: _clean_missing_values(value) for key, value in record.items()}
        for record in df.to_dict(orient="records")
    ]


def _clean_missing_values(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _clean_missing_values(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_clean_missing_values(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_clean_missing_values(item) for item in value)
    if value is None:
        return None
    try:
        import pandas as pd
        return None if bool(pd.isna(value)) else value
    except (ImportError, TypeError, ValueError):
        return value


def to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, (int, float)):
        if value != value or value <= 0:
            return None
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(value)
