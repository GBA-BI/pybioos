import os
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from bioos.errors import ConfigurationError


DEFAULT_REPOSITORY_ENDPOINT = "https://network.miracle.ac.cn"


def resolve_repository_endpoint(endpoint: Optional[str] = None) -> str:
    return _resolve_endpoint(
        explicit=endpoint,
        env_name="BIOOS_REPOSITORY_ENDPOINT",
        config_names=("repository_endpoint", "repositoryaddr"),
        default_endpoint=DEFAULT_REPOSITORY_ENDPOINT,
    )


def normalize_endpoint(endpoint: str, default_scheme: str = "https") -> str:
    if not endpoint:
        raise ConfigurationError("endpoint")
    value = str(endpoint).strip()
    parsed = urlparse(value)
    if not parsed.scheme:
        value = f"{default_scheme}://{value}"
    return value.rstrip("/")


def endpoint_from_drs_uri(uri: str, default_scheme: str = "http") -> Optional[str]:
    if not uri or not str(uri).startswith("drs://"):
        return None
    parsed = urlparse(str(uri).strip())
    if not parsed.netloc:
        return None
    return normalize_endpoint(parsed.netloc, default_scheme=default_scheme)


def _resolve_endpoint(
    explicit: Optional[str],
    env_name: str,
    config_names,
    default_endpoint: str,
) -> str:
    value = explicit or os.getenv(env_name)
    if not value:
        client_config = _load_client_config()
        for name in config_names:
            if client_config.get(name):
                value = client_config[name]
                break
    if not value:
        value = default_endpoint
    if not value:
        raise ConfigurationError(env_name)
    return normalize_endpoint(value)


def _load_client_config() -> Dict[str, Any]:
    try:
        from bioos.cli.config_store import load_client_config

        return load_client_config()
    except Exception:
        return {}
