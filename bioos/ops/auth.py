import os
from typing import Any, Dict, Optional, Tuple

from bioos.cli.config_store import get_config_path, load_client_config

DEFAULT_ENDPOINT = "https://bio-top.miracle.ac.cn"


def resolve_auth_settings(
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    endpoint: Optional[str] = None,
    region: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    client = load_client_config()
    resolved_access_key, access_key_source = _resolve_value(
        explicit_value=access_key,
        env_names=("MIRACLE_ACCESS_KEY",),
        config_values=(client.get("MIRACLE_ACCESS_KEY"),),
    )
    resolved_secret_key, secret_key_source = _resolve_value(
        explicit_value=secret_key,
        env_names=("MIRACLE_SECRET_KEY",),
        config_values=(client.get("MIRACLE_SECRET_KEY"),),
    )
    resolved_endpoint, endpoint_source = _resolve_value(
        explicit_value=endpoint,
        env_names=("BIOOS_ENDPOINT",),
        config_values=(client.get("serveraddr"), client.get("endpoint")),
        default=DEFAULT_ENDPOINT,
    )
    resolved_region, region_source = _resolve_value(
        explicit_value=region,
        env_names=("BIOOS_REGION",),
        config_values=(client.get("region"),),
    )
    return {
        "access_key": resolved_access_key,
        "secret_key": resolved_secret_key,
        "endpoint": resolved_endpoint,
        "region": resolved_region,
        "access_key_source": access_key_source,
        "secret_key_source": secret_key_source,
        "endpoint_source": endpoint_source,
        "region_source": region_source,
        "config_path": str(get_config_path()),
    }


def resolve_credentials(access_key: Optional[str] = None, secret_key: Optional[str] = None) -> Tuple[str, str]:
    settings = resolve_auth_settings(access_key=access_key, secret_key=secret_key)
    ak = settings["access_key"]
    sk = settings["secret_key"]
    if not ak:
        raise ValueError(
            f"Missing access key. Provide --ak, set MIRACLE_ACCESS_KEY, or configure {settings['config_path']}."
        )
    if not sk:
        raise ValueError(
            f"Missing secret key. Provide --sk, set MIRACLE_SECRET_KEY, or configure {settings['config_path']}."
        )
    return ak, sk


def resolve_endpoint(endpoint: Optional[str] = None) -> str:
    settings = resolve_auth_settings(endpoint=endpoint)
    return settings["endpoint"] or DEFAULT_ENDPOINT


def login_to_bioos(
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    endpoint: Optional[str] = None,
) -> Tuple[str, str, str]:
    from bioos import bioos

    settings = resolve_auth_settings(access_key=access_key, secret_key=secret_key, endpoint=endpoint)
    ak, sk = resolve_credentials(access_key, secret_key)
    resolved_endpoint = settings["endpoint"] or DEFAULT_ENDPOINT
    login_kwargs = {"access_key": ak, "secret_key": sk, "endpoint": resolved_endpoint}
    if settings["region"]:
        login_kwargs["region"] = settings["region"]
    bioos.login(**login_kwargs)
    return ak, sk, resolved_endpoint


def login_with_args(args: Any) -> Tuple[str, str, str]:
    return login_to_bioos(
        access_key=getattr(args, "ak", None),
        secret_key=getattr(args, "sk", None),
        endpoint=getattr(args, "endpoint", None),
    )


def resolve_workspace(workspace_name: str) -> Tuple[str, Dict[str, Any]]:
    from bioos import bioos

    workspaces = bioos.list_workspaces()
    matched = workspaces[workspaces["Name"] == workspace_name]
    if getattr(matched, "empty", True):
        raise ValueError(f"Workspace not found: {workspace_name}")
    row = matched.iloc[0].to_dict()
    return str(row["ID"]), row


def workspace_context_from_args(args: Any):
    from bioos import bioos

    login_with_args(args)
    workspace_id, _ = resolve_workspace(args.workspace_name)
    return workspace_id, bioos.workspace(workspace_id)


def _resolve_value(
    explicit_value: Optional[str],
    env_names: Tuple[str, ...],
    config_values: Tuple[Optional[str], ...],
    default: Optional[str] = None,
) -> Tuple[Optional[str], str]:
    if explicit_value:
        return explicit_value, "cli"

    for env_name in env_names:
        value = os.getenv(env_name)
        if value:
            return value, "env"

    for value in config_values:
        if value:
            return value, "config"

    if default is not None:
        return default, "default"

    return None, "missing"
