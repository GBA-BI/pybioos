import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from bioos.cli.config_store import (
    get_config_path,
    load_account_config,
    load_client_config,
    load_datasite_config,
    load_main_web_config,
    load_repo_config,
)
from bioos.service.main_web import MainWebClient
from bioos.service.datasite import DataSiteClient
from bioos.service.repository import RepositoryClient

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


def resolve_aai_settings(token: Optional[str] = None, cookie: Optional[str] = None, service: str = "repo") -> Dict[str, Optional[str]]:
    normalized_service = _normalize_aai_service(service)
    service_config = load_repo_config() if normalized_service == "repo" else load_datasite_config()
    resolved_token, token_source = _resolve_value(
        explicit_value=token,
        env_names=(f"BIOOS_{normalized_service.upper()}_TOKEN", "BIOOS_AAI_TOKEN", "MIRACLE_AAI_TOKEN"),
        config_values=(service_config.get("token"), service_config.get("access_token"), service_config.get("session")),
    )
    resolved_cookie, cookie_source = _resolve_value(
        explicit_value=cookie,
        env_names=(f"BIOOS_{normalized_service.upper()}_COOKIE", "BIOOS_AAI_COOKIE", "MIRACLE_AAI_COOKIE"),
        config_values=(service_config.get("cookie"), service_config.get("session_cookie")),
    )
    resolved_url, url_source = _resolve_value(
        explicit_value=None,
        env_names=(f"BIOOS_{normalized_service.upper()}_URL",),
        config_values=(service_config.get("url"),),
        default=_default_aai_url(normalized_service),
    )
    return {
        "service": normalized_service,
        "token": resolved_token,
        "token_source": token_source,
        "cookie": resolved_cookie,
        "cookie_source": cookie_source,
        "url": resolved_url,
        "url_source": url_source,
        "passport_issued_at": service_config.get("passport_issued_at"),
        "passport_expires_at": service_config.get("passport_expires_at"),
        "passport_status": _derive_passport_status(
            token=resolved_token,
            expires_at=service_config.get("passport_expires_at"),
        ),
        "configured": bool(resolved_url),
        "config_path": str(get_config_path()),
    }


def resolve_account_link_settings(path: Optional[str] = None) -> Dict[str, Optional[str]]:
    account = load_account_config(path)
    bioos_settings = resolve_auth_settings()
    repo_aai = resolve_aai_settings(service="repo")
    datasite_aai = resolve_aai_settings(service="datasite")
    link_mode = account.get("link_mode")
    return {
        "link_mode": link_mode,
        "status": _derive_account_link_status(
            link_mode=link_mode,
            bioos_access_key=bioos_settings.get("access_key"),
            repo_token=repo_aai.get("token"),
            datasite_token=datasite_aai.get("token"),
        ),
        "bioos_configured": bool(bioos_settings.get("access_key") and bioos_settings.get("secret_key")),
        "aai_repo_configured": bool(repo_aai.get("token") or repo_aai.get("cookie")),
        "aai_datasite_configured": bool(datasite_aai.get("token") or datasite_aai.get("cookie")),
        "config_path": str(get_config_path(path)),
    }


def resolve_main_web_settings(
    login_token: Optional[str] = None,
    csrf_token: Optional[str] = None,
    cookie: Optional[str] = None,
    url: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    web = load_main_web_config()
    resolved_login_token, login_token_source = _resolve_value(
        explicit_value=login_token,
        env_names=("BIOOS_MAIN_LOGIN_TOKEN",),
        config_values=(web.get("login_token"), web.get("x_login_token")),
    )
    resolved_csrf_token, csrf_token_source = _resolve_value(
        explicit_value=csrf_token,
        env_names=("BIOOS_MAIN_CSRF_TOKEN",),
        config_values=(web.get("csrf_token"),),
    )
    resolved_cookie, cookie_source = _resolve_value(
        explicit_value=cookie,
        env_names=("BIOOS_MAIN_COOKIE",),
        config_values=(web.get("cookie"),),
    )
    resolved_url, url_source = _resolve_value(
        explicit_value=url,
        env_names=("BIOOS_MAIN_WEB_URL",),
        config_values=(web.get("url"),),
        default="https://cloud.miracle.ac.cn",
    )
    return {
        "url": resolved_url,
        "url_source": url_source,
        "login_token": resolved_login_token,
        "login_token_source": login_token_source,
        "csrf_token": resolved_csrf_token,
        "csrf_token_source": csrf_token_source,
        "cookie": resolved_cookie,
        "cookie_source": cookie_source,
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


def resolve_aai_with_args(args: Any, service: str = "repo") -> Dict[str, Optional[str]]:
    return resolve_aai_settings(
        token=getattr(args, "token", None),
        cookie=getattr(args, "cookie", None),
        service=service,
    )


def build_repository_client(token: Optional[str] = None, cookie: Optional[str] = None) -> RepositoryClient:
    settings = resolve_aai_settings(token=token, cookie=cookie, service="repo")
    _ensure_aai_token_is_usable(settings, explicit_token=token)
    return RepositoryClient(
        base_url=settings["url"] or _default_aai_url("repo"),
        token=settings["token"],
        cookie=settings["cookie"],
    )


def build_datasite_client(token: Optional[str] = None, cookie: Optional[str] = None) -> DataSiteClient:
    settings = resolve_aai_settings(token=token, cookie=cookie, service="datasite")
    _ensure_aai_token_is_usable(settings, explicit_token=token)
    return DataSiteClient(
        base_url=settings["url"] or _default_aai_url("datasite"),
        token=settings["token"],
        cookie=settings["cookie"],
    )


def build_main_web_client(
    login_token: Optional[str] = None,
    csrf_token: Optional[str] = None,
    cookie: Optional[str] = None,
    url: Optional[str] = None,
) -> MainWebClient:
    settings = resolve_main_web_settings(
        login_token=login_token,
        csrf_token=csrf_token,
        cookie=cookie,
        url=url,
    )
    return MainWebClient(
        base_url=settings["url"] or "https://cloud.miracle.ac.cn",
        login_token=settings["login_token"],
        csrf_token=settings["csrf_token"],
        cookie=settings["cookie"],
    )


def login_to_main_web(
    *,
    account_name: str,
    password: str,
    user_name: Optional[str] = None,
    url: Optional[str] = None,
    timeout: int = 30,
) -> Dict[str, Optional[str]]:
    settings = resolve_main_web_settings(url=url)
    return MainWebClient.login_with_password(
        base_url=settings["url"] or "https://cloud.miracle.ac.cn",
        account_name=account_name,
        password=password,
        user_name=user_name,
        timeout=timeout,
    )


def _ensure_aai_token_is_usable(settings: Dict[str, Optional[str]], explicit_token: Optional[str] = None) -> None:
    if explicit_token:
        return
    token = settings.get("token")
    if not token:
        return
    passport_status = settings.get("passport_status")
    if passport_status != "expired":
        return
    service = settings.get("service") or "repo"
    expires_at = settings.get("passport_expires_at") or "unknown time"
    raise ValueError(
        f"{service} passport in {settings['config_path']} expired at {expires_at}. "
        f"Run 'bioos aai refresh' or 'bioos aai login' to refresh it."
    )


def _derive_passport_status(token: Optional[str], expires_at: Optional[str]) -> str:
    if not token:
        return "missing"
    if not expires_at:
        return "unknown"
    try:
        expires_dt = _parse_iso8601(expires_at)
    except ValueError:
        return "unknown"
    now = datetime.now(timezone.utc)
    return "expired" if expires_dt <= now else "valid"


def _parse_iso8601(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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


def _default_aai_url(service: str) -> str:
    normalized_service = _normalize_aai_service(service)
    if normalized_service == "repo":
        return "https://network.miracle.ac.cn"
    return "https://mclibrary.miracle.ac.cn"


def _normalize_aai_service(service: str) -> str:
    normalized = (service or "repo").strip().lower()
    if normalized not in {"repo", "datasite"}:
        raise ValueError(f"Unsupported AAI service: {service}. Expected one of: repo, datasite.")
    return normalized


def _derive_account_link_status(
    *,
    link_mode: Optional[str],
    bioos_access_key: Optional[str],
    repo_token: Optional[str],
    datasite_token: Optional[str],
) -> str:
    if not link_mode:
        return "missing"
    if bioos_access_key and (repo_token or datasite_token):
        return "linked-ready"
    if bioos_access_key or repo_token or datasite_token:
        return "partially-configured"
    return "configured"
