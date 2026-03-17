import os
from typing import Any, Dict, Optional, Tuple

DEFAULT_ENDPOINT = "https://bio-top.miracle.ac.cn"


def resolve_credentials(access_key: Optional[str] = None, secret_key: Optional[str] = None) -> Tuple[str, str]:
    ak = access_key or os.getenv("MIRACLE_ACCESS_KEY") or os.getenv("VOLC_ACCESSKEY")
    sk = secret_key or os.getenv("MIRACLE_SECRET_KEY") or os.getenv("VOLC_SECRETKEY")
    if not ak:
        raise ValueError("Missing access key. Provide --ak or set MIRACLE_ACCESS_KEY / VOLC_ACCESSKEY.")
    if not sk:
        raise ValueError("Missing secret key. Provide --sk or set MIRACLE_SECRET_KEY / VOLC_SECRETKEY.")
    return ak, sk


def resolve_endpoint(endpoint: Optional[str] = None) -> str:
    return endpoint or os.getenv("BIOOS_ENDPOINT") or DEFAULT_ENDPOINT


def login_to_bioos(
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    endpoint: Optional[str] = None,
) -> Tuple[str, str, str]:
    from bioos import bioos

    ak, sk = resolve_credentials(access_key, secret_key)
    resolved_endpoint = resolve_endpoint(endpoint)
    bioos.login(access_key=ak, secret_key=sk, endpoint=resolved_endpoint)
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
