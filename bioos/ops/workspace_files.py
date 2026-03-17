import os
from typing import Optional

from bioos.ops.auth import login_to_bioos, resolve_workspace


def upload_dashboard_file_to_workspace(
    workspace_name: str,
    local_file_path: str,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    endpoint: Optional[str] = None,
):
    from bioos import bioos

    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Local file not found: {local_file_path}")
    filename = os.path.basename(local_file_path)
    if filename != "__dashboard__.md":
        raise ValueError(f"Dashboard file must be named __dashboard__.md, got: {filename}")

    login_to_bioos(access_key=access_key, secret_key=secret_key, endpoint=endpoint)
    workspace_id, _ = resolve_workspace(workspace_name)
    ws = bioos.workspace(workspace_id)
    success = ws.files.upload(sources=[local_file_path], target="", flatten=True)
    if not success:
        raise RuntimeError("Dashboard upload failed.")
    s3_url = ws.files.s3_urls(["__dashboard__.md"])[0]
    return {
        "success": True,
        "workspace_name": workspace_name,
        "workspace_id": workspace_id,
        "local_file_path": local_file_path,
        "s3_url": s3_url,
        "expected_s3_url": f"s3://bioos-{workspace_id}/__dashboard__.md",
    }
