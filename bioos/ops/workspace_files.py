import os
from typing import Iterable, List, Optional, Union

from bioos.ops.auth import login_to_bioos, resolve_workspace

DEFAULT_UPLOAD_CHECKPOINT_DIR = os.path.join(
    os.path.expanduser("~"), ".bioos", "upload-checkpoints")


def _normalize_local_sources(sources: Union[str, Iterable[str]]) -> List[str]:
    if isinstance(sources, str):
        sources = [sources]

    normalized_sources = []
    for source in sources:
        if source is None:
            continue
        normalized_source = os.path.normpath(os.path.expanduser(str(source)))
        if not normalized_source:
            continue
        normalized_sources.append(normalized_source)

    if not normalized_sources:
        raise ValueError("At least one local source file is required.")
    return normalized_sources


def _resolve_workspace_upload_key(local_file_path: str,
                                  target: str,
                                  flatten: bool) -> str:
    if flatten:
        to_upload_path = os.path.basename(local_file_path)
    else:
        to_upload_path = os.path.normpath(local_file_path)

    if os.path.isabs(to_upload_path):
        to_upload_path = to_upload_path.lstrip("/")

    return os.path.normpath(os.path.join(target, to_upload_path))


def _upload_local_files_with_workspace(
    ws,
    workspace_id: str,
    workspace_name: str,
    sources: Union[str, Iterable[str]],
    target: str = "",
    flatten: bool = True,
    skip_existing: bool = False,
    checkpoint_dir: Optional[str] = None,
    max_retries: int = 3,
    task_num: int = 10,
):
    normalized_sources = _normalize_local_sources(sources)
    max_retries = max(int(max_retries) if max_retries is not None else 3, 0)
    task_num = max(int(task_num) if task_num is not None else 10, 1)
    missing_files = [source for source in normalized_sources if not os.path.exists(source)]
    if missing_files:
        raise FileNotFoundError(f"Local file not found: {missing_files[0]}")

    directory_sources = [source for source in normalized_sources if os.path.isdir(source)]
    if directory_sources:
        raise IsADirectoryError(f"Directory upload is not supported yet: {directory_sources[0]}")

    resolved_checkpoint_dir = os.path.abspath(
        os.path.expanduser(checkpoint_dir or DEFAULT_UPLOAD_CHECKPOINT_DIR))

    upload_plan = []
    for source in normalized_sources:
        key = _resolve_workspace_upload_key(source, target, flatten)
        upload_plan.append({
            "source": source,
            "key": key,
            "s3_url": ws.files.s3_urls([key])[0],
        })

    pending_uploads = []
    skipped_uploads = []
    for item in upload_plan:
        if skip_existing and ws.files.tos_handler.object_exists(item["key"]):
            skipped_uploads.append(item)
            continue
        pending_uploads.append(item)

    failed_sources = []
    if pending_uploads:
        failed_sources = ws.files.tos_handler.upload_objects(
            files_to_upload=[item["source"] for item in pending_uploads],
            target_path=target,
            flatten=flatten,
            checkpoint_dir=resolved_checkpoint_dir,
            max_retries=max_retries,
            task_num=task_num,
        )

    failed_source_set = set(failed_sources)
    failed_uploads = [item for item in pending_uploads if item["source"] in failed_source_set]
    uploaded_files = [item for item in pending_uploads if item["source"] not in failed_source_set]

    if failed_uploads:
        failed_list = ", ".join(item["source"] for item in failed_uploads)
        raise RuntimeError(f"Failed to upload {len(failed_uploads)} file(s): {failed_list}")

    return {
        "success": True,
        "workspace_name": workspace_name,
        "workspace_id": workspace_id,
        "target": target,
        "flatten": flatten,
        "skip_existing": skip_existing,
        "checkpoint_dir": resolved_checkpoint_dir,
        "max_retries": max_retries,
        "task_num": task_num,
        "uploaded_count": len(uploaded_files),
        "skipped_count": len(skipped_uploads),
        "uploaded_files": uploaded_files,
        "skipped_files": skipped_uploads,
    }


def upload_local_files_to_workspace(
    workspace_name: str,
    sources: Union[str, Iterable[str]],
    target: str = "",
    flatten: bool = True,
    skip_existing: bool = False,
    checkpoint_dir: Optional[str] = None,
    max_retries: int = 3,
    task_num: int = 10,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    endpoint: Optional[str] = None,
):
    from bioos import bioos

    login_to_bioos(access_key=access_key, secret_key=secret_key, endpoint=endpoint)
    workspace_id, _ = resolve_workspace(workspace_name)
    ws = bioos.workspace(workspace_id)
    return _upload_local_files_with_workspace(
        ws=ws,
        workspace_id=workspace_id,
        workspace_name=workspace_name,
        sources=sources,
        target=target,
        flatten=flatten,
        skip_existing=skip_existing,
        checkpoint_dir=checkpoint_dir,
        max_retries=max_retries,
        task_num=task_num,
    )


def upload_dashboard_file_to_workspace(
    workspace_name: str,
    local_file_path: str,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    endpoint: Optional[str] = None,
):
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Local file not found: {local_file_path}")
    filename = os.path.basename(local_file_path)
    if filename != "__dashboard__.md":
        raise ValueError(f"Dashboard file must be named __dashboard__.md, got: {filename}")

    result = upload_local_files_to_workspace(
        workspace_name=workspace_name,
        sources=[local_file_path],
        target="",
        flatten=True,
        access_key=access_key,
        secret_key=secret_key,
        endpoint=endpoint,
    )
    uploaded_file = result["uploaded_files"][0]
    return {
        "success": True,
        "workspace_name": workspace_name,
        "workspace_id": result["workspace_id"],
        "local_file_path": local_file_path,
        "s3_url": uploaded_file["s3_url"],
        "expected_s3_url": f"s3://bioos-{result['workspace_id']}/__dashboard__.md",
    }
