from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from bioos.ops.auth import login_to_bioos, resolve_workspace
from bioos.ops.formatters import dataframe_records, to_iso

DEFAULT_ENDPOINT = "https://bio-top.miracle.ac.cn"


def service():
    from bioos.config import Config

    return Config.service()


@dataclass
class WorkspaceProfileOptions:
    workspace_name: str
    submission_limit: int = 5
    artifact_limit_per_submission: int = 10
    sample_rows_per_data_model: int = 3
    include_artifacts: bool = True
    include_failure_details: bool = True
    include_ies: bool = True
    include_signed_urls: bool = False
    endpoint: str = DEFAULT_ENDPOINT
    access_key: Optional[str] = None
    secret_key: Optional[str] = None


def get_workspace_profile_data(options: WorkspaceProfileOptions) -> Dict[str, Any]:
    from bioos import bioos

    login_to_bioos(
        access_key=options.access_key,
        secret_key=options.secret_key,
        endpoint=options.endpoint,
    )
    workspace_id, workspace_row = resolve_workspace(options.workspace_name)
    ws = bioos.workspace(workspace_id)
    ies_records, ies_warning, ies_coverage = collect_ies_records(ws, options)

    workspace_section = build_workspace_section(workspace_id, workspace_row, ws, options, ies_records)
    workflows = build_workflows_section(workspace_id)
    workflow_lookup = {item["id"]: item["name"] for item in workflows}
    data_models = build_data_models_section(workspace_id, options)
    submissions = build_submissions_section(workspace_id, workflow_lookup, options)
    submission_metrics, metric_warnings = collect_submission_metrics(workspace_id, submissions)
    failure_summaries = build_failure_summaries(workspace_id, submissions, options)
    artifact_summaries, artifact_warnings, artifact_coverage = collect_artifact_summaries(
        ws,
        workspace_section.get("s3_bucket"),
        submissions,
        options,
    )
    ies_apps = build_ies_section(ies_records)

    warnings = []
    if not options.include_signed_urls:
        warnings.append("Signed artifact URLs are omitted by default.")
    if ies_warning:
        warnings.append(ies_warning)
    warnings.extend(metric_warnings)
    warnings.extend(artifact_warnings)

    return {
        "success": True,
        "workspace": workspace_section,
        "summary": build_summary(workflows, data_models, submissions, submission_metrics),
        "coverage": {
            "workspace": "partial",
            "workflows": "full",
            "data_models": "full",
            "submissions": "full",
            "failure_details": "full" if options.include_failure_details else "not_requested",
            "artifacts": artifact_coverage,
            "ies": ies_coverage,
        },
        "workflows": workflows,
        "data_models": data_models,
        "recent_submissions": submissions,
        "lineage": build_lineage(submissions),
        "failure_summaries": failure_summaries,
        "artifact_summaries": artifact_summaries,
        "ies_apps": ies_apps,
        "warnings": warnings,
    }


def safe_json_loads(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not value:
        return {}
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def normalize_params(items: Any) -> List[Dict[str, Any]]:
    result = []
    for item in items or []:
        result.append(
            {
                "name": item.get("Name"),
                "type": item.get("Type"),
                "required": not bool(item.get("Optional")),
                "default_value": item.get("Default"),
            }
        )
    return result


def summarize_cluster_bindings(env_info: Any) -> List[Dict[str, Any]]:
    records = dataframe_records(env_info)
    return [
        {
            "cluster_id": item.get("cluster_id"),
            "name": item.get("name"),
            "description": item.get("description"),
            "type": item.get("type"),
        }
        for item in records
    ]


def summarize_ies_error(exc: Exception) -> str:
    message = str(exc)
    if "none attached webapp type cluster" in message.lower():
        return "IES is unavailable for this workspace because no webapp type cluster is attached."
    return f"Failed to query IES apps: {message}"


def collect_ies_records(ws: Any, options: WorkspaceProfileOptions) -> Tuple[List[Dict[str, Any]], Optional[str], str]:
    if not options.include_ies:
        return [], None, "not_requested"
    try:
        return dataframe_records(ws.webinstanceapps.list()), None, "full"
    except Exception as exc:
        return [], summarize_ies_error(exc), "partial"


def build_workspace_section(
    workspace_id: str,
    workspace_row: Dict[str, Any],
    ws: Any,
    options: WorkspaceProfileOptions,
    ies_records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    basic_info = ws.basic_info or {}
    return {
        "id": workspace_id,
        "name": workspace_row.get("Name") or basic_info.get("name"),
        "description": workspace_row.get("Description") or basic_info.get("description"),
        "owner_name": workspace_row.get("OwnerName") or basic_info.get("owner"),
        "endpoint": options.endpoint,
        "s3_bucket": workspace_row.get("S3Bucket") or basic_info.get("s3_bucket"),
        "created_at": to_iso(workspace_row.get("CreateTime") or basic_info.get("create_time")),
        "updated_at": to_iso(workspace_row.get("UpdateTime")),
        "cluster_bindings": summarize_cluster_bindings(ws.env_info),
        "has_ies": bool(ies_records) if options.include_ies else None,
    }


def build_workflows_section(workspace_id: str) -> List[Dict[str, Any]]:
    from bioos.service.api import list_workflows

    items = list_workflows(workspace_id=workspace_id, page_number=1, page_size=100) or []
    workflows = []
    for item in items:
        workflows.append(
            {
                "id": item.get("ID"),
                "name": item.get("Name"),
                "description": item.get("Description"),
                "status": (item.get("Status") or {}).get("Phase", item.get("Status")),
                "language": item.get("Language"),
                "source_type": item.get("SourceType"),
                "tag": item.get("Tag"),
                "main_workflow_path": item.get("MainWorkflowPath"),
                "owner_name": item.get("OwnerName"),
                "created_at": to_iso(item.get("CreateTime")),
                "updated_at": to_iso(item.get("UpdateTime")),
                "inputs": normalize_params(item.get("Inputs")),
                "outputs": normalize_params(item.get("Outputs")),
            }
        )
    workflows.sort(key=lambda item: item.get("created_at") or "", reverse=True)
    return workflows


def preview_data_model_rows(workspace_id: str, model_id: str, sample_rows: int) -> Dict[str, Any]:
    content = service().list_data_model_rows(
        {
            "WorkspaceID": workspace_id,
            "ID": model_id,
            "PageNumber": 1,
            "PageSize": sample_rows,
        }
    )
    headers = content.get("Headers", [])
    preview_rows = []
    for row in content.get("Rows", []):
        if isinstance(row, list):
            preview_rows.append(dict(zip(headers, row)))
        elif isinstance(row, dict):
            preview_rows.append(row)
    return {
        "columns": headers,
        "sample_rows": preview_rows,
        "row_count": content.get("TotalCount"),
    }


def list_data_model_records(workspace_id: str) -> List[Dict[str, Any]]:
    payload = service().list_data_models({"WorkspaceID": workspace_id}) or {}
    items = payload.get("Items") or []
    if not isinstance(items, list):
        return []
    normal_items = [item for item in items if isinstance(item, dict) and item.get("Type") == "normal"]
    return normal_items or [item for item in items if isinstance(item, dict)]


def build_data_models_section(workspace_id: str, options: WorkspaceProfileOptions) -> List[Dict[str, Any]]:
    models = []
    for item in list_data_model_records(workspace_id):
        model_id = item.get("ID")
        preview = {"columns": [], "sample_rows": [], "row_count": item.get("RowCount")}
        if model_id and options.sample_rows_per_data_model > 0:
            try:
                preview = preview_data_model_rows(
                    workspace_id,
                    model_id,
                    options.sample_rows_per_data_model,
                )
            except Exception:
                pass
        models.append(
            {
                "id": model_id,
                "name": item.get("Name"),
                "type": item.get("Type"),
                "row_count": preview.get("row_count") or item.get("RowCount"),
                "columns": preview.get("columns", []),
                "sample_rows": preview.get("sample_rows", []),
                "created_at": to_iso(item.get("CreateTime")),
                "updated_at": to_iso(item.get("UpdateTime")),
            }
        )
    return models


def build_submissions_section(
    workspace_id: str,
    workflow_lookup: Dict[str, str],
    options: WorkspaceProfileOptions,
) -> List[Dict[str, Any]]:
    from bioos.service.api import list_submissions

    items = list_submissions(
        workspace_id=workspace_id,
        page_number=1,
        page_size=max(50, options.submission_limit),
    ) or []
    items.sort(key=lambda item: item.get("StartTime") or 0, reverse=True)

    submissions = []
    for item in items[: options.submission_limit]:
        data_entity = item.get("DataEntity") or {}
        submissions.append(
            {
                "id": item.get("ID"),
                "name": item.get("Name"),
                "description": item.get("Description"),
                "status": item.get("Status"),
                "workflow_id": item.get("WorkflowID"),
                "workflow_name": workflow_lookup.get(item.get("WorkflowID")),
                "owner_name": item.get("OwnerName"),
                "start_time": to_iso(item.get("StartTime")),
                "finish_time": to_iso(item.get("FinishTime")),
                "duration_seconds": item.get("Duration"),
                "data_model_id": item.get("DataModelID"),
                "data_model_name": data_entity.get("Name"),
                "row_ids": data_entity.get("RowIDs") or [],
                "cluster_id": item.get("ClusterID"),
                "cluster_type": item.get("ClusterType"),
                "input_binding": safe_json_loads(item.get("Inputs")) if isinstance(item.get("Inputs"), str) else {},
                "output_binding": safe_json_loads(item.get("Outputs")) if isinstance(item.get("Outputs"), str) else {},
                "options": item.get("ExposedOptions") or {},
                "final_execution_dir": item.get("FinalExecutionDir"),
                "run_status": item.get("RunStatus") or {},
            }
        )
    return submissions


def fetch_all_submission_records(workspace_id: str, page_size: int = 100) -> Tuple[List[Dict[str, Any]], int]:
    all_items: List[Dict[str, Any]] = []
    page_number = 1
    total_count: Optional[int] = None
    while True:
        payload = service().list_submissions(
            {"WorkspaceID": workspace_id, "PageNumber": page_number, "PageSize": page_size, "Filter": {}}
        ) or {}
        if total_count is None and payload.get("TotalCount") is not None:
            total_count = int(payload["TotalCount"])
        items = payload.get("Items") or []
        if not isinstance(items, list) or not items:
            break
        all_items.extend(item for item in items if isinstance(item, dict))
        if total_count is not None and len(all_items) >= total_count:
            break
        if total_count is None and len(items) < page_size:
            break
        page_number += 1
    return all_items, total_count if total_count is not None else len(all_items)


def collect_submission_metrics(
    workspace_id: str,
    submissions: List[Dict[str, Any]],
) -> Tuple[Dict[str, int], List[str]]:
    recent_count = len(submissions)
    fallback = {
        "submission_count": recent_count,
        "recent_submission_count": recent_count,
        "succeeded_submission_count": sum(1 for item in submissions if item.get("status") == "Succeeded"),
        "failed_submission_count": sum(1 for item in submissions if item.get("status") == "Failed"),
        "running_submission_count": sum(
            1 for item in submissions if item.get("status") in {"Running", "Pending"}
        ),
    }
    try:
        all_items, total_count = fetch_all_submission_records(workspace_id)
        return {
            "submission_count": total_count,
            "recent_submission_count": recent_count,
            "succeeded_submission_count": sum(1 for item in all_items if item.get("Status") == "Succeeded"),
            "failed_submission_count": sum(1 for item in all_items if item.get("Status") == "Failed"),
            "running_submission_count": sum(
                1 for item in all_items if item.get("Status") in {"Running", "Pending"}
            ),
        }, []
    except Exception as exc:
        return fallback, [f"Failed to query total submission metrics; falling back to recent submissions: {exc}"]


def summarize_failure_message(raw_message: str) -> str:
    if not raw_message:
        return "Submission failed, but no detailed run error was returned."
    job_match = re.search(r"Job ([^ ]+) exited with return code (\d+)", raw_message)
    if job_match:
        return f"Job {job_match.group(1)} exited with return code {job_match.group(2)}."
    workflow_match = re.search(r'message":"([^"]+)"', raw_message)
    if workflow_match:
        return workflow_match.group(1)
    return raw_message.strip()


def infer_failed_task(raw_message: str) -> Optional[str]:
    match = re.search(r"Job ([^ ]+) exited", raw_message or "")
    return match.group(1) if match else None


def build_failure_summaries(
    workspace_id: str,
    submissions: List[Dict[str, Any]],
    options: WorkspaceProfileOptions,
) -> List[Dict[str, Any]]:
    from bioos.resource.workflows import Submission

    if not options.include_failure_details:
        return []
    summaries = []
    for submission in submissions:
        if submission.get("status") != "Failed":
            continue
        try:
            submission_obj = Submission(workspace_id, submission["id"])
            failed_runs = [run for run in submission_obj.runs if run.status == "Failed"]
            if not failed_runs:
                continue
            run = failed_runs[0]
            raw_message = run.error if isinstance(run.error, str) else ""
            summaries.append(
                {
                    "submission_id": submission["id"],
                    "workflow_name": submission.get("workflow_name"),
                    "run_id": run.id,
                    "failed_task": infer_failed_task(raw_message),
                    "error_summary": summarize_failure_message(raw_message),
                    "raw_message": raw_message,
                }
            )
        except Exception as exc:
            summaries.append(
                {
                    "submission_id": submission["id"],
                    "workflow_name": submission.get("workflow_name"),
                    "run_id": None,
                    "failed_task": None,
                    "error_summary": f"Failed to retrieve run-level error: {exc}",
                    "raw_message": "",
                }
            )
    return summaries


def strip_execution_prefix(execution_dir: Optional[str], bucket: Optional[str]) -> Optional[str]:
    if not execution_dir:
        return None
    if execution_dir.startswith("s3://"):
        parts = execution_dir[5:].split("/", 1)
        if len(parts) == 2:
            bucket_name, object_key = parts
            if bucket and bucket_name == bucket:
                return object_key
            return object_key
    return execution_dir.lstrip("/")


def categorize_file(key: str) -> str:
    name = Path(key).name
    if name == "stdout":
        return "stdout"
    if name == "stderr":
        return "stderr"
    if name == "script":
        return "script"
    if name == "log" or (name.startswith("workflow.") and name.endswith(".log")):
        return "log"
    if name == "rc" or name.endswith(".list") or name == "cromwell_glob_control_file":
        return "control"
    return "result"


def summarize_artifacts(
    ws: Any,
    bucket: Optional[str],
    submission: Dict[str, Any],
    options: WorkspaceProfileOptions,
) -> Optional[Dict[str, Any]]:
    prefix = strip_execution_prefix(submission.get("final_execution_dir"), bucket)
    if not prefix:
        return None
    records = dataframe_records(ws.files.list(prefix=prefix, recursive=True))
    if not records:
        return None
    total_size = 0
    sample_files = []
    for record in records:
        size = record.get("size") or 0
        if size != size:
            size = 0
        size = int(size)
        total_size += size
        entry = {
            "key": record.get("key"),
            "size_bytes": size,
            "category": categorize_file(record.get("key", "")),
        }
        if options.include_signed_urls:
            entry["s3_url"] = record.get("s3_url")
            entry["https_url"] = record.get("https_url")
        sample_files.append(entry)
    order = {"result": 0, "log": 1, "stderr": 2, "stdout": 3, "script": 4, "control": 5}
    sample_files.sort(key=lambda item: (order.get(item["category"], 99), -item["size_bytes"]))
    categories = {item["category"] for item in sample_files}
    return {
        "submission_id": submission["id"],
        "workflow_name": submission.get("workflow_name"),
        "execution_dir": prefix,
        "file_count": len(records),
        "total_size_bytes": total_size,
        "has_stdout": "stdout" in categories,
        "has_stderr": "stderr" in categories,
        "has_workflow_log": "log" in categories,
        "has_result_files": "result" in categories,
        "sample_files": sample_files[: options.artifact_limit_per_submission],
    }


def collect_artifact_summaries(
    ws: Any,
    bucket: Optional[str],
    submissions: List[Dict[str, Any]],
    options: WorkspaceProfileOptions,
) -> Tuple[List[Dict[str, Any]], List[str], str]:
    if not options.include_artifacts:
        return [], [], "not_requested"
    summaries: List[Dict[str, Any]] = []
    warnings: List[str] = []
    had_error = False
    for submission in submissions:
        try:
            summary = summarize_artifacts(ws, bucket, submission, options)
            if summary:
                summaries.append(summary)
        except Exception as exc:
            had_error = True
            warnings.append(f"Failed to summarize artifacts for submission {submission.get('id')}: {exc}")
    return summaries, warnings, "partial" if had_error else "full"


def build_ies_section(ies_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ies_apps = []
    for item in ies_records:
        status = item.get("Status")
        state = status.get("State") if isinstance(status, dict) else str(status)
        ies_apps.append(
            {
                "id": item.get("ID"),
                "name": item.get("Name"),
                "description": item.get("Description"),
                "state": state,
                "owner_name": item.get("OwnerName"),
                "resource_size": item.get("ResourceSize"),
                "storage_capacity": item.get("StorageCapacity"),
                "created_at": to_iso(item.get("CreateTime")),
                "updated_at": to_iso(item.get("UpdateTime")),
            }
        )
    return ies_apps


def build_lineage(submissions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "data_model_id": submission.get("data_model_id"),
            "data_model_name": submission.get("data_model_name"),
            "workflow_id": submission.get("workflow_id"),
            "workflow_name": submission.get("workflow_name"),
            "submission_id": submission.get("id"),
            "submission_status": submission.get("status"),
            "row_ids": submission.get("row_ids", []),
            "final_execution_dir": submission.get("final_execution_dir"),
        }
        for submission in submissions
    ]


def build_summary(
    workflows: List[Dict[str, Any]],
    data_models: List[Dict[str, Any]],
    submissions: List[Dict[str, Any]],
    submission_metrics: Dict[str, int],
) -> Dict[str, Any]:
    total_submissions = submission_metrics.get("submission_count", len(submissions))
    recent_submissions = submission_metrics.get("recent_submission_count", len(submissions))
    succeeded = submission_metrics.get(
        "succeeded_submission_count",
        sum(1 for item in submissions if item.get("status") == "Succeeded"),
    )
    failed = submission_metrics.get(
        "failed_submission_count",
        sum(1 for item in submissions if item.get("status") == "Failed"),
    )
    running = submission_metrics.get(
        "running_submission_count",
        sum(1 for item in submissions if item.get("status") in {"Running", "Pending"}),
    )
    latest_submission = submissions[0] if submissions else None
    if failed > 0 and succeeded == 0:
        health_status = "error"
    elif failed > 0 or running > 0:
        health_status = "warning"
    else:
        health_status = "healthy"
    summary = (
        f"Workspace has {len(workflows)} workflows, {len(data_models)} data models, "
        f"and {total_submissions} total submissions."
    )
    if recent_submissions != total_submissions:
        summary += f" This profile includes details for the {recent_submissions} most recent submissions."
    if failed > 0:
        summary += f" {failed} submission(s) failed."
    elif running > 0:
        summary += f" {running} submission(s) still running or pending."
    else:
        summary += " No recent failures were found."
    return {
        "workflow_count": len(workflows),
        "data_model_count": len(data_models),
        "submission_count": total_submissions,
        "recent_submission_count": recent_submissions,
        "succeeded_submission_count": succeeded,
        "failed_submission_count": failed,
        "running_submission_count": running,
        "latest_submission_id": latest_submission.get("id") if latest_submission else None,
        "latest_activity_at": latest_submission.get("finish_time") if latest_submission else None,
        "health_status": health_status,
        "health_summary": summary,
    }
