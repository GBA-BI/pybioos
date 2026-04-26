import argparse
import sys
from typing import Any, Iterable, Optional

from bioos import bioos_workflow, bw_import, bw_import_status_check, bw_status_check, get_submission_logs
from bioos.cli import (
    add_workspace_members,
    build_docker_image,
    check_build_status,
    check_ies_status,
    create_iesapp,
    create_workspace_bioos,
    delete_workspace_members,
    delete_submission,
    download_files_from_workspace,
    export_bioos_workspace,
    fetch_wdl_from_dockstore,
    generate_inputs_json_template_bioos,
    get_docker_image_url,
    get_ies_events,
    get_workspace_profile,
    list_bioos_workspaces,
    list_files_from_workspace,
    list_workspace_members,
    list_submissions_from_workspace,
    list_workflows_from_workspace,
    search_dockstore,
    upload_dashboard_file,
    upload_files_to_workspace,
    usage_metrics,
    update_workspace_members,
    validate_wdl,
)
from bioos.cli.common import add_argument, add_auth_arguments, add_bool_argument, add_output_arguments, run_cli
from bioos.cli.config_store import EXAMPLE_CONFIG, get_config_path
from bioos.ops.auth import login_to_bioos, resolve_auth_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bioos",
        description="Bio-OS command line interface.",
    )
    subparsers = parser.add_subparsers(dest="command")

    _add_auth_group(subparsers)
    _add_config_group(subparsers)
    _add_workspace_group(subparsers)
    _add_workflow_group(subparsers)
    _add_submission_group(subparsers)
    _add_file_group(subparsers)
    _add_usage_group(subparsers)
    _add_ies_group(subparsers)
    _add_dockstore_group(subparsers)
    _add_docker_group(subparsers)

    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    handler = getattr(args, "handler", None)
    if handler is None:
        current_parser = getattr(args, "_parser", parser)
        current_parser.print_help()
        return 0
    return run_cli(handler, args)


def _add_auth_group(subparsers: Any) -> None:
    auth_parser = subparsers.add_parser("auth", help="Authentication and credential status commands.")
    auth_subparsers = auth_parser.add_subparsers(dest="auth_command")
    auth_parser.set_defaults(_parser=auth_parser)

    status_parser = auth_subparsers.add_parser("status", help="Show the resolved authentication status.")
    add_auth_arguments(status_parser)
    add_output_arguments(status_parser)
    status_parser.set_defaults(_parser=status_parser)
    status_parser.set_defaults(handler=_handle_auth_status)


def _add_config_group(subparsers: Any) -> None:
    config_parser = subparsers.add_parser("config", help="Inspect local CLI configuration.")
    config_subparsers = config_parser.add_subparsers(dest="config_command")
    config_parser.set_defaults(_parser=config_parser)

    path_parser = config_subparsers.add_parser("path", help="Show the config file path used by the CLI.")
    add_output_arguments(path_parser)
    path_parser.set_defaults(_parser=path_parser)
    path_parser.set_defaults(handler=_handle_config_path)

    example_parser = config_subparsers.add_parser("example", help="Print an example ~/.bioos/config.yaml payload.")
    add_output_arguments(example_parser)
    example_parser.set_defaults(_parser=example_parser)
    example_parser.set_defaults(handler=_handle_config_example)


def _add_workspace_group(subparsers: Any) -> None:
    workspace_parser = subparsers.add_parser("workspace", help="Workspace commands.")
    workspace_subparsers = workspace_parser.add_subparsers(dest="workspace_command")
    workspace_parser.set_defaults(_parser=workspace_parser)

    list_parser = workspace_subparsers.add_parser("list", help="List Bio-OS workspaces.")
    add_auth_arguments(list_parser)
    add_output_arguments(list_parser)
    add_argument(
        list_parser,
        "page_size",
        required=False,
        type=int,
        default=None,
        help="Maximum number of workspaces to return. This is applied locally after fetching.",
    )
    list_parser.set_defaults(_parser=list_parser)
    list_parser.set_defaults(handler=list_bioos_workspaces.handle)

    create_parser = workspace_subparsers.add_parser("create", help="Create a Bio-OS workspace.")
    add_auth_arguments(create_parser)
    add_output_arguments(create_parser)
    add_argument(create_parser, "workspace_name", required=True, help="Workspace name to create.")
    add_argument(create_parser, "workspace_description", required=True, help="Workspace description.")
    create_parser.set_defaults(_parser=create_parser)
    create_parser.set_defaults(handler=create_workspace_bioos.handle)

    export_parser = workspace_subparsers.add_parser("export", help="Export workspace metadata to a local path.")
    add_auth_arguments(export_parser)
    add_output_arguments(export_parser)
    add_argument(export_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(export_parser, "export_path", required=True, help="Local export path.")
    export_parser.set_defaults(_parser=export_parser)
    export_parser.set_defaults(handler=export_bioos_workspace.handle)

    profile_parser = workspace_subparsers.add_parser("profile", help="Get a high-level Bio-OS workspace profile.")
    add_auth_arguments(profile_parser)
    add_output_arguments(profile_parser)
    add_argument(profile_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(
        profile_parser,
        "submission_limit",
        required=False,
        type=int,
        default=5,
        help="Number of recent submissions to include.",
    )
    add_argument(
        profile_parser,
        "artifact_limit_per_submission",
        required=False,
        type=int,
        default=10,
        help="Number of artifact samples to keep per submission.",
    )
    add_argument(
        profile_parser,
        "sample_rows_per_data_model",
        required=False,
        type=int,
        default=3,
        help="Number of sample rows to include per data model.",
    )
    add_bool_argument(profile_parser, "include_artifacts", default=True, help_text="Include artifact summaries.")
    add_bool_argument(
        profile_parser,
        "include_failure_details",
        default=True,
        help_text="Include run-level failure summaries.",
    )
    add_bool_argument(profile_parser, "include_ies", default=True, help_text="Include IES information.")
    add_bool_argument(
        profile_parser,
        "include_signed_urls",
        default=False,
        help_text="Include signed file URLs in artifact summaries.",
    )
    profile_parser.set_defaults(_parser=profile_parser)
    profile_parser.set_defaults(handler=get_workspace_profile.handle)

    dashboard_parser = workspace_subparsers.add_parser(
        "dashboard-upload",
        help="Upload __dashboard__.md to the root of a workspace bucket.",
    )
    add_auth_arguments(dashboard_parser)
    add_output_arguments(dashboard_parser)
    add_argument(dashboard_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(dashboard_parser, "local_file_path", required=True, help="Path to __dashboard__.md.")
    dashboard_parser.set_defaults(_parser=dashboard_parser)
    dashboard_parser.set_defaults(handler=upload_dashboard_file.handle)

    member_parser = workspace_subparsers.add_parser("member", help="Workspace member management commands.")
    member_subparsers = member_parser.add_subparsers(dest="member_command")
    member_parser.set_defaults(_parser=member_parser)

    member_list_parser = member_subparsers.add_parser("list", help="List workspace members.")
    add_auth_arguments(member_list_parser)
    add_output_arguments(member_list_parser)
    add_argument(member_list_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(member_list_parser, "page_number", required=False, type=int, help="Page number.")
    add_argument(member_list_parser, "page_size", required=False, type=int, help="Page size.")
    add_bool_argument(
        member_list_parser,
        "in_workspace",
        default=True,
        help_text="Only list users already in the workspace.",
    )
    add_argument(
        member_list_parser,
        "role",
        required=False,
        action="append",
        help="Optional member role filter. Can be specified multiple times.",
    )
    add_argument(
        member_list_parser,
        "keyword",
        required=False,
        help="Optional username keyword filter.",
    )
    member_list_parser.set_defaults(_parser=member_list_parser)
    member_list_parser.set_defaults(handler=list_workspace_members.handle)

    member_add_parser = member_subparsers.add_parser("add", help="Add members to a workspace.")
    add_auth_arguments(member_add_parser)
    add_output_arguments(member_add_parser)
    add_argument(member_add_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(
        member_add_parser,
        "name",
        required=False,
        action="append",
        help="Member username. Can be specified multiple times.",
    )
    add_argument(
        member_add_parser,
        "role",
        required=True,
        help="Workspace member role: Visitor, User, or Admin.",
    )
    member_add_parser.set_defaults(_parser=member_add_parser)
    member_add_parser.set_defaults(handler=add_workspace_members.handle)

    member_update_parser = member_subparsers.add_parser("update", help="Update workspace members.")
    add_auth_arguments(member_update_parser)
    add_output_arguments(member_update_parser)
    add_argument(member_update_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(
        member_update_parser,
        "name",
        required=True,
        action="append",
        help="Member username. Can be specified multiple times.",
    )
    add_argument(
        member_update_parser,
        "role",
        required=True,
        help="Workspace member role: Visitor, User, or Admin.",
    )
    member_update_parser.set_defaults(_parser=member_update_parser)
    member_update_parser.set_defaults(handler=update_workspace_members.handle)

    member_delete_parser = member_subparsers.add_parser("delete", help="Delete members from a workspace.")
    add_auth_arguments(member_delete_parser)
    add_output_arguments(member_delete_parser)
    add_argument(member_delete_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(
        member_delete_parser,
        "name",
        required=False,
        action="append",
        help="Member username. Can be specified multiple times.",
    )
    member_delete_parser.set_defaults(_parser=member_delete_parser)
    member_delete_parser.set_defaults(handler=delete_workspace_members.handle)


def _add_workflow_group(subparsers: Any) -> None:
    workflow_parser = subparsers.add_parser("workflow", help="Workflow commands.")
    workflow_subparsers = workflow_parser.add_subparsers(dest="workflow_command")
    workflow_parser.set_defaults(_parser=workflow_parser)

    list_parser = workflow_subparsers.add_parser("list", help="List workflows from a workspace.")
    add_auth_arguments(list_parser)
    add_output_arguments(list_parser)
    add_argument(list_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(list_parser, "search_keyword", required=False, default=None, help="Optional workflow keyword.")
    add_argument(list_parser, "page_number", required=False, type=int, default=1, help="Page number.")
    add_argument(list_parser, "page_size", required=False, type=int, default=10, help="Page size.")
    list_parser.set_defaults(_parser=list_parser)
    list_parser.set_defaults(handler=list_workflows_from_workspace.handle)

    input_template_parser = workflow_subparsers.add_parser(
        "input-template",
        help="Generate the inputs template for a workflow.",
    )
    add_auth_arguments(input_template_parser)
    add_output_arguments(input_template_parser)
    add_argument(input_template_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(input_template_parser, "workflow_name", required=True, help="Workflow name.")
    input_template_parser.set_defaults(_parser=input_template_parser)
    input_template_parser.set_defaults(handler=generate_inputs_json_template_bioos.handle)

    import_parser = workflow_subparsers.add_parser("import", help="Import a workflow into Bio-OS.")
    add_auth_arguments(import_parser)
    add_output_arguments(import_parser)
    add_argument(import_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(import_parser, "workflow_name", required=True, help="Workflow name.")
    add_argument(import_parser, "workflow_source", required=True, help="Local WDL file path or git repository URL.")
    add_argument(import_parser, "workflow_desc", required=False, default="", help="Workflow description.")
    add_argument(import_parser, "main_path", required=False, default="", help="Main workflow file path.")
    add_bool_argument(import_parser, "monitor", default=False, help_text="Monitor the workflow import status.")
    add_argument(
        import_parser,
        "monitor_interval",
        required=False,
        type=int,
        default=60,
        help="Polling interval in seconds.",
    )
    import_parser.set_defaults(_parser=import_parser, output="text")
    import_parser.set_defaults(handler=bw_import.handle)

    import_status_parser = workflow_subparsers.add_parser(
        "import-status",
        help="Check workflow import status.",
    )
    add_auth_arguments(import_status_parser)
    add_output_arguments(import_status_parser)
    add_argument(import_status_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(import_status_parser, "workflow_id", required=True, help="Workflow ID.")
    import_status_parser.set_defaults(_parser=import_status_parser, output="text")
    import_status_parser.set_defaults(handler=bw_import_status_check.handle)

    run_status_parser = workflow_subparsers.add_parser("run-status", help="Check workflow run status.")
    add_auth_arguments(run_status_parser)
    add_output_arguments(run_status_parser)
    add_argument(run_status_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(run_status_parser, "submission_id", required=True, help="Submission ID.")
    add_argument(run_status_parser, "page_size", required=False, type=int, default=0, help="Page size.")
    run_status_parser.set_defaults(_parser=run_status_parser, output="text")
    run_status_parser.set_defaults(handler=bw_status_check.handle)

    submit_parser = workflow_subparsers.add_parser("submit", help="Submit a Bio-OS workflow run.")
    add_auth_arguments(submit_parser)
    add_output_arguments(submit_parser)
    add_argument(submit_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(submit_parser, "workflow_name", required=True, help="Workflow name.")
    add_argument(submit_parser, "input_json", required=True, help="Input JSON file path.")
    add_argument(submit_parser, "data_model_name", required=False, default="dm", help="Generated data model name.")
    add_bool_argument(submit_parser, "call_caching", default=False, help_text="Enable call caching.")
    add_argument(
        submit_parser,
        "submission_desc",
        required=False,
        default="Submit by pybioos.",
        help="Submission description.",
    )
    add_bool_argument(submit_parser, "force_reupload", default=False, help_text="Force file re-upload.")
    add_bool_argument(submit_parser, "mount_tos", default=False, help_text="Mount TOS.")
    add_bool_argument(submit_parser, "monitor", default=False, help_text="Monitor submission until completion.")
    add_argument(
        submit_parser,
        "monitor_interval",
        required=False,
        type=int,
        default=600,
        help="Polling interval in seconds.",
    )
    add_bool_argument(submit_parser, "download_results", default=False, help_text="Download results after completion.")
    add_argument(submit_parser, "download_dir", required=False, default=".", help="Local download directory.")
    submit_parser.set_defaults(_parser=submit_parser, output="text")
    submit_parser.set_defaults(handler=bioos_workflow.handle)

    validate_parser = workflow_subparsers.add_parser("validate", help="Validate a WDL file with womtool.")
    add_output_arguments(validate_parser)
    add_argument(validate_parser, "wdl_path", required=True, help="Path to the WDL file.")
    validate_parser.set_defaults(_parser=validate_parser)
    validate_parser.set_defaults(handler=validate_wdl.handle)


def _add_submission_group(subparsers: Any) -> None:
    submission_parser = subparsers.add_parser("submission", help="Submission commands.")
    submission_subparsers = submission_parser.add_subparsers(dest="submission_command")
    submission_parser.set_defaults(_parser=submission_parser)

    list_parser = submission_subparsers.add_parser("list", help="List submissions from a workspace.")
    add_auth_arguments(list_parser)
    add_output_arguments(list_parser)
    add_argument(list_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(list_parser, "workflow_name", required=False, default=None, help="Optional workflow name filter.")
    add_argument(list_parser, "search_keyword", required=False, default=None, help="Optional submission keyword.")
    add_argument(list_parser, "status", required=False, default=None, help="Optional submission status filter.")
    add_argument(list_parser, "page_number", required=False, type=int, default=1, help="Page number.")
    add_argument(list_parser, "page_size", required=False, type=int, default=10, help="Page size.")
    list_parser.set_defaults(_parser=list_parser)
    list_parser.set_defaults(handler=list_submissions_from_workspace.handle)

    delete_parser = submission_subparsers.add_parser("delete", help="Delete a submission from a workspace.")
    add_auth_arguments(delete_parser)
    add_output_arguments(delete_parser)
    add_argument(delete_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(delete_parser, "submission_id", required=True, help="Submission ID to delete.")
    delete_parser.set_defaults(_parser=delete_parser)
    delete_parser.set_defaults(handler=delete_submission.handle)

    logs_parser = submission_subparsers.add_parser("logs", help="Download workflow submission logs.")
    add_auth_arguments(logs_parser)
    add_output_arguments(logs_parser)
    add_argument(logs_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(logs_parser, "submission_id", required=True, help="Submission ID.")
    add_argument(logs_parser, "output_dir", required=False, default=".", help="Local directory to save the logs.")
    logs_parser.set_defaults(_parser=logs_parser, output="text")
    logs_parser.set_defaults(handler=get_submission_logs.handle)


def _add_file_group(subparsers: Any) -> None:
    file_parser = subparsers.add_parser("file", help="Workspace file commands.")
    file_subparsers = file_parser.add_subparsers(dest="file_command")
    file_parser.set_defaults(_parser=file_parser)

    upload_parser = file_subparsers.add_parser("upload", help="Upload local files to a workspace.")
    add_auth_arguments(upload_parser)
    add_output_arguments(upload_parser)
    add_argument(upload_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(
        upload_parser,
        "source",
        required=True,
        action="append",
        help="Local file path. Can be specified multiple times.",
    )
    add_argument(upload_parser, "target", required=False, default="", help="Target prefix path in the workspace bucket.")
    add_bool_argument(upload_parser, "flatten", default=True, help_text="Flatten local paths during upload.")
    add_bool_argument(
        upload_parser,
        "skip_existing",
        default=False,
        help_text="Skip files whose target object already exists.",
    )
    add_argument(
        upload_parser,
        "checkpoint_dir",
        required=False,
        default=None,
        help="Directory for resumable upload checkpoints.",
    )
    add_argument(
        upload_parser,
        "max_retries",
        required=False,
        type=int,
        default=3,
        help="Number of retries per file after the initial attempt.",
    )
    add_argument(
        upload_parser,
        "task_num",
        required=False,
        type=int,
        default=10,
        help="Parallel task count for multipart uploads.",
    )
    upload_parser.set_defaults(_parser=upload_parser)
    upload_parser.set_defaults(handler=upload_files_to_workspace.handle)

    list_parser = file_subparsers.add_parser("list", help="List files from a workspace.")
    add_auth_arguments(list_parser)
    add_output_arguments(list_parser)
    add_argument(list_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(list_parser, "prefix", required=False, default="", help="Prefix path to list.")
    add_bool_argument(list_parser, "recursive", default=False, help_text="List files recursively.")
    list_parser.set_defaults(_parser=list_parser)
    list_parser.set_defaults(handler=list_files_from_workspace.handle)

    download_parser = file_subparsers.add_parser("download", help="Download files from a workspace.")
    add_auth_arguments(download_parser)
    add_output_arguments(download_parser)
    add_argument(download_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(
        download_parser,
        "source",
        required=True,
        action="append",
        help="Source file path in the workspace. Can be specified multiple times.",
    )
    add_argument(download_parser, "target", required=True, help="Local target path.")
    add_bool_argument(download_parser, "flatten", default=False, help_text="Flatten directories during download.")
    download_parser.set_defaults(_parser=download_parser)
    download_parser.set_defaults(handler=download_files_from_workspace.handle)


def _add_ies_group(subparsers: Any) -> None:
    ies_parser = subparsers.add_parser("ies", help="IES application commands.")
    ies_subparsers = ies_parser.add_subparsers(dest="ies_command")
    ies_parser.set_defaults(_parser=ies_parser)

    create_parser = ies_subparsers.add_parser("create", help="Create a new IES application instance.")
    add_auth_arguments(create_parser)
    add_output_arguments(create_parser)
    add_argument(create_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(create_parser, "ies_name", required=True, help="IES instance name.")
    add_argument(create_parser, "ies_desc", required=True, help="IES instance description.")
    add_argument(create_parser, "ies_resource", required=False, default="2c-4gib", help="Resource size.")
    add_argument(
        create_parser,
        "ies_storage",
        required=False,
        type=int,
        default=42949672960,
        help="Storage capacity in bytes.",
    )
    add_argument(
        create_parser,
        "ies_image",
        required=False,
        default="registry-vpc.miracle.ac.cn/infcprelease/ies-default:v0.0.14",
        help="Docker image URL.",
    )
    add_bool_argument(create_parser, "ies_ssh", default=True, help_text="Enable SSH.")
    add_argument(
        create_parser,
        "ies_run_limit",
        required=False,
        type=int,
        default=10800,
        help="Maximum running time in seconds.",
    )
    add_argument(
        create_parser,
        "ies_idle_timeout",
        required=False,
        type=int,
        default=10800,
        help="Idle timeout in seconds.",
    )
    add_bool_argument(create_parser, "ies_auto_start", default=True, help_text="Auto-start the instance.")
    create_parser.set_defaults(_parser=create_parser)
    create_parser.set_defaults(handler=create_iesapp.handle)

    status_parser = ies_subparsers.add_parser("status", help="Check IES instance status.")
    add_auth_arguments(status_parser)
    add_output_arguments(status_parser)
    add_argument(status_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(status_parser, "ies_name", required=True, help="IES instance name.")
    status_parser.set_defaults(_parser=status_parser)
    status_parser.set_defaults(handler=check_ies_status.handle)

    events_parser = ies_subparsers.add_parser("events", help="Get IES events.")
    add_auth_arguments(events_parser)
    add_output_arguments(events_parser)
    add_argument(events_parser, "workspace_name", required=True, help="Workspace name.")
    add_argument(events_parser, "ies_name", required=True, help="IES instance name.")
    events_parser.set_defaults(_parser=events_parser)
    events_parser.set_defaults(handler=get_ies_events.handle)


def _add_usage_group(subparsers: Any) -> None:
    usage_parser = subparsers.add_parser("usage", help="Account-level usage and asset metrics.")
    usage_subparsers = usage_parser.add_subparsers(dest="usage_command")
    usage_parser.set_defaults(_parser=usage_parser)

    asset_data_parser = usage_subparsers.add_parser("asset-data", help="Get asset usage time-series data.")
    add_auth_arguments(asset_data_parser)
    add_output_arguments(asset_data_parser)
    add_argument(asset_data_parser, "start_time", required=True, type=int, help="Start timestamp at an exact hour.")
    add_argument(asset_data_parser, "end_time", required=True, type=int, help="End timestamp at an exact hour.")
    add_argument(
        asset_data_parser,
        "type",
        required=True,
        help="Asset usage type: WorkspaceVisit or WorkflowUse.",
    )
    asset_data_parser.set_defaults(_parser=asset_data_parser)
    asset_data_parser.set_defaults(handler=usage_metrics.handle_asset_usage_data)

    asset_list_parser = usage_subparsers.add_parser("asset-list", help="List asset usage records.")
    add_auth_arguments(asset_list_parser)
    add_output_arguments(asset_list_parser)
    add_argument(asset_list_parser, "start_time", required=True, type=int, help="Start timestamp at an exact hour.")
    add_argument(asset_list_parser, "end_time", required=True, type=int, help="End timestamp at an exact hour.")
    add_argument(
        asset_list_parser,
        "type",
        required=True,
        help="Asset usage type: WorkspaceVisit or WorkflowUse.",
    )
    asset_list_parser.set_defaults(_parser=asset_list_parser)
    asset_list_parser.set_defaults(handler=usage_metrics.handle_asset_usage_list)

    asset_total_parser = usage_subparsers.add_parser("asset-total", help="Get total asset usage.")
    add_auth_arguments(asset_total_parser)
    add_output_arguments(asset_total_parser)
    add_argument(asset_total_parser, "start_time", required=True, type=int, help="Start timestamp at an exact hour.")
    add_argument(asset_total_parser, "end_time", required=True, type=int, help="End timestamp at an exact hour.")
    add_argument(
        asset_total_parser,
        "type",
        required=True,
        help="Asset usage type: WorkspaceVisit or WorkflowUse.",
    )
    asset_total_parser.set_defaults(_parser=asset_total_parser)
    asset_total_parser.set_defaults(handler=usage_metrics.handle_asset_usage_total)

    resource_data_parser = usage_subparsers.add_parser("resource-data", help="Get resource usage time-series data.")
    add_auth_arguments(resource_data_parser)
    add_output_arguments(resource_data_parser)
    add_argument(resource_data_parser, "start_time", required=True, type=int, help="Start timestamp at an exact hour.")
    add_argument(resource_data_parser, "end_time", required=True, type=int, help="End timestamp at an exact hour.")
    add_argument(
        resource_data_parser,
        "type",
        required=True,
        help="Resource usage type: cpu, memory, storage, tos, or gpu.",
    )
    add_argument(
        resource_data_parser,
        "sub_dimension",
        required=False,
        action="append",
        help="Optional sub-dimension. Can be specified multiple times.",
    )
    resource_data_parser.set_defaults(_parser=resource_data_parser)
    resource_data_parser.set_defaults(handler=usage_metrics.handle_resource_usage_data)

    resource_workspace_list_parser = usage_subparsers.add_parser(
        "resource-workspace-list",
        help="List workspace resource usage.",
    )
    add_auth_arguments(resource_workspace_list_parser)
    add_output_arguments(resource_workspace_list_parser)
    add_argument(
        resource_workspace_list_parser,
        "start_time",
        required=True,
        type=int,
        help="Start timestamp at an exact hour.",
    )
    add_argument(
        resource_workspace_list_parser,
        "end_time",
        required=True,
        type=int,
        help="End timestamp at an exact hour.",
    )
    resource_workspace_list_parser.set_defaults(_parser=resource_workspace_list_parser)
    resource_workspace_list_parser.set_defaults(handler=usage_metrics.handle_workspace_resource_usage)

    resource_user_list_parser = usage_subparsers.add_parser(
        "resource-user-list",
        help="List user resource usage.",
    )
    add_auth_arguments(resource_user_list_parser)
    add_output_arguments(resource_user_list_parser)
    add_argument(
        resource_user_list_parser,
        "start_time",
        required=True,
        type=int,
        help="Start timestamp at an exact hour.",
    )
    add_argument(
        resource_user_list_parser,
        "end_time",
        required=True,
        type=int,
        help="End timestamp at an exact hour.",
    )
    resource_user_list_parser.set_defaults(_parser=resource_user_list_parser)
    resource_user_list_parser.set_defaults(handler=usage_metrics.handle_user_resource_usage)

    resource_total_parser = usage_subparsers.add_parser("resource-total", help="Get total resource usage.")
    add_auth_arguments(resource_total_parser)
    add_output_arguments(resource_total_parser)
    add_argument(resource_total_parser, "start_time", required=True, type=int, help="Start timestamp at an exact hour.")
    add_argument(resource_total_parser, "end_time", required=True, type=int, help="End timestamp at an exact hour.")
    resource_total_parser.set_defaults(_parser=resource_total_parser)
    resource_total_parser.set_defaults(handler=usage_metrics.handle_total_resource_usage)


def _add_dockstore_group(subparsers: Any) -> None:
    dockstore_parser = subparsers.add_parser("dockstore", help="Dockstore discovery commands.")
    dockstore_subparsers = dockstore_parser.add_subparsers(dest="dockstore_command")
    dockstore_parser.set_defaults(_parser=dockstore_parser)

    search_parser = dockstore_subparsers.add_parser("search", help="Search workflows from Dockstore.")
    add_output_arguments(search_parser)
    search_parser.add_argument(
        "--query",
        action="append",
        nargs=3,
        metavar=("FIELD", "OPERATOR", "TERM"),
        required=True,
        help="Query tuple: field operator term. Can be repeated.",
    )
    add_argument(search_parser, "top_n", required=False, type=int, default=3, help="Number of top results to return.")
    add_argument(
        search_parser,
        "query_type",
        required=False,
        default="match_phrase",
        choices=("match_phrase", "wildcard"),
        help="Search query type.",
    )
    add_bool_argument(search_parser, "sentence", default=False, help_text="Treat search terms as sentence queries.")
    add_bool_argument(search_parser, "output_full", default=False, help_text="Include more workflow metadata.")
    search_parser.set_defaults(_parser=search_parser)
    search_parser.set_defaults(handler=search_dockstore.handle)

    fetch_parser = dockstore_subparsers.add_parser("fetch", help="Download workflow files from Dockstore.")
    add_output_arguments(fetch_parser)
    add_argument(fetch_parser, "url", required=True, help="Dockstore workflow URL or path.")
    add_argument(fetch_parser, "output_path", required=False, default=".", help="Output directory.")
    fetch_parser.set_defaults(_parser=fetch_parser)
    fetch_parser.set_defaults(handler=fetch_wdl_from_dockstore.handle)


def _add_docker_group(subparsers: Any) -> None:
    docker_parser = subparsers.add_parser("docker", help="Docker helper commands.")
    docker_subparsers = docker_parser.add_subparsers(dest="docker_command")
    docker_parser.set_defaults(_parser=docker_parser)

    build_parser = docker_subparsers.add_parser("build", help="Submit a Docker image build request.")
    add_output_arguments(build_parser)
    add_argument(build_parser, "repo_name", required=True, help="Repository name.")
    add_argument(build_parser, "tag", required=True, help="Image tag.")
    add_argument(build_parser, "source_path", required=True, help="Path to Dockerfile or zip archive.")
    add_argument(
        build_parser,
        "registry",
        required=False,
        default="registry-vpc.miracle.ac.cn",
        help="Registry.",
    )
    add_argument(
        build_parser,
        "namespace_name",
        required=False,
        default="auto-build",
        help="Registry namespace.",
    )
    build_parser.set_defaults(_parser=build_parser)
    build_parser.set_defaults(handler=build_docker_image.handle)

    status_parser = docker_subparsers.add_parser("status", help="Check Docker image build status.")
    add_output_arguments(status_parser)
    add_argument(status_parser, "task_id", required=True, help="Build task ID.")
    status_parser.set_defaults(_parser=status_parser)
    status_parser.set_defaults(handler=check_build_status.handle)

    url_parser = docker_subparsers.add_parser("url", help="Build the full Docker image URL.")
    add_output_arguments(url_parser)
    add_argument(url_parser, "repo_name", required=True, help="Repository name.")
    add_argument(url_parser, "tag", required=True, help="Image tag.")
    add_argument(url_parser, "registry", required=False, default="registry-vpc.miracle.ac.cn", help="Registry.")
    add_argument(url_parser, "namespace_name", required=False, default="auto-build", help="Registry namespace.")
    url_parser.set_defaults(_parser=url_parser)
    url_parser.set_defaults(handler=get_docker_image_url.handle)


def _handle_auth_status(args: argparse.Namespace) -> dict:
    settings = resolve_auth_settings(
        access_key=getattr(args, "ak", None),
        secret_key=getattr(args, "sk", None),
        endpoint=getattr(args, "endpoint", None),
    )
    status = {
        "success": False,
        "login_status": "Not logged in",
        "config_path": settings["config_path"],
        "config_exists": get_config_path().is_file(),
        "access_key_source": settings["access_key_source"],
        "secret_key_source": settings["secret_key_source"],
        "endpoint_source": settings["endpoint_source"],
        "endpoint": settings["endpoint"],
        "access_key": _mask_value(settings["access_key"]),
    }

    try:
        login_to_bioos(
            access_key=getattr(args, "ak", None),
            secret_key=getattr(args, "sk", None),
            endpoint=getattr(args, "endpoint", None),
        )
        from bioos import bioos

        login_info = bioos.status()
        status.update(
            {
                "success": True,
                "login_status": login_info.login_status,
                "region": login_info.region,
            }
        )
    except Exception as exc:
        status["error"] = str(exc)

    return status


def _handle_config_path(_: argparse.Namespace) -> dict:
    config_path = get_config_path()
    return {
        "config_path": str(config_path),
        "exists": config_path.is_file(),
    }


def _handle_config_example(_: argparse.Namespace) -> dict:
    return {
        "config_path": str(get_config_path()),
        "example": EXAMPLE_CONFIG,
    }


def _mask_value(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if len(value) <= 6:
        return "*" * len(value)
    return f"{value[:4]}...{value[-2:]}"




if __name__ == "__main__":
    sys.exit(main())
