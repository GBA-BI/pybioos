import argparse
import re
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urlparse

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
from bioos.cli.common import (
    add_argument,
    add_auth_arguments,
    add_bool_argument,
    add_json_input_arguments,
    add_output_arguments,
    load_json_input,
    run_cli,
)
from bioos.cli.config_store import EXAMPLE_CONFIG, get_config_path
from bioos.ops.auth import login_to_bioos, resolve_auth_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bioos",
        description="Bio-OS command line interface.",
    )
    subparsers = parser.add_subparsers(dest="command")

    _add_auth_group(subparsers)
    _add_aai_group(subparsers)
    _add_account_group(subparsers)
    _add_config_group(subparsers)
    _add_workspace_group(subparsers)
    _add_workflow_group(subparsers)
    _add_submission_group(subparsers)
    _add_file_group(subparsers)
    _add_repo_group(subparsers)
    _add_datasite_group(subparsers)
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

    connect_aai_parser = auth_subparsers.add_parser(
        "connect-aai",
        help="Save BioOS SDK credentials, verify BioOS auth, then save/sync AAI repository passport in one step.",
    )
    add_auth_arguments(connect_aai_parser)
    add_output_arguments(connect_aai_parser)
    add_argument(connect_aai_parser, "account_name", required=False, default=None, help="BioOS main-site account name for password login.")
    add_argument(connect_aai_parser, "password", required=False, default=None, help="BioOS main-site password for password login.")
    add_argument(connect_aai_parser, "user_name", required=False, default=None, help="Optional sub-user name for main-site user login.")
    add_argument(connect_aai_parser, "web_url", required=False, default=None, help="BioOS main site web URL override.")
    add_argument(connect_aai_parser, "login_token", required=False, default=None, help="BioOS main site X-LoginToken override.")
    add_argument(connect_aai_parser, "csrf_token", required=False, default=None, help="BioOS main site csrfToken override.")
    add_argument(connect_aai_parser, "cookie", required=False, default=None, help="BioOS main site web cookie override.")
    add_argument(connect_aai_parser, "curl", required=False, default=None, help="Inline cURL command copied from the BioOS main web GraphQL request.")
    add_argument(connect_aai_parser, "curl_file", required=False, default=None, help="Path to a text file containing the copied cURL command.")
    add_bool_argument(
        connect_aai_parser,
        "save_client",
        default=True,
        help_text="Save BioOS SDK credentials into config before AAI sync.",
    )
    add_bool_argument(
        connect_aai_parser,
        "save_main_web",
        default=True,
        help_text="Save resolved BioOS main-web auth materials into config before syncing passport.",
    )
    add_bool_argument(
        connect_aai_parser,
        "sync_passport",
        default=True,
        help_text="Get repository passport and save it into repo/datasite config.",
    )
    add_argument(connect_aai_parser, "expires_in", required=False, type=int, default=2592000, help="Passport expiration in seconds. Defaults to 30 days.")
    add_argument(connect_aai_parser, "config_path", required=False, default=None, help="Optional config path override.")
    connect_aai_parser.set_defaults(_parser=connect_aai_parser)
    connect_aai_parser.set_defaults(handler=_handle_auth_connect_aai)


def _add_aai_group(subparsers: Any) -> None:
    aai_parser = subparsers.add_parser("aai", help="AAI authentication commands.")
    aai_subparsers = aai_parser.add_subparsers(dest="aai_command")
    aai_parser.set_defaults(_parser=aai_parser)

    auth_parser = aai_subparsers.add_parser("auth", help="Show AAI auth settings.")
    add_output_arguments(auth_parser)
    add_argument(auth_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(
        auth_parser,
        "cookie",
        required=False,
        default=None,
        help="AAI session cookie override, e.g. ory_kratos_session=... .",
    )
    add_argument(auth_parser, "service", required=False, default="repo", help="Target service: repo or datasite.")
    auth_parser.set_defaults(_parser=auth_parser)
    auth_parser.set_defaults(handler=_handle_aai_auth)

    status_parser = aai_subparsers.add_parser("status", help="Show a consolidated view of main-web, repo, and datasite authentication status.")
    add_output_arguments(status_parser)
    add_argument(status_parser, "web_url", required=False, default=None, help="BioOS main site web URL override.")
    add_argument(status_parser, "login_token", required=False, default=None, help="BioOS main site X-LoginToken override.")
    add_argument(status_parser, "csrf_token", required=False, default=None, help="BioOS main site csrfToken override.")
    add_argument(status_parser, "cookie", required=False, default=None, help="BioOS main site web cookie override.")
    status_parser.set_defaults(_parser=status_parser)
    status_parser.set_defaults(handler=_handle_aai_status)

    import_curl_parser = aai_subparsers.add_parser("import-main-web-curl", help="Extract BioOS main web auth materials from a copied cURL command.")
    add_output_arguments(import_curl_parser)
    add_argument(import_curl_parser, "curl", required=False, default=None, help="Inline cURL command copied from the BioOS main web GraphQL request.")
    add_argument(import_curl_parser, "curl_file", required=False, default=None, help="Path to a text file containing the copied cURL command.")
    import_curl_parser.set_defaults(_parser=import_curl_parser)
    import_curl_parser.set_defaults(handler=_handle_aai_import_main_web_curl)

    login_parser = aai_subparsers.add_parser(
        "login",
        help="Unified AAI login entry: import BioOS main web auth materials, optionally save them, then sync repository passport.",
    )
    add_output_arguments(login_parser)
    add_argument(login_parser, "account_name", required=False, default=None, help="BioOS main-site account name for password login.")
    add_argument(login_parser, "password", required=False, default=None, help="BioOS main-site password for password login.")
    add_argument(login_parser, "user_name", required=False, default=None, help="Optional sub-user name for main-site user login.")
    add_argument(login_parser, "web_url", required=False, default=None, help="BioOS main site web URL override.")
    add_argument(login_parser, "login_token", required=False, default=None, help="BioOS main site X-LoginToken override.")
    add_argument(login_parser, "csrf_token", required=False, default=None, help="BioOS main site csrfToken override.")
    add_argument(login_parser, "cookie", required=False, default=None, help="BioOS main site web cookie override.")
    add_argument(login_parser, "curl", required=False, default=None, help="Inline cURL command copied from the BioOS main web GraphQL request.")
    add_argument(login_parser, "curl_file", required=False, default=None, help="Path to a text file containing the copied cURL command.")
    add_bool_argument(
        login_parser,
        "save_main_web",
        default=True,
        help_text="Save resolved BioOS main-web auth materials into config before syncing passport.",
    )
    add_bool_argument(
        login_parser,
        "sync_passport",
        default=True,
        help_text="Get repository passport and save it into repo/datasite config.",
    )
    add_argument(login_parser, "expires_in", required=False, type=int, default=2592000, help="Passport expiration in seconds. Defaults to 30 days.")
    add_argument(login_parser, "config_path", required=False, default=None, help="Optional config path override.")
    login_parser.set_defaults(_parser=login_parser)
    login_parser.set_defaults(handler=_handle_aai_login)

    refresh_parser = aai_subparsers.add_parser(
        "refresh",
        help="Refresh repository passport using saved or provided BioOS main-web auth materials.",
    )
    add_output_arguments(refresh_parser)
    add_argument(refresh_parser, "account_name", required=False, default=None, help="BioOS main-site account name for password login.")
    add_argument(refresh_parser, "password", required=False, default=None, help="BioOS main-site password for password login.")
    add_argument(refresh_parser, "user_name", required=False, default=None, help="Optional sub-user name for main-site user login.")
    add_argument(refresh_parser, "web_url", required=False, default=None, help="BioOS main site web URL override.")
    add_argument(refresh_parser, "login_token", required=False, default=None, help="BioOS main site X-LoginToken override.")
    add_argument(refresh_parser, "csrf_token", required=False, default=None, help="BioOS main site csrfToken override.")
    add_argument(refresh_parser, "cookie", required=False, default=None, help="BioOS main site web cookie override.")
    add_argument(refresh_parser, "curl", required=False, default=None, help="Inline cURL command copied from the BioOS main web GraphQL request.")
    add_argument(refresh_parser, "curl_file", required=False, default=None, help="Path to a text file containing the copied cURL command.")
    add_bool_argument(
        refresh_parser,
        "save_main_web",
        default=True,
        help_text="Save resolved BioOS main-web auth materials into config before refreshing passport.",
    )
    add_argument(refresh_parser, "expires_in", required=False, type=int, default=2592000, help="Passport expiration in seconds. Defaults to 30 days.")
    add_argument(refresh_parser, "config_path", required=False, default=None, help="Optional config path override.")
    refresh_parser.set_defaults(_parser=refresh_parser)
    refresh_parser.set_defaults(handler=_handle_aai_refresh)

    sync_curl_parser = aai_subparsers.add_parser("sync-from-curl", help="Extract BioOS main web auth materials from a copied cURL command, then get and save repository passport.")
    add_output_arguments(sync_curl_parser)
    add_argument(sync_curl_parser, "curl", required=False, default=None, help="Inline cURL command copied from the BioOS main web GraphQL request.")
    add_argument(sync_curl_parser, "curl_file", required=False, default=None, help="Path to a text file containing the copied cURL command.")
    add_argument(sync_curl_parser, "expires_in", required=False, type=int, default=2592000, help="Passport expiration in seconds. Defaults to 30 days.")
    add_argument(sync_curl_parser, "config_path", required=False, default=None, help="Optional config path override.")
    sync_curl_parser.set_defaults(_parser=sync_curl_parser)
    sync_curl_parser.set_defaults(handler=_handle_aai_sync_from_curl)

    account_parser = aai_subparsers.add_parser("account", help="AAI repository account linkage via BioOS main site.")
    account_subparsers = account_parser.add_subparsers(dest="aai_account_command")
    account_parser.set_defaults(_parser=account_parser)

    account_status_parser = account_subparsers.add_parser("status", help="Check whether the current BioOS web account is linked to repository AAI.")
    add_output_arguments(account_status_parser)
    add_argument(account_status_parser, "web_url", required=False, default=None, help="BioOS main site web URL override.")
    add_argument(account_status_parser, "login_token", required=False, default=None, help="BioOS main site X-LoginToken override.")
    add_argument(account_status_parser, "csrf_token", required=False, default=None, help="BioOS main site csrfToken override.")
    add_argument(account_status_parser, "cookie", required=False, default=None, help="BioOS main site web cookie override.")
    account_status_parser.set_defaults(_parser=account_status_parser)
    account_status_parser.set_defaults(handler=_handle_aai_account_status)

    passport_parser = aai_subparsers.add_parser("passport", help="Repository passport retrieval via BioOS main site.")
    passport_subparsers = passport_parser.add_subparsers(dest="aai_passport_command")
    passport_parser.set_defaults(_parser=passport_parser)

    passport_get_parser = passport_subparsers.add_parser("get", help="Get repository passport from current BioOS web account.")
    add_output_arguments(passport_get_parser)
    add_argument(passport_get_parser, "web_url", required=False, default=None, help="BioOS main site web URL override.")
    add_argument(passport_get_parser, "login_token", required=False, default=None, help="BioOS main site X-LoginToken override.")
    add_argument(passport_get_parser, "csrf_token", required=False, default=None, help="BioOS main site csrfToken override.")
    add_argument(passport_get_parser, "cookie", required=False, default=None, help="BioOS main site web cookie override.")
    add_argument(passport_get_parser, "expires_in", required=False, type=int, default=None, help="Optional passport expiration in seconds.")
    add_argument(
        passport_get_parser,
        "save_to",
        required=False,
        action="append",
        help="Save passport token into config section(s): repo, datasite. Can be repeated.",
    )
    add_argument(passport_get_parser, "config_path", required=False, default=None, help="Optional config path override when saving.")
    passport_get_parser.set_defaults(_parser=passport_get_parser)
    passport_get_parser.set_defaults(handler=_handle_aai_passport_get)

    sync_parser = aai_subparsers.add_parser("sync-from-bioos", help="Get repository passport from current BioOS web account and save it into repo/datasite config.")
    add_output_arguments(sync_parser)
    add_argument(sync_parser, "web_url", required=False, default=None, help="BioOS main site web URL override.")
    add_argument(sync_parser, "login_token", required=False, default=None, help="BioOS main site X-LoginToken override.")
    add_argument(sync_parser, "csrf_token", required=False, default=None, help="BioOS main site csrfToken override.")
    add_argument(sync_parser, "cookie", required=False, default=None, help="BioOS main site web cookie override.")
    add_argument(sync_parser, "expires_in", required=False, type=int, default=2592000, help="Passport expiration in seconds. Defaults to 30 days.")
    add_argument(sync_parser, "config_path", required=False, default=None, help="Optional config path override.")
    sync_parser.set_defaults(_parser=sync_parser)
    sync_parser.set_defaults(handler=_handle_aai_sync_from_bioos)


def _add_account_group(subparsers: Any) -> None:
    account_parser = subparsers.add_parser("account", help="BioOS and AAI account linking commands.")
    account_subparsers = account_parser.add_subparsers(dest="account_command")
    account_parser.set_defaults(_parser=account_parser)

    link_parser = account_subparsers.add_parser("link", help="Show account link settings.")
    add_output_arguments(link_parser)
    add_argument(link_parser, "config_path", required=False, default=None, help="Optional config path override.")
    link_parser.set_defaults(_parser=link_parser)
    link_parser.set_defaults(handler=_handle_account_link)


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


def _add_repo_group(subparsers: Any) -> None:
    repo_parser = subparsers.add_parser("repo", help="Repository commands.")
    repo_subparsers = repo_parser.add_subparsers(dest="repo_command")
    repo_parser.set_defaults(_parser=repo_parser)

    dataset_parser = repo_subparsers.add_parser("dataset", help="Repository data set commands.")
    dataset_subparsers = dataset_parser.add_subparsers(dest="dataset_command")
    dataset_parser.set_defaults(_parser=dataset_parser)

    dataset_list_parser = dataset_subparsers.add_parser("list", help="List repository data sets.")
    add_output_arguments(dataset_list_parser)
    add_argument(dataset_list_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_list_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_list_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(dataset_list_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(dataset_list_parser, "display_level", required=False, default="Minimal", help="Display level, e.g. Minimal.")
    add_argument(dataset_list_parser, "order_by", required=False, default="createTime:desc,id:desc", help="Sort order.")
    dataset_list_parser.set_defaults(_parser=dataset_list_parser)
    dataset_list_parser.set_defaults(handler=_handle_repo_dataset_list)

    dataset_get_parser = dataset_subparsers.add_parser("get", help="Get a repository data set.")
    add_output_arguments(dataset_get_parser)
    add_argument(dataset_get_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_get_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_get_parser, "id", required=True, help="Repository data set ID.")
    dataset_get_parser.set_defaults(_parser=dataset_get_parser)
    dataset_get_parser.set_defaults(handler=_handle_repo_dataset_get)

    dataset_export_parser = dataset_subparsers.add_parser("export", help="Start or request a repository data set export.")
    add_output_arguments(dataset_export_parser)
    add_json_input_arguments(dataset_export_parser)
    add_argument(dataset_export_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_export_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_export_parser, "data_set_id", required=True, help="Repository data set ID.")
    dataset_export_parser.set_defaults(_parser=dataset_export_parser)
    dataset_export_parser.set_defaults(handler=_handle_repo_dataset_export)

    dataset_import_parser = dataset_subparsers.add_parser("import", help="Import a repository data set from JSON payload.")
    add_output_arguments(dataset_import_parser)
    add_json_input_arguments(dataset_import_parser)
    add_argument(dataset_import_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_import_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    dataset_import_parser.set_defaults(_parser=dataset_import_parser)
    dataset_import_parser.set_defaults(handler=_handle_repo_dataset_import)

    dataset_archive_parser = dataset_subparsers.add_parser("archive-access", help="Get repository archive TOS access info.")
    add_output_arguments(dataset_archive_parser)
    add_argument(dataset_archive_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_archive_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_archive_parser, "path", required=False, default=None, help="Optional archive path.")
    dataset_archive_parser.set_defaults(_parser=dataset_archive_parser)
    dataset_archive_parser.set_defaults(handler=_handle_repo_dataset_archive_access)

    dataset_files_parser = dataset_subparsers.add_parser("files", help="List files under a repository data set.")
    add_output_arguments(dataset_files_parser)
    add_argument(dataset_files_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_files_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_files_parser, "data_set_id", required=True, help="Repository data set ID.")
    add_argument(dataset_files_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(dataset_files_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(dataset_files_parser, "order_by", required=False, default=None, help="Sort order, e.g. id:asc.")
    dataset_files_parser.set_defaults(_parser=dataset_files_parser)
    dataset_files_parser.set_defaults(handler=_handle_repo_dataset_files)

    dataset_file_ids_parser = dataset_subparsers.add_parser("file-ids", help="List file IDs under a repository data set.")
    add_output_arguments(dataset_file_ids_parser)
    add_argument(dataset_file_ids_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_file_ids_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_file_ids_parser, "data_set_id", required=True, help="Repository data set ID.")
    dataset_file_ids_parser.set_defaults(_parser=dataset_file_ids_parser)
    dataset_file_ids_parser.set_defaults(handler=_handle_repo_dataset_file_ids)

    dac_parser = repo_subparsers.add_parser("dac", help="Repository DAC commands.")
    dac_subparsers = dac_parser.add_subparsers(dest="dac_command")
    dac_parser.set_defaults(_parser=dac_parser)

    dac_list_parser = dac_subparsers.add_parser("list", help="List DACs.")
    add_output_arguments(dac_list_parser)
    add_argument(dac_list_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dac_list_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dac_list_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(dac_list_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(dac_list_parser, "limit", required=False, type=int, default=None, help="Optional limit.")
    add_argument(dac_list_parser, "scope", required=False, action="append", help="Scope filter. Can be repeated.")
    dac_list_parser.set_defaults(_parser=dac_list_parser)
    dac_list_parser.set_defaults(handler=_handle_repo_dac_list)

    dac_create_parser = dac_subparsers.add_parser("create", help="Create a DAC from JSON payload.")
    add_output_arguments(dac_create_parser)
    add_json_input_arguments(dac_create_parser)
    add_argument(dac_create_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dac_create_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    dac_create_parser.set_defaults(_parser=dac_create_parser)
    dac_create_parser.set_defaults(handler=_handle_repo_dac_create)

    dac_update_parser = dac_subparsers.add_parser("update", help="Update a DAC from JSON payload.")
    add_output_arguments(dac_update_parser)
    add_json_input_arguments(dac_update_parser)
    add_argument(dac_update_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dac_update_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dac_update_parser, "id", required=True, help="DAC ID.")
    dac_update_parser.set_defaults(_parser=dac_update_parser)
    dac_update_parser.set_defaults(handler=_handle_repo_dac_update)

    dac_delete_parser = dac_subparsers.add_parser("delete", help="Delete a DAC.")
    add_output_arguments(dac_delete_parser)
    add_argument(dac_delete_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dac_delete_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dac_delete_parser, "id", required=True, help="DAC ID.")
    dac_delete_parser.set_defaults(_parser=dac_delete_parser)
    dac_delete_parser.set_defaults(handler=_handle_repo_dac_delete)

    dac_check_parser = dac_subparsers.add_parser("check-create", help="Check whether a DAC can be created.")
    add_output_arguments(dac_check_parser)
    add_argument(dac_check_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dac_check_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dac_check_parser, "name", required=False, default=None, help="Optional DAC name to validate.")
    dac_check_parser.set_defaults(_parser=dac_check_parser)
    dac_check_parser.set_defaults(handler=_handle_repo_dac_check)

    member_parser = dac_subparsers.add_parser("member", help="Repository DAC member commands.")
    member_subparsers = member_parser.add_subparsers(dest="member_command")
    member_parser.set_defaults(_parser=member_parser)

    member_list_parser = member_subparsers.add_parser("list", help="List DAC members.")
    add_output_arguments(member_list_parser)
    add_argument(member_list_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(member_list_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(member_list_parser, "dac_id", required=True, help="DAC ID.")
    add_argument(member_list_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(member_list_parser, "size", required=False, type=int, default=None, help="Page size.")
    member_list_parser.set_defaults(_parser=member_list_parser)
    member_list_parser.set_defaults(handler=_handle_repo_dac_member_list)

    member_upsert_parser = member_subparsers.add_parser("upsert", help="Create or update DAC members from JSON payload.")
    add_output_arguments(member_upsert_parser)
    add_json_input_arguments(member_upsert_parser)
    add_argument(member_upsert_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(member_upsert_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(member_upsert_parser, "dac_id", required=True, help="DAC ID.")
    member_upsert_parser.set_defaults(_parser=member_upsert_parser)
    member_upsert_parser.set_defaults(handler=_handle_repo_dac_member_upsert)

    member_remove_parser = member_subparsers.add_parser("remove", help="Remove DAC members using JSON payload.")
    add_output_arguments(member_remove_parser)
    add_json_input_arguments(member_remove_parser)
    add_argument(member_remove_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(member_remove_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(member_remove_parser, "dac_id", required=True, help="DAC ID.")
    member_remove_parser.set_defaults(_parser=member_remove_parser)
    member_remove_parser.set_defaults(handler=_handle_repo_dac_member_remove)

    file_parser = repo_subparsers.add_parser("file", help="Repository file commands.")
    file_subparsers = file_parser.add_subparsers(dest="file_command")
    file_parser.set_defaults(_parser=file_parser)

    file_list_parser = file_subparsers.add_parser("list-by-dataset", help="List files under a repository data set.")
    add_output_arguments(file_list_parser)
    add_argument(file_list_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(file_list_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(file_list_parser, "data_set_id", required=True, help="Repository data set ID.")
    add_argument(file_list_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(file_list_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(file_list_parser, "order_by", required=False, default=None, help="Sort order, e.g. id:asc.")
    file_list_parser.set_defaults(_parser=file_list_parser)
    file_list_parser.set_defaults(handler=_handle_repo_dataset_files)

    file_ids_parser = file_subparsers.add_parser("ids", help="List file IDs under a repository data set.")
    add_output_arguments(file_ids_parser)
    add_argument(file_ids_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(file_ids_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(file_ids_parser, "data_set_id", required=True, help="Repository data set ID.")
    file_ids_parser.set_defaults(_parser=file_ids_parser)
    file_ids_parser.set_defaults(handler=_handle_repo_dataset_file_ids)

    application_parser = repo_subparsers.add_parser("application", help="Repository application commands.")
    application_subparsers = application_parser.add_subparsers(dest="application_command")
    application_parser.set_defaults(_parser=application_parser)

    application_list_parser = application_subparsers.add_parser("list", help="List repository applications.")
    add_output_arguments(application_list_parser)
    add_argument(application_list_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(application_list_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(application_list_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(application_list_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(application_list_parser, "app_type", required=False, default=None, help="Application type.")
    add_argument(application_list_parser, "field", required=False, default=None, help="Field filter.")
    add_bool_argument(application_list_parser, "show_pending_approval", default=False, help_text="Include pending approvals.")
    application_list_parser.set_defaults(_parser=application_list_parser)
    application_list_parser.set_defaults(handler=_handle_repo_application_list)

    library_parser = repo_subparsers.add_parser("library", help="Repository data library commands.")
    library_subparsers = library_parser.add_subparsers(dest="library_command")
    library_parser.set_defaults(_parser=library_parser)

    library_list_parser = library_subparsers.add_parser("list", help="List repository data libraries.")
    add_output_arguments(library_list_parser)
    add_argument(library_list_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(library_list_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    library_list_parser.set_defaults(_parser=library_list_parser)
    library_list_parser.set_defaults(handler=_handle_repo_library_list)

    schema_job_parser = repo_subparsers.add_parser("schema-job", help="Repository schema job commands.")
    schema_job_subparsers = schema_job_parser.add_subparsers(dest="schema_job_command")
    schema_job_parser.set_defaults(_parser=schema_job_parser)

    schema_job_list_parser = schema_job_subparsers.add_parser("list", help="List repository schema jobs.")
    add_output_arguments(schema_job_list_parser)
    add_argument(schema_job_list_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(schema_job_list_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(schema_job_list_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(schema_job_list_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(schema_job_list_parser, "order_by", required=False, default="startTime:desc", help="Sort order.")
    add_argument(schema_job_list_parser, "scope", required=False, action="append", help="Scope filter. Can be repeated.")
    add_argument(schema_job_list_parser, "job_type", required=False, default=None, help="Optional job type filter.")
    schema_job_list_parser.set_defaults(_parser=schema_job_list_parser)
    schema_job_list_parser.set_defaults(handler=_handle_repo_schema_job_list)

    schema_job_export_parser = schema_job_subparsers.add_parser("export", help="List repository export schema jobs.")
    add_output_arguments(schema_job_export_parser)
    add_argument(schema_job_export_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(schema_job_export_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(schema_job_export_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(schema_job_export_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(schema_job_export_parser, "order_by", required=False, default="startTime:desc", help="Sort order.")
    add_argument(schema_job_export_parser, "scope", required=False, action="append", help="Scope filter. Can be repeated.")
    schema_job_export_parser.set_defaults(_parser=schema_job_export_parser)
    schema_job_export_parser.set_defaults(handler=_handle_repo_schema_job_export)

    schema_job_import_parser = schema_job_subparsers.add_parser("import", help="List repository import schema jobs.")
    add_output_arguments(schema_job_import_parser)
    add_argument(schema_job_import_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(schema_job_import_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(schema_job_import_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(schema_job_import_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(schema_job_import_parser, "order_by", required=False, default="startTime:desc", help="Sort order.")
    add_argument(schema_job_import_parser, "scope", required=False, action="append", help="Scope filter. Can be repeated.")
    schema_job_import_parser.set_defaults(_parser=schema_job_import_parser)
    schema_job_import_parser.set_defaults(handler=_handle_repo_schema_job_import)

    data_site_parser = repo_subparsers.add_parser("data-site", help="Repository data site helper commands.")
    data_site_subparsers = data_site_parser.add_subparsers(dest="data_site_command")
    data_site_parser.set_defaults(_parser=data_site_parser)

    pre_signed_url_parser = data_site_subparsers.add_parser("pre-signed-url", help="Get repository data site pre-signed URL.")
    add_output_arguments(pre_signed_url_parser)
    add_argument(pre_signed_url_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(pre_signed_url_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(pre_signed_url_parser, "filename", required=True, help="Repository config filename, e.g. repository-config/footer.json.")
    pre_signed_url_parser.set_defaults(_parser=pre_signed_url_parser)
    pre_signed_url_parser.set_defaults(handler=_handle_repo_data_site_pre_signed_url)

    pylons_parser = repo_subparsers.add_parser("pylons", help="Repository pylons helper commands.")
    pylons_subparsers = pylons_parser.add_subparsers(dest="pylons_command")
    pylons_parser.set_defaults(_parser=pylons_parser)

    admins_parser = pylons_subparsers.add_parser("admins", help="List pylons admins.")
    add_output_arguments(admins_parser)
    add_argument(admins_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(admins_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    admins_parser.set_defaults(_parser=admins_parser)
    admins_parser.set_defaults(handler=_handle_repo_pylons_admins)

    org_names_parser = pylons_subparsers.add_parser("organization-names", help="Resolve organization names by ID.")
    add_output_arguments(org_names_parser)
    add_argument(org_names_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(org_names_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(org_names_parser, "id", required=True, help="Organization ID.")
    org_names_parser.set_defaults(_parser=org_names_parser)
    org_names_parser.set_defaults(handler=_handle_repo_pylons_organization_names)

    identity_parser = pylons_subparsers.add_parser("identity", help="Resolve identity by ID.")
    add_output_arguments(identity_parser)
    add_argument(identity_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(identity_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(identity_parser, "id", required=True, help="Identity ID.")
    identity_parser.set_defaults(_parser=identity_parser)
    identity_parser.set_defaults(handler=_handle_repo_pylons_identity)


def _add_datasite_group(subparsers: Any) -> None:
    datasite_parser = subparsers.add_parser("datasite", help="Data site commands.")
    datasite_subparsers = datasite_parser.add_subparsers(dest="datasite_command")
    datasite_parser.set_defaults(_parser=datasite_parser)

    application_parser = datasite_subparsers.add_parser("application", help="Data site application commands.")
    application_subparsers = application_parser.add_subparsers(dest="application_command")
    application_parser.set_defaults(_parser=application_parser)

    application_list_parser = application_subparsers.add_parser("list", help="List applications.")
    add_output_arguments(application_list_parser)
    add_argument(application_list_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(application_list_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(application_list_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(application_list_parser, "size", required=False, type=int, default=None, help="Page size.")
    application_list_parser.set_defaults(_parser=application_list_parser)
    application_list_parser.set_defaults(handler=_handle_datasite_application_list)

    application_permit_parser = application_subparsers.add_parser("permit", help="Permit an application task.")
    add_output_arguments(application_permit_parser)
    add_argument(application_permit_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(application_permit_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(application_permit_parser, "id", required=True, help="Application ID.")
    add_argument(application_permit_parser, "task_id", required=True, help="Task ID.")
    application_permit_parser.set_defaults(_parser=application_permit_parser)
    application_permit_parser.set_defaults(handler=_handle_datasite_application_permit)

    application_reject_parser = application_subparsers.add_parser("reject", help="Reject an application task.")
    add_output_arguments(application_reject_parser)
    add_argument(application_reject_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(application_reject_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(application_reject_parser, "id", required=True, help="Application ID.")
    add_argument(application_reject_parser, "task_id", required=True, help="Task ID.")
    application_reject_parser.set_defaults(_parser=application_reject_parser)
    application_reject_parser.set_defaults(handler=_handle_datasite_application_reject)

    dataset_parser = datasite_subparsers.add_parser("dataset", help="Data site data set commands.")
    dataset_subparsers = dataset_parser.add_subparsers(dest="dataset_command")
    dataset_parser.set_defaults(_parser=dataset_parser)

    dataset_list_parser = dataset_subparsers.add_parser("list", help="List data site data sets.")
    add_output_arguments(dataset_list_parser)
    add_argument(dataset_list_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_list_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_list_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(dataset_list_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(dataset_list_parser, "tab", required=False, default=None, help="Optional tab filter.")
    add_argument(dataset_list_parser, "display_level", required=False, default="Minimal", help="Display level, e.g. Minimal.")
    add_argument(dataset_list_parser, "order_by", required=False, default="createTime:desc,id:desc", help="Sort order.")
    dataset_list_parser.set_defaults(_parser=dataset_list_parser)
    dataset_list_parser.set_defaults(handler=_handle_datasite_dataset_list)

    dataset_get_parser = dataset_subparsers.add_parser("get", help="Get a data site data set.")
    add_output_arguments(dataset_get_parser)
    add_argument(dataset_get_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_get_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_get_parser, "id", required=True, help="Data set ID.")
    dataset_get_parser.set_defaults(_parser=dataset_get_parser)
    dataset_get_parser.set_defaults(handler=_handle_datasite_dataset_get)

    dataset_template_parser = dataset_subparsers.add_parser("template", help="Print JSON templates for the data site data set workflow.")
    add_output_arguments(dataset_template_parser)
    add_argument(
        dataset_template_parser,
        "kind",
        required=False,
        default="create",
        help="Template kind: create, apply, upsert-files, release, update-config, delete-files.",
    )
    dataset_template_parser.set_defaults(_parser=dataset_template_parser)
    dataset_template_parser.set_defaults(handler=_handle_datasite_dataset_template)

    dataset_create_parser = dataset_subparsers.add_parser("create", help="Create a data site data set from JSON payload.")
    add_output_arguments(dataset_create_parser)
    add_json_input_arguments(dataset_create_parser)
    add_argument(dataset_create_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_create_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    dataset_create_parser.set_defaults(_parser=dataset_create_parser)
    dataset_create_parser.set_defaults(handler=_handle_datasite_dataset_create)

    dataset_apply_parser = dataset_subparsers.add_parser("apply", help="Submit a data site data set application.")
    add_output_arguments(dataset_apply_parser)
    add_json_input_arguments(dataset_apply_parser)
    add_argument(dataset_apply_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_apply_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    dataset_apply_parser.set_defaults(_parser=dataset_apply_parser)
    dataset_apply_parser.set_defaults(handler=_handle_datasite_dataset_apply)

    dataset_update_parser = dataset_subparsers.add_parser("update", help="Update a data site data set from JSON payload.")
    add_output_arguments(dataset_update_parser)
    add_json_input_arguments(dataset_update_parser)
    add_argument(dataset_update_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_update_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_update_parser, "id", required=True, help="Data set ID.")
    dataset_update_parser.set_defaults(_parser=dataset_update_parser)
    dataset_update_parser.set_defaults(handler=_handle_datasite_dataset_update)

    dataset_config_parser = dataset_subparsers.add_parser("update-config", help="Patch data site data set config from JSON payload.")
    add_output_arguments(dataset_config_parser)
    add_json_input_arguments(dataset_config_parser)
    add_argument(dataset_config_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_config_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_config_parser, "id", required=True, help="Data set ID.")
    dataset_config_parser.set_defaults(_parser=dataset_config_parser)
    dataset_config_parser.set_defaults(handler=_handle_datasite_dataset_update_config)

    dataset_delete_parser = dataset_subparsers.add_parser("delete", help="Delete a data site data set.")
    add_output_arguments(dataset_delete_parser)
    add_argument(dataset_delete_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_delete_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_delete_parser, "id", required=True, help="Data set ID.")
    dataset_delete_parser.set_defaults(_parser=dataset_delete_parser)
    dataset_delete_parser.set_defaults(handler=_handle_datasite_dataset_delete)

    dataset_permission_parser = dataset_subparsers.add_parser("get-permission", help="Get data site data set permission.")
    add_output_arguments(dataset_permission_parser)
    add_argument(dataset_permission_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_permission_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_permission_parser, "id", required=True, help="Data set ID.")
    dataset_permission_parser.set_defaults(_parser=dataset_permission_parser)
    dataset_permission_parser.set_defaults(handler=_handle_datasite_dataset_permission)

    dataset_release_parser = dataset_subparsers.add_parser("release", help="Release a data site data set.")
    add_output_arguments(dataset_release_parser)
    add_json_input_arguments(dataset_release_parser)
    add_argument(dataset_release_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_release_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_release_parser, "id", required=True, help="Data set ID.")
    dataset_release_parser.set_defaults(_parser=dataset_release_parser)
    dataset_release_parser.set_defaults(handler=_handle_datasite_dataset_release)

    dataset_revoke_parser = dataset_subparsers.add_parser("revoke", help="Revoke a data site data set.")
    add_output_arguments(dataset_revoke_parser)
    add_json_input_arguments(dataset_revoke_parser)
    add_argument(dataset_revoke_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_revoke_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_revoke_parser, "id", required=True, help="Data set ID.")
    dataset_revoke_parser.set_defaults(_parser=dataset_revoke_parser)
    dataset_revoke_parser.set_defaults(handler=_handle_datasite_dataset_revoke)

    dataset_check_parser = dataset_subparsers.add_parser("check-create", help="Check data site data set creation params.")
    add_output_arguments(dataset_check_parser)
    add_argument(dataset_check_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_check_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_check_parser, "name", required=False, default=None, help="Optional data set name.")
    dataset_check_parser.set_defaults(_parser=dataset_check_parser)
    dataset_check_parser.set_defaults(handler=_handle_datasite_dataset_check)

    dataset_archive_parser = dataset_subparsers.add_parser("archive-access", help="Get data site archive TOS access info.")
    add_output_arguments(dataset_archive_parser)
    add_argument(dataset_archive_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_archive_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_archive_parser, "path", required=False, default=None, help="Optional archive path.")
    dataset_archive_parser.set_defaults(_parser=dataset_archive_parser)
    dataset_archive_parser.set_defaults(handler=_handle_datasite_dataset_archive_access)

    dataset_export_parser = dataset_subparsers.add_parser("export", help="Start or request a data site data set export.")
    add_output_arguments(dataset_export_parser)
    add_json_input_arguments(dataset_export_parser)
    add_argument(dataset_export_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_export_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_export_parser, "data_set_id", required=True, help="Data set ID.")
    dataset_export_parser.set_defaults(_parser=dataset_export_parser)
    dataset_export_parser.set_defaults(handler=_handle_datasite_dataset_export)

    dataset_import_parser = dataset_subparsers.add_parser("import", help="Import a data site data set from JSON payload.")
    add_output_arguments(dataset_import_parser)
    add_json_input_arguments(dataset_import_parser)
    add_argument(dataset_import_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_import_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    dataset_import_parser.set_defaults(_parser=dataset_import_parser)
    dataset_import_parser.set_defaults(handler=_handle_datasite_dataset_import)

    dataset_files_parser = dataset_subparsers.add_parser("files", help="List files under a data site data set.")
    add_output_arguments(dataset_files_parser)
    add_argument(dataset_files_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_files_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_files_parser, "data_set_id", required=True, help="Data set ID.")
    add_argument(dataset_files_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(dataset_files_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(dataset_files_parser, "order_by", required=False, default=None, help="Sort order, e.g. id:asc.")
    dataset_files_parser.set_defaults(_parser=dataset_files_parser)
    dataset_files_parser.set_defaults(handler=_handle_datasite_dataset_files)

    dataset_download_files_parser = dataset_subparsers.add_parser(
        "download-files",
        help="Filter files in a data site data set, resolve DRS URLs, and download matching files.",
    )
    add_output_arguments(dataset_download_files_parser)
    add_argument(dataset_download_files_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_download_files_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_download_files_parser, "data_set_id", required=True, help="Data set ID.")
    add_argument(dataset_download_files_parser, "target", required=True, help="Local download directory.")
    add_argument(dataset_download_files_parser, "page", required=False, type=int, default=1, help="Page number.")
    add_argument(dataset_download_files_parser, "size", required=False, type=int, default=100, help="Page size.")
    add_argument(dataset_download_files_parser, "order_by", required=False, default="id:asc", help="Sort order, e.g. id:asc.")
    add_argument(dataset_download_files_parser, "name_contains", required=False, default=None, help="Case-insensitive substring filter on file name.")
    add_argument(dataset_download_files_parser, "regex", required=False, default=None, help="Regular expression filter on file name.")
    add_argument(dataset_download_files_parser, "drs_url", required=False, default=None, help="Optional exact DRS URL filter.")
    add_argument(dataset_download_files_parser, "limit", required=False, type=int, default=None, help="Maximum number of matched files to download.")
    add_bool_argument(
        dataset_download_files_parser,
        "dry_run",
        default=False,
        help_text="Preview matched files without downloading them.",
    )
    dataset_download_files_parser.set_defaults(_parser=dataset_download_files_parser)
    dataset_download_files_parser.set_defaults(handler=_handle_datasite_dataset_download_files)

    dataset_file_ids_parser = dataset_subparsers.add_parser("file-ids", help="List file IDs under a data site data set.")
    add_output_arguments(dataset_file_ids_parser)
    add_argument(dataset_file_ids_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_file_ids_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_file_ids_parser, "data_set_id", required=True, help="Data set ID.")
    dataset_file_ids_parser.set_defaults(_parser=dataset_file_ids_parser)
    dataset_file_ids_parser.set_defaults(handler=_handle_datasite_dataset_file_ids)

    dataset_file_upsert_parser = dataset_subparsers.add_parser("upsert-files", help="Upsert files under a data site data set from JSON payload.")
    add_output_arguments(dataset_file_upsert_parser)
    add_json_input_arguments(dataset_file_upsert_parser)
    add_argument(dataset_file_upsert_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_file_upsert_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_file_upsert_parser, "data_set_id", required=True, help="Data set ID.")
    dataset_file_upsert_parser.set_defaults(_parser=dataset_file_upsert_parser)
    dataset_file_upsert_parser.set_defaults(handler=_handle_datasite_dataset_files_upsert)

    dataset_file_delete_parser = dataset_subparsers.add_parser("delete-files", help="Delete files under a data site data set using JSON payload.")
    add_output_arguments(dataset_file_delete_parser)
    add_json_input_arguments(dataset_file_delete_parser)
    add_argument(dataset_file_delete_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(dataset_file_delete_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(dataset_file_delete_parser, "data_set_id", required=True, help="Data set ID.")
    dataset_file_delete_parser.set_defaults(_parser=dataset_file_delete_parser)
    dataset_file_delete_parser.set_defaults(handler=_handle_datasite_dataset_files_delete)

    file_parser = datasite_subparsers.add_parser("file", help="Data site file commands.")
    file_subparsers = file_parser.add_subparsers(dest="file_command")
    file_parser.set_defaults(_parser=file_parser)

    file_list_parser = file_subparsers.add_parser("list", help="List data site files.")
    add_output_arguments(file_list_parser)
    add_argument(file_list_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(file_list_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(file_list_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(file_list_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(file_list_parser, "order_by", required=False, default=None, help="Sort order, e.g. id:asc.")
    file_list_parser.set_defaults(_parser=file_list_parser)
    file_list_parser.set_defaults(handler=_handle_datasite_file_list)

    file_types_parser = file_subparsers.add_parser("types", help="List data file types.")
    add_output_arguments(file_types_parser)
    add_argument(file_types_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(file_types_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    file_types_parser.set_defaults(_parser=file_types_parser)
    file_types_parser.set_defaults(handler=_handle_datasite_file_types)

    file_drs_parser = file_subparsers.add_parser("list-drs", help="List DRS URLs for files in a data set.")
    add_output_arguments(file_drs_parser)
    add_argument(file_drs_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(file_drs_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(file_drs_parser, "data_set_id", required=True, help="Data set ID.")
    add_argument(file_drs_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(file_drs_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(file_drs_parser, "order_by", required=False, default=None, help="Sort order, e.g. id:asc.")
    file_drs_parser.set_defaults(_parser=file_drs_parser)
    file_drs_parser.set_defaults(handler=_handle_datasite_file_list_drs)

    schema_job_parser = datasite_subparsers.add_parser("schema-job", help="Data site schema job commands.")
    schema_job_subparsers = schema_job_parser.add_subparsers(dest="schema_job_command")
    schema_job_parser.set_defaults(_parser=schema_job_parser)

    schema_job_list_parser = schema_job_subparsers.add_parser("list", help="List schema jobs.")
    add_output_arguments(schema_job_list_parser)
    add_argument(schema_job_list_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(schema_job_list_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(schema_job_list_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(schema_job_list_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(schema_job_list_parser, "order_by", required=False, default="startTime:desc", help="Sort order.")
    add_argument(schema_job_list_parser, "scope", required=False, action="append", help="Scope filter. Can be repeated.")
    add_argument(schema_job_list_parser, "job_type", required=False, default=None, help="Optional job type filter.")
    schema_job_list_parser.set_defaults(_parser=schema_job_list_parser)
    schema_job_list_parser.set_defaults(handler=_handle_datasite_schema_job_list)

    schema_job_export_parser = schema_job_subparsers.add_parser("export", help="List export schema jobs.")
    add_output_arguments(schema_job_export_parser)
    add_argument(schema_job_export_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(schema_job_export_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(schema_job_export_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(schema_job_export_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(schema_job_export_parser, "order_by", required=False, default="startTime:desc", help="Sort order.")
    add_argument(schema_job_export_parser, "scope", required=False, action="append", help="Scope filter. Can be repeated.")
    schema_job_export_parser.set_defaults(_parser=schema_job_export_parser)
    schema_job_export_parser.set_defaults(handler=_handle_datasite_schema_job_export)

    schema_job_import_parser = schema_job_subparsers.add_parser("import", help="List import schema jobs.")
    add_output_arguments(schema_job_import_parser)
    add_argument(schema_job_import_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(schema_job_import_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(schema_job_import_parser, "page", required=False, type=int, default=None, help="Page number.")
    add_argument(schema_job_import_parser, "size", required=False, type=int, default=None, help="Page size.")
    add_argument(schema_job_import_parser, "order_by", required=False, default="startTime:desc", help="Sort order.")
    add_argument(schema_job_import_parser, "scope", required=False, action="append", help="Scope filter. Can be repeated.")
    schema_job_import_parser.set_defaults(_parser=schema_job_import_parser)
    schema_job_import_parser.set_defaults(handler=_handle_datasite_schema_job_import)

    schema_job_delete_parser = schema_job_subparsers.add_parser("delete", help="Delete a schema job.")
    add_output_arguments(schema_job_delete_parser)
    add_argument(schema_job_delete_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(schema_job_delete_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(schema_job_delete_parser, "id", required=True, help="Schema job ID.")
    schema_job_delete_parser.set_defaults(_parser=schema_job_delete_parser)
    schema_job_delete_parser.set_defaults(handler=_handle_datasite_schema_job_delete)

    drs_parser = datasite_subparsers.add_parser("drs", help="Data site DRS commands.")
    drs_subparsers = drs_parser.add_subparsers(dest="drs_command")
    drs_parser.set_defaults(_parser=drs_parser)

    drs_object_parser = drs_subparsers.add_parser("object", help="Get a DRS object.")
    add_output_arguments(drs_object_parser)
    add_argument(drs_object_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(drs_object_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(drs_object_parser, "object_id", required=True, help="DRS object ID.")
    drs_object_parser.set_defaults(_parser=drs_object_parser)
    drs_object_parser.set_defaults(handler=_handle_datasite_drs_object)

    drs_object_auth_parser = drs_subparsers.add_parser("object-auth", help="Post to a DRS object endpoint with JSON payload.")
    add_output_arguments(drs_object_auth_parser)
    add_json_input_arguments(drs_object_auth_parser)
    add_argument(drs_object_auth_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(drs_object_auth_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(drs_object_auth_parser, "object_id", required=True, help="DRS object ID.")
    drs_object_auth_parser.set_defaults(_parser=drs_object_auth_parser)
    drs_object_auth_parser.set_defaults(handler=_handle_datasite_drs_object_auth)

    drs_access_parser = drs_subparsers.add_parser("access", help="Get DRS access for an object.")
    add_output_arguments(drs_access_parser)
    add_argument(drs_access_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(drs_access_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(drs_access_parser, "object_id", required=True, help="DRS object ID.")
    add_argument(drs_access_parser, "access_id", required=True, help="DRS access ID.")
    drs_access_parser.set_defaults(_parser=drs_access_parser)
    drs_access_parser.set_defaults(handler=_handle_datasite_drs_access)

    drs_access_auth_parser = drs_subparsers.add_parser("access-auth", help="Post to a DRS access endpoint with JSON payload.")
    add_output_arguments(drs_access_auth_parser)
    add_json_input_arguments(drs_access_auth_parser)
    add_argument(drs_access_auth_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(drs_access_auth_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(drs_access_auth_parser, "object_id", required=True, help="DRS object ID.")
    add_argument(drs_access_auth_parser, "access_id", required=True, help="DRS access ID.")
    drs_access_auth_parser.set_defaults(_parser=drs_access_auth_parser)
    drs_access_auth_parser.set_defaults(handler=_handle_datasite_drs_access_auth)

    drs_resolve_parser = drs_subparsers.add_parser("resolve", help="Resolve a DRS URL into object metadata and access details.")
    add_output_arguments(drs_resolve_parser)
    add_argument(drs_resolve_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(drs_resolve_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(drs_resolve_parser, "drs_url", required=True, help="DRS URL, for example drs://host/object-id.")
    add_argument(drs_resolve_parser, "access_id", required=False, default=None, help="Optional access ID override.")
    drs_resolve_parser.set_defaults(_parser=drs_resolve_parser)
    drs_resolve_parser.set_defaults(handler=_handle_datasite_drs_resolve)

    drs_download_parser = drs_subparsers.add_parser("download", help="Download a file from a DRS URL to a local path.")
    add_output_arguments(drs_download_parser)
    add_argument(drs_download_parser, "token", required=False, default=None, help="AAI token override.")
    add_argument(drs_download_parser, "cookie", required=False, default=None, help="AAI session cookie override.")
    add_argument(drs_download_parser, "drs_url", required=True, help="DRS URL, for example drs://host/object-id.")
    add_argument(drs_download_parser, "target", required=True, help="Local file path or target directory.")
    add_argument(drs_download_parser, "access_id", required=False, default=None, help="Optional access ID override.")
    drs_download_parser.set_defaults(_parser=drs_download_parser)
    drs_download_parser.set_defaults(handler=_handle_datasite_drs_download)


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


def _save_client_auth(
    *,
    access_key: Optional[str],
    secret_key: Optional[str],
    endpoint: Optional[str],
    region: Optional[str],
    config_path: Optional[str],
) -> list[dict]:
    from bioos.cli.config_store import update_section_values

    values = {}
    if access_key:
        values["MIRACLE_ACCESS_KEY"] = access_key
    if secret_key:
        values["MIRACLE_SECRET_KEY"] = secret_key
    if endpoint:
        values["serveraddr"] = endpoint
    if region:
        values["region"] = region
    if not values:
        return []

    saved_path = update_section_values("client", values, path=config_path)
    return [
        {
            "section": "client",
            "config_path": str(saved_path),
            "fields": sorted(values.keys()),
        }
    ]


def _handle_auth_connect_aai(args: argparse.Namespace) -> dict:
    saved = []
    if getattr(args, "save_client", True):
        settings = resolve_auth_settings(
            access_key=getattr(args, "ak", None),
            secret_key=getattr(args, "sk", None),
            endpoint=getattr(args, "endpoint", None),
            region=getattr(args, "region", None),
        )
        saved.extend(
            _save_client_auth(
                access_key=settings.get("access_key"),
                secret_key=settings.get("secret_key"),
                endpoint=settings.get("endpoint"),
                region=settings.get("region"),
                config_path=getattr(args, "config_path", None),
            )
        )

    auth_status = _handle_auth_status(args)
    if not auth_status.get("success"):
        result = {
            "saved": saved,
            "auth": auth_status,
            "synced": False,
        }
        return result

    aai_args = argparse.Namespace(
        account_name=getattr(args, "account_name", None),
        password=getattr(args, "password", None),
        user_name=getattr(args, "user_name", None),
        web_url=getattr(args, "web_url", None),
        login_token=getattr(args, "login_token", None),
        csrf_token=getattr(args, "csrf_token", None),
        cookie=getattr(args, "cookie", None),
        curl=getattr(args, "curl", None),
        curl_file=getattr(args, "curl_file", None),
        save_main_web=getattr(args, "save_main_web", True),
        sync_passport=getattr(args, "sync_passport", True),
        expires_in=getattr(args, "expires_in", None),
        config_path=getattr(args, "config_path", None),
    )
    aai_result = _handle_aai_login(aai_args)

    return {
        "saved": saved + (aai_result.get("saved") or []),
        "auth": auth_status,
        "aai": aai_result,
        "synced": bool(aai_result.get("synced")),
    }


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


def _handle_aai_auth(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import resolve_aai_with_args

    settings = resolve_aai_with_args(args, service=getattr(args, "service", "repo"))
    return {
        "service": settings["service"],
        "configured": bool(settings.get("url")),
        "authenticated": bool(settings.get("token") or settings.get("cookie")),
        "url_source": settings["url_source"],
        "url": settings["url"],
        "token_source": settings["token_source"],
        "token": _mask_value(settings["token"]),
        "cookie_source": settings["cookie_source"],
        "cookie_configured": bool(settings.get("cookie")),
        "passport_issued_at": settings.get("passport_issued_at"),
        "passport_expires_at": settings.get("passport_expires_at"),
        "passport_status": settings.get("passport_status"),
        "config_path": settings["config_path"],
    }


def _handle_aai_status(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import resolve_aai_settings, resolve_main_web_settings

    main_web = resolve_main_web_settings(
        login_token=getattr(args, "login_token", None),
        csrf_token=getattr(args, "csrf_token", None),
        cookie=getattr(args, "cookie", None),
        url=getattr(args, "web_url", None),
    )
    repo = resolve_aai_settings(service="repo")
    datasite = resolve_aai_settings(service="datasite")
    return {
        "main_web": {
            "url": main_web["url"],
            "url_source": main_web["url_source"],
            "login_token_source": main_web["login_token_source"],
            "login_token_configured": bool(main_web.get("login_token")),
            "csrf_token_source": main_web["csrf_token_source"],
            "csrf_token_configured": bool(main_web.get("csrf_token")),
            "cookie_source": main_web["cookie_source"],
            "cookie_configured": bool(main_web.get("cookie")),
        },
        "repo": {
            "url": repo["url"],
            "url_source": repo["url_source"],
            "token_source": repo["token_source"],
            "token_configured": bool(repo.get("token")),
            "cookie_source": repo["cookie_source"],
            "cookie_configured": bool(repo.get("cookie")),
            "passport_issued_at": repo.get("passport_issued_at"),
            "passport_expires_at": repo.get("passport_expires_at"),
            "passport_status": repo.get("passport_status"),
        },
        "datasite": {
            "url": datasite["url"],
            "url_source": datasite["url_source"],
            "token_source": datasite["token_source"],
            "token_configured": bool(datasite.get("token")),
            "cookie_source": datasite["cookie_source"],
            "cookie_configured": bool(datasite.get("cookie")),
            "passport_issued_at": datasite.get("passport_issued_at"),
            "passport_expires_at": datasite.get("passport_expires_at"),
            "passport_status": datasite.get("passport_status"),
        },
    }


def _handle_aai_import_main_web_curl(args: argparse.Namespace) -> dict:
    curl_text = getattr(args, "curl", None)
    curl_file = getattr(args, "curl_file", None)
    if bool(curl_text) == bool(curl_file):
        raise ValueError("Use exactly one of --curl or --curl-file.")
    if curl_file:
        from pathlib import Path

        curl_text = Path(curl_file).read_text(encoding="utf-8")
    if not curl_text:
        raise ValueError("Missing cURL input.")

    url_match = re.search(r"curl\s+'([^']+)'", curl_text)
    login_token_match = re.search(r"-H\s+'x-logintoken:\s*([^']+)'", curl_text, flags=re.IGNORECASE)
    csrf_match = re.search(r"-H\s+'x-csrf-token:\s*([^']+)'", curl_text, flags=re.IGNORECASE)
    cookie_match = re.search(r"-b\s+'([^']+)'", curl_text)

    web_url = None
    if url_match:
        url_value = url_match.group(1)
        parts = url_value.split("/", 3)
        if len(parts) >= 3:
            web_url = f"{parts[0]}//{parts[2]}"

    return {
        "web_url": web_url,
        "login_token": login_token_match.group(1).strip() if login_token_match else None,
        "login_token_masked": _mask_value(login_token_match.group(1).strip()) if login_token_match else None,
        "csrf_token": csrf_match.group(1).strip() if csrf_match else None,
        "csrf_token_masked": _mask_value(csrf_match.group(1).strip()) if csrf_match else None,
        "cookie": cookie_match.group(1).strip() if cookie_match else None,
        "cookie_configured": bool(cookie_match),
    }


def _extract_main_web_auth_from_args(args: argparse.Namespace) -> dict:
    import_args = argparse.Namespace(
        curl=getattr(args, "curl", None),
        curl_file=getattr(args, "curl_file", None),
    )
    return _handle_aai_import_main_web_curl(import_args)


def _resolve_main_web_auth_inputs(args: argparse.Namespace) -> dict:
    has_password_auth = bool(getattr(args, "account_name", None) and getattr(args, "password", None))
    has_direct_auth = any(
        getattr(args, name, None)
        for name in ("login_token", "csrf_token", "cookie")
    )
    has_web_url_override = bool(getattr(args, "web_url", None))
    has_curl_auth = bool(getattr(args, "curl", None) or getattr(args, "curl_file", None))

    selected_modes = sum(bool(flag) for flag in (has_password_auth, has_direct_auth, has_curl_auth))
    if selected_modes > 1:
        raise ValueError("Use only one main-web auth source: password login, direct auth flags, or --curl/--curl-file.")
    if has_curl_auth and has_web_url_override:
        raise ValueError("Do not combine --web-url with --curl/--curl-file. The web URL is derived from the cURL input.")

    if has_password_auth:
        from bioos.ops.auth import login_to_main_web

        auth = login_to_main_web(
            account_name=getattr(args, "account_name"),
            password=getattr(args, "password"),
            user_name=getattr(args, "user_name", None),
            url=getattr(args, "web_url", None),
        )
        return {
            "web_url": auth.get("web_url"),
            "login_token": auth.get("login_token"),
            "csrf_token": auth.get("csrf_token"),
            "cookie": auth.get("cookie"),
            "source": "password",
            "extracted": {
                "web_url": auth.get("web_url"),
                "login_token_masked": _mask_value(auth.get("login_token")),
                "csrf_token_masked": _mask_value(auth.get("csrf_token")),
                "cookie_configured": bool(auth.get("cookie")),
                "login_result": {
                    "account_name": auth.get("login_result", {}).get("AccountName") if isinstance(auth.get("login_result"), dict) else None,
                    "email": auth.get("login_result", {}).get("Email") if isinstance(auth.get("login_result"), dict) else None,
                    "role": auth.get("login_result", {}).get("Role") if isinstance(auth.get("login_result"), dict) else None,
                },
            },
        }

    if has_curl_auth:
        extracted = _extract_main_web_auth_from_args(args)
        return {
            "web_url": extracted.get("web_url"),
            "login_token": extracted.get("login_token"),
            "csrf_token": extracted.get("csrf_token"),
            "cookie": extracted.get("cookie"),
            "source": "curl",
            "extracted": {
                "web_url": extracted.get("web_url"),
                "login_token_masked": extracted.get("login_token_masked"),
                "csrf_token_masked": extracted.get("csrf_token_masked"),
                "cookie_configured": extracted.get("cookie_configured"),
            },
        }

    return {
        "web_url": getattr(args, "web_url", None),
        "login_token": getattr(args, "login_token", None),
        "csrf_token": getattr(args, "csrf_token", None),
        "cookie": getattr(args, "cookie", None),
        "source": "direct",
        "extracted": None,
    }


def _save_main_web_auth(
    *,
    web_url: Optional[str],
    login_token: Optional[str],
    csrf_token: Optional[str],
    cookie: Optional[str],
    config_path: Optional[str],
) -> list[dict]:
    from bioos.cli.config_store import update_section_values

    values = {}
    if web_url:
        values["url"] = web_url
    if login_token:
        values["login_token"] = login_token
    if csrf_token:
        values["csrf_token"] = csrf_token
    if cookie:
        values["cookie"] = cookie
    if not values:
        return []

    saved_path = update_section_values("main_web", values, path=config_path)
    return [
        {
            "section": "main_web",
            "config_path": str(saved_path),
            "fields": sorted(values.keys()),
        }
    ]


def _handle_account_link(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import resolve_account_link_settings

    return resolve_account_link_settings(path=getattr(args, "config_path", None))


def _handle_aai_account_status(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_main_web_client, resolve_main_web_settings

    settings = resolve_main_web_settings(
        login_token=getattr(args, "login_token", None),
        csrf_token=getattr(args, "csrf_token", None),
        cookie=getattr(args, "cookie", None),
        url=getattr(args, "web_url", None),
    )
    client = build_main_web_client(
        login_token=getattr(args, "login_token", None),
        csrf_token=getattr(args, "csrf_token", None),
        cookie=getattr(args, "cookie", None),
        url=getattr(args, "web_url", None),
    )
    result = client.check_repository_account_exist()
    return {
        "web_url": settings["url"],
        "web_url_source": settings["url_source"],
        "login_token_source": settings["login_token_source"],
        "csrf_token_source": settings["csrf_token_source"],
        "cookie_source": settings["cookie_source"],
        "linked": _is_repository_account_linked(result),
        "result": result,
    }


def _handle_aai_passport_get(args: argparse.Namespace) -> dict:
    from bioos.cli.config_store import update_section_values
    from bioos.ops.auth import build_main_web_client, resolve_main_web_settings

    settings = resolve_main_web_settings(
        login_token=getattr(args, "login_token", None),
        csrf_token=getattr(args, "csrf_token", None),
        cookie=getattr(args, "cookie", None),
        url=getattr(args, "web_url", None),
    )
    client = build_main_web_client(
        login_token=getattr(args, "login_token", None),
        csrf_token=getattr(args, "csrf_token", None),
        cookie=getattr(args, "cookie", None),
        url=getattr(args, "web_url", None),
    )
    result = client.get_repository_passport(expires_in=getattr(args, "expires_in", None))
    passport = result.get("Passport") if isinstance(result, dict) else None
    issued_at, expires_at = _compute_passport_timestamps(getattr(args, "expires_in", None))
    save_targets = getattr(args, "save_to", None) or []
    saved = []
    if passport:
        for target in save_targets:
            normalized = (target or "").strip().lower()
            if normalized not in {"repo", "datasite"}:
                raise ValueError(f"Unsupported save target: {target}. Expected repo and/or datasite.")
            config_path = update_section_values(
                normalized,
                {
                    "token": passport,
                    "passport_issued_at": issued_at,
                    "passport_expires_at": expires_at,
                },
                path=getattr(args, "config_path", None),
            )
            saved.append(
                {
                    "section": normalized,
                    "config_path": str(config_path),
                }
            )
    return {
        "web_url": settings["url"],
        "web_url_source": settings["url_source"],
        "login_token_source": settings["login_token_source"],
        "csrf_token_source": settings["csrf_token_source"],
        "cookie_source": settings["cookie_source"],
        "passport": passport,
        "passport_masked": _mask_value(passport),
        "passport_issued_at": issued_at,
        "passport_expires_at": expires_at,
        "saved": saved,
        "result": result,
    }


def _handle_aai_sync_from_bioos(args: argparse.Namespace) -> dict:
    sync_args = argparse.Namespace(
        web_url=getattr(args, "web_url", None),
        login_token=getattr(args, "login_token", None),
        csrf_token=getattr(args, "csrf_token", None),
        cookie=getattr(args, "cookie", None),
        expires_in=getattr(args, "expires_in", None),
        save_to=["repo", "datasite"],
        config_path=getattr(args, "config_path", None),
    )
    result = _handle_aai_passport_get(sync_args)
    result["synced"] = True
    return result


def _handle_aai_sync_from_curl(args: argparse.Namespace) -> dict:
    extracted = _extract_main_web_auth_from_args(args)
    sync_args = argparse.Namespace(
        web_url=extracted.get("web_url"),
        login_token=extracted.get("login_token"),
        csrf_token=extracted.get("csrf_token"),
        cookie=extracted.get("cookie"),
        expires_in=getattr(args, "expires_in", None),
        config_path=getattr(args, "config_path", None),
    )
    result = _handle_aai_sync_from_bioos(sync_args)
    result["extracted"] = {
        "web_url": extracted.get("web_url"),
        "login_token_masked": extracted.get("login_token_masked"),
        "csrf_token_masked": extracted.get("csrf_token_masked"),
        "cookie_configured": extracted.get("cookie_configured"),
    }
    return result


def _handle_aai_login(args: argparse.Namespace) -> dict:
    resolved = _resolve_main_web_auth_inputs(args)
    save_main_web = getattr(args, "save_main_web", True)
    sync_passport = getattr(args, "sync_passport", True)

    saved = []
    if save_main_web:
        saved.extend(
            _save_main_web_auth(
                web_url=resolved.get("web_url"),
                login_token=resolved.get("login_token"),
                csrf_token=resolved.get("csrf_token"),
                cookie=resolved.get("cookie"),
                config_path=getattr(args, "config_path", None),
            )
        )

    result = {
        "mode": resolved["source"],
        "saved": saved,
        "main_web": {
            "web_url": resolved.get("web_url"),
            "login_token_configured": bool(resolved.get("login_token")),
            "csrf_token_configured": bool(resolved.get("csrf_token")),
            "cookie_configured": bool(resolved.get("cookie")),
        },
        "synced": False,
    }

    if resolved.get("extracted"):
        result["extracted"] = resolved["extracted"]

    if not sync_passport:
        return result

    link_status = _handle_aai_account_status(
        argparse.Namespace(
            web_url=resolved.get("web_url"),
            login_token=resolved.get("login_token"),
            csrf_token=resolved.get("csrf_token"),
            cookie=resolved.get("cookie"),
        )
    )
    result["account_link"] = link_status
    if not link_status.get("linked"):
        result["message"] = (
            "The current BioOS web account is not linked to an AAI repository account. "
            "Link the account first, then run 'bioos aai login' again."
        )
        return result

    sync_args = argparse.Namespace(
        web_url=resolved.get("web_url"),
        login_token=resolved.get("login_token"),
        csrf_token=resolved.get("csrf_token"),
        cookie=resolved.get("cookie"),
        expires_in=getattr(args, "expires_in", None),
        config_path=getattr(args, "config_path", None),
    )
    sync_result = _handle_aai_sync_from_bioos(sync_args)
    result.update(sync_result)
    if saved:
        passport_saved = sync_result.get("saved") or []
        result["saved"] = saved + passport_saved
    return result


def _handle_aai_refresh(args: argparse.Namespace) -> dict:
    refresh_args = argparse.Namespace(
        account_name=getattr(args, "account_name", None),
        password=getattr(args, "password", None),
        user_name=getattr(args, "user_name", None),
        web_url=getattr(args, "web_url", None),
        login_token=getattr(args, "login_token", None),
        csrf_token=getattr(args, "csrf_token", None),
        cookie=getattr(args, "cookie", None),
        curl=getattr(args, "curl", None),
        curl_file=getattr(args, "curl_file", None),
        save_main_web=getattr(args, "save_main_web", True),
        sync_passport=True,
        expires_in=getattr(args, "expires_in", None),
        config_path=getattr(args, "config_path", None),
    )
    result = _handle_aai_login(refresh_args)
    result["refreshed"] = bool(result.get("synced"))
    return result


def _handle_repo_dataset_list(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_datasets(
        page=args.page,
        size=args.size,
        display_level=args.display_level,
        order_by=args.order_by,
    )


def _handle_repo_dataset_get(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.get_dataset(args.id)


def _handle_repo_dataset_export(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.export_dataset(args.data_set_id, payload=load_json_input(args))


def _handle_repo_dataset_import(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.import_dataset(load_json_input(args))


def _handle_repo_dataset_archive_access(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.get_dataset_archive_access(path=args.path)


def _handle_repo_dataset_files(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_dataset_files(args.data_set_id, page=args.page, size=args.size, order_by=args.order_by)


def _handle_repo_dataset_file_ids(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_dataset_file_ids(args.data_set_id)


def _handle_repo_dac_list(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_dacs(page=args.page, size=args.size, limit=args.limit, scope=args.scope)


def _handle_repo_dac_create(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.create_dac(load_json_input(args))


def _handle_repo_dac_update(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.update_dac(args.id, load_json_input(args))


def _handle_repo_dac_delete(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.delete_dac(args.id)


def _handle_repo_dac_check(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.check_dac(name=args.name)


def _handle_repo_dac_member_list(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_dac_members(args.dac_id, page=args.page, size=args.size)


def _handle_repo_dac_member_upsert(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.upsert_dac_member(args.dac_id, load_json_input(args))


def _handle_repo_dac_member_remove(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.remove_dac_member(args.dac_id, load_json_input(args))


def _handle_repo_application_list(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_applications(
        page=args.page,
        size=args.size,
        app_type=args.app_type,
        field=args.field,
        show_pending_approval=args.show_pending_approval,
    )


def _handle_repo_library_list(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_libraries()


def _handle_repo_schema_job_list(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_schema_jobs(
        page=args.page,
        size=args.size,
        order_by=args.order_by,
        scope=args.scope,
        job_type=args.job_type,
    )


def _handle_repo_schema_job_export(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.get_export_schema_jobs(page=args.page, size=args.size, order_by=args.order_by, scope=args.scope)


def _handle_repo_schema_job_import(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.get_import_schema_jobs(page=args.page, size=args.size, order_by=args.order_by, scope=args.scope)


def _handle_repo_data_site_pre_signed_url(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.get_datasite_pre_signed_url(args.filename)


def _handle_repo_pylons_admins(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_admins()


def _handle_repo_pylons_organization_names(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.get_organization_names(args.id)


def _handle_repo_pylons_identity(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_repository_client

    client = build_repository_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.get_identity(args.id)


def _handle_datasite_application_list(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_applications(page=args.page, size=args.size)


def _handle_datasite_application_permit(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.permit_application(args.id, args.task_id)


def _handle_datasite_application_reject(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.reject_application(args.id, args.task_id)


def _handle_datasite_dataset_list(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_datasets(
        page=args.page,
        size=args.size,
        tab=args.tab,
        display_level=args.display_level,
        order_by=args.order_by,
    )


def _handle_datasite_dataset_get(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.get_dataset(args.id)


def _handle_datasite_dataset_template(args: argparse.Namespace) -> dict:
    kind = (getattr(args, "kind", None) or "create").strip().lower()
    templates = {
        "create": {
            "name": "mc-dataset-demo",
            "description": "Demo data set created from pybioos CLI.",
            "createTime": 1778112000,
            "updateTime": 1778112000,
            "owners": "BioOS Team",
            "category": "Genomics",
            "catalogue": "Human omics",
            "labels": ["WGS", "Cancer"],
            "docURL": "https://example.org/docs/mc-dataset-demo",
            "emails": ["owner@example.org"],
            "licence": "CC-BY-4.0",
            "projectDataTypes": ["FASTQ", "BAM"],
            "sampleScope": "Human cohort",
            "externalLink": "https://example.org/project/mc-dataset-demo",
            "externalLinkDescription": "Project home page",
            "exampleTutorial": "https://example.org/tutorials/mc-dataset-demo",
            "tools": "https://example.org/tools/mc-dataset-demo",
            "publications": [
                {
                    "name": "Example publication",
                    "accessURL": "https://doi.org/10.1000/example",
                    "authors": "Alice Zhang; Bob Li",
                    "quotation": "Zhang A, Li B. Example dataset paper. 2025.",
                }
            ],
            "dataFilesAccessURL": "https://example.org/downloads/mc-dataset-demo/files.csv",
            "dataSetAccessMethodURL": "https://example.org/downloads/mc-dataset-demo/access_methods.json",
            "dataSetTablesAccessURL": {
                "sample_sheet": "https://example.org/downloads/mc-dataset-demo/sample_sheet.tsv"
            },
        },
        "apply": {
            "type": "DataSetCreate",
            "dataSetID": "replace-with-created-data-set-id",
            "title": "Publish mc-dataset-demo to data site",
            "reason": "Initial release from pybioos workflow.",
        },
        "upsert-files": {
            "dataFiles": [
                {
                    "name": "example_01.fastq.gz",
                    "description": "Paired-end read 1",
                    "createTime": 1778112000,
                    "updateTime": 1778112000,
                    "fileType": "FASTQ",
                    "fileSize": 123456789,
                    "accessURL": "s3://bucket/path/example_01.fastq.gz",
                    "source": "s3://bucket/path/example_01.fastq.gz",
                    "checksums": [
                        {
                            "type": "md5",
                            "value": "d41d8cd98f00b204e9800998ecf8427e",
                        }
                    ],
                }
            ],
            "duplicatedReplace": True,
        },
        "release": {
            "comment": "Release approved and ready for publication."
        },
        "update-config": {
            "customHeaderOrders": {
                "sample_id": "1",
                "subject_id": "2",
                "tumor_type": "3",
            }
        },
        "delete-files": {
            "dataFileIDs": ["replace-with-file-id-1", "replace-with-file-id-2"]
        },
    }
    if kind not in templates:
        raise ValueError("Unsupported template kind. Expected create, apply, upsert-files, release, update-config, or delete-files.")
    return {
        "kind": kind,
        "template": templates[kind],
    }


def _handle_datasite_dataset_create(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.create_dataset(load_json_input(args))


def _handle_datasite_dataset_apply(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.apply_dataset(load_json_input(args))


def _handle_datasite_dataset_update(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.update_dataset(args.id, load_json_input(args))


def _handle_datasite_dataset_update_config(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.update_dataset_config(args.id, load_json_input(args))


def _handle_datasite_dataset_delete(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.delete_dataset(args.id)


def _handle_datasite_dataset_permission(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.get_dataset_permission(args.id)


def _handle_datasite_dataset_release(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.release_dataset(args.id, payload=load_json_input(args))


def _handle_datasite_dataset_revoke(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.revoke_dataset(args.id, payload=load_json_input(args))


def _handle_datasite_dataset_check(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.check_dataset(name=args.name)


def _handle_datasite_dataset_archive_access(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.get_dataset_archive_access(path=args.path)


def _handle_datasite_dataset_export(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.export_dataset(args.data_set_id, payload=load_json_input(args))


def _handle_datasite_dataset_import(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.import_dataset(load_json_input(args))


def _handle_datasite_dataset_files(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_dataset_files(args.data_set_id, page=args.page, size=args.size, order_by=args.order_by)


def _handle_datasite_dataset_download_files(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    result = client.list_dataset_files(args.data_set_id, page=args.page, size=args.size, order_by=args.order_by)
    items = result.get("items") if isinstance(result, dict) else None
    if not isinstance(items, list):
        return {
            "data_set_id": args.data_set_id,
            "matched_count": 0,
            "downloaded_count": 0,
            "items": [],
            "source": result,
        }

    matched = _filter_datasite_file_items(
        items=items,
        name_contains=getattr(args, "name_contains", None),
        regex_pattern=getattr(args, "regex", None),
        exact_drs_url=getattr(args, "drs_url", None),
        limit=getattr(args, "limit", None),
    )
    target_dir = Path(args.target).expanduser()
    downloads = []
    for item in matched:
        file_name = item.get("name")
        drs_url = item.get("drs_url")
        download_item = {
            "id": item.get("id"),
            "name": file_name,
            "drs_url": drs_url,
        }
        if not getattr(args, "dry_run", False):
            resolved = _resolve_drs_download_info(
                client=client,
                drs_url=drs_url,
                access_id=None,
            )
            access_url = resolved.get("access_url")
            if not access_url:
                raise ValueError(f"Failed to resolve downloadable access URL for DRS {drs_url}.")
            output_path = _determine_download_target(
                target=str(target_dir),
                file_name=resolved.get("file_name") or file_name,
                access_url=access_url,
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)
            urllib.request.urlretrieve(access_url, output_path)
            download_item["output_path"] = str(output_path)
            download_item["access_url"] = access_url
        downloads.append(download_item)

    return {
        "data_set_id": args.data_set_id,
        "matched_count": len(matched),
        "downloaded_count": 0 if getattr(args, "dry_run", False) else len(downloads),
        "dry_run": bool(getattr(args, "dry_run", False)),
        "target": str(target_dir),
        "items": downloads,
    }


def _handle_datasite_dataset_file_ids(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_dataset_file_ids(args.data_set_id)


def _handle_datasite_dataset_files_upsert(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.upsert_dataset_files(args.data_set_id, load_json_input(args))


def _handle_datasite_dataset_files_delete(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.delete_dataset_files(args.data_set_id, load_json_input(args))


def _handle_datasite_file_list(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_files(page=args.page, size=args.size, order_by=args.order_by)


def _handle_datasite_file_list_drs(args: argparse.Namespace) -> dict:
    result = _handle_datasite_dataset_files(args)
    items = result.get("items") if isinstance(result, dict) else None
    if not isinstance(items, list):
        return {"items": [], "source": result}
    drs_items = []
    for item in items:
        if isinstance(item, dict):
            drs_url = item.get("drsURL") or item.get("drs_url")
            if drs_url:
                drs_items.append(
                    {
                        "id": item.get("id"),
                        "name": item.get("name"),
                        "drs_url": drs_url,
                    }
                )
    return {
        "count": len(drs_items),
        "items": drs_items,
    }


def _handle_datasite_file_types(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_file_types()


def _handle_datasite_schema_job_list(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.list_schema_jobs(
        page=args.page,
        size=args.size,
        order_by=args.order_by,
        scope=args.scope,
        job_type=args.job_type,
    )


def _handle_datasite_schema_job_export(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.get_export_schema_jobs(page=args.page, size=args.size, order_by=args.order_by, scope=args.scope)


def _handle_datasite_schema_job_import(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.get_import_schema_jobs(page=args.page, size=args.size, order_by=args.order_by, scope=args.scope)


def _handle_datasite_schema_job_delete(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.delete_schema_job(args.id)


def _handle_datasite_drs_object(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.get_drs_object(args.object_id)


def _handle_datasite_drs_object_auth(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.post_drs_object(args.object_id, load_json_input(args))


def _handle_datasite_drs_access(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.get_drs_access(args.object_id, args.access_id)


def _handle_datasite_drs_access_auth(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    return client.post_drs_access(args.object_id, args.access_id, load_json_input(args))


def _handle_datasite_drs_resolve(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    resolved = _resolve_drs_download_info(
        client=client,
        drs_url=args.drs_url,
        access_id=getattr(args, "access_id", None),
    )
    return {
        "drs_url": args.drs_url,
        "object_id": resolved["object_id"],
        "selected_access_id": resolved.get("selected_access_id"),
        "access_url": resolved.get("access_url"),
        "file_name": resolved.get("file_name"),
        "object": resolved.get("object"),
        "access": resolved.get("access"),
    }


def _handle_datasite_drs_download(args: argparse.Namespace) -> dict:
    from bioos.ops.auth import build_datasite_client

    client = build_datasite_client(token=getattr(args, "token", None), cookie=getattr(args, "cookie", None))
    resolved = _resolve_drs_download_info(
        client=client,
        drs_url=args.drs_url,
        access_id=getattr(args, "access_id", None),
    )
    access_url = resolved.get("access_url")
    if not access_url:
        raise ValueError("Failed to resolve a downloadable HTTPS access URL from the DRS object.")
    output_path = _determine_download_target(
        target=args.target,
        file_name=resolved.get("file_name"),
        access_url=access_url,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(access_url, output_path)
    return {
        "success": True,
        "drs_url": args.drs_url,
        "object_id": resolved["object_id"],
        "selected_access_id": resolved.get("selected_access_id"),
        "access_url": access_url,
        "output_path": str(output_path),
        "file_name": output_path.name,
    }


def _mask_value(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if len(value) <= 6:
        return "*" * len(value)
    return f"{value[:4]}...{value[-2:]}"


def _is_repository_account_linked(result: Any) -> bool:
    if not isinstance(result, dict):
        return False
    exist_value = result.get("Exist")
    if isinstance(exist_value, bool):
        return exist_value
    if exist_value is not None:
        return str(exist_value).strip().lower() in {"1", "true", "yes"}
    email = result.get("Email")
    organizations = result.get("Organizations")
    return bool(email or organizations)


def _compute_passport_timestamps(expires_in: Optional[int]) -> tuple[Optional[str], Optional[str]]:
    if not expires_in:
        return None, None
    issued_at = datetime.now(timezone.utc)
    expires_at = issued_at + timedelta(seconds=expires_in)
    return issued_at.isoformat(), expires_at.isoformat()


def _parse_drs_url(drs_url: str) -> tuple[str, str]:
    parsed = urlparse(drs_url)
    if parsed.scheme != "drs":
        raise ValueError(f"Unsupported DRS URL: {drs_url}")
    object_id = parsed.path.lstrip("/")
    if not object_id:
        raise ValueError(f"Missing object ID in DRS URL: {drs_url}")
    return parsed.netloc, object_id


def _resolve_drs_download_info(client: Any, drs_url: str, access_id: Optional[str] = None) -> dict:
    _, object_id = _parse_drs_url(drs_url)
    obj = client.get_drs_object(object_id)
    access_url = None
    selected_access_id = access_id
    access_result = None

    if isinstance(obj, dict):
        access_url = _extract_access_url(obj)
        if not selected_access_id:
            selected_access_id = _select_access_id(obj)
        if not access_url and selected_access_id:
            access_result = client.get_drs_access(object_id, selected_access_id)
            access_url = _extract_access_url(access_result)

    return {
        "object_id": object_id,
        "selected_access_id": selected_access_id,
        "access_url": access_url,
        "file_name": _extract_drs_file_name(obj, drs_url, access_url),
        "object": obj,
        "access": access_result,
    }


def _select_access_id(obj: dict) -> Optional[str]:
    methods = obj.get("access_methods") or obj.get("accessMethods") or []
    if not isinstance(methods, list):
        return None
    for method in methods:
        if isinstance(method, dict):
            access_id = method.get("access_id") or method.get("accessId")
            if access_id:
                return access_id
    return None


def _extract_access_url(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    for key in ("url", "access_url", "accessUrl"):
        value = payload.get(key)
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            return value
    methods = payload.get("access_methods") or payload.get("accessMethods") or []
    if isinstance(methods, list):
        for method in methods:
            if isinstance(method, dict):
                for key in ("access_url", "accessUrl"):
                    value = method.get(key)
                    if isinstance(value, str) and value.startswith(("http://", "https://")):
                        return value
    access_urls = payload.get("access_urls") or payload.get("accessURLs")
    if isinstance(access_urls, dict):
        for value in access_urls.values():
            if isinstance(value, str) and value.startswith(("http://", "https://")):
                return value
    return None


def _extract_drs_file_name(obj: Any, drs_url: str, access_url: Optional[str]) -> str:
    if isinstance(obj, dict):
        for key in ("name", "fileName", "filename"):
            value = obj.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    if access_url:
        access_path = urlparse(access_url).path
        basename = Path(access_path).name
        if basename:
            return basename
    return _parse_drs_url(drs_url)[1]


def _determine_download_target(target: str, file_name: Optional[str], access_url: Optional[str]) -> Path:
    target_path = Path(target).expanduser()
    if target.endswith("/") or target_path.is_dir():
        chosen_name = file_name or "downloaded-file"
        return target_path / chosen_name
    if not target_path.suffix and not target_path.exists():
        guessed_name = file_name or (Path(urlparse(access_url or "").path).name if access_url else None)
        if guessed_name:
            return target_path / guessed_name
    return target_path


def _filter_datasite_file_items(
    *,
    items: list[Any],
    name_contains: Optional[str],
    regex_pattern: Optional[str],
    exact_drs_url: Optional[str],
    limit: Optional[int],
) -> list[dict]:
    regex = re.compile(regex_pattern) if regex_pattern else None
    substring = name_contains.lower() if name_contains else None
    matched: list[dict] = []

    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "")
        drs_url = item.get("drsURL") or item.get("drs_url")
        if not drs_url:
            continue
        if substring and substring not in name.lower():
            continue
        if regex and not regex.search(name):
            continue
        if exact_drs_url and drs_url != exact_drs_url:
            continue
        matched.append(
            {
                "id": item.get("id"),
                "name": name,
                "drs_url": drs_url,
            }
        )
        if limit is not None and len(matched) >= limit:
            break
    return matched




if __name__ == "__main__":
    sys.exit(main())
