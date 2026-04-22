import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

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
    main as cli_main,
    search_dockstore,
    upload_dashboard_file,
    upload_files_to_workspace,
    usage_metrics,
    update_workspace_members,
    validate_wdl,
)
from bioos import bioos_workflow, bw_import, bw_import_status_check, bw_status_check, get_submission_logs as submission_logs_module
from bioos.ops import auth as auth_ops


class TestCliHandlers(unittest.TestCase):
    def test_list_bioos_workspaces_handle(self):
        args = SimpleNamespace(page_size=1, ak="ak", sk="sk", endpoint="ep")
        fake_df = MagicMock()
        fake_df.empty = False
        fake_selected = MagicMock()

        with patch("bioos.cli.list_bioos_workspaces.login_with_args"), \
                patch("bioos.bioos.list_workspaces", return_value=fake_df), \
                patch("bioos.cli.list_bioos_workspaces.dataframe_records", return_value=[{"Name": "ws1"}, {"Name": "ws2"}]):
            fake_df.__getitem__.return_value = fake_selected
            result = list_bioos_workspaces.handle(args)

        self.assertEqual(result, [{"Name": "ws1"}])

    def test_list_workflows_from_workspace_handle(self):
        args = SimpleNamespace(workspace_name="ws", search_keyword="rna", page_number=2, page_size=5)
        with patch("bioos.cli.list_workflows_from_workspace.workspace_context_from_args", return_value=("wid", object())), \
                patch("bioos.service.api.list_workflows", return_value=[{"ID": "wf1"}]) as mocked:
            result = list_workflows_from_workspace.handle(args)
        mocked.assert_called_once_with(workspace_id="wid", search_keyword="rna", page_number=2, page_size=5)
        self.assertEqual(result, [{"ID": "wf1"}])

    def test_list_submissions_from_workspace_handle(self):
        args = SimpleNamespace(
            workspace_name="ws",
            workflow_name="wf",
            search_keyword="kw",
            status="Succeeded",
            page_number=1,
            page_size=10,
        )
        with patch("bioos.cli.list_submissions_from_workspace.workspace_context_from_args", return_value=("wid", object())), \
                patch("bioos.service.api.list_submissions", return_value=[{"ID": "sub1"}]) as mocked:
            result = list_submissions_from_workspace.handle(args)
        mocked.assert_called_once_with(
            workspace_id="wid",
            workflow_name="wf",
            search_keyword="kw",
            status="Succeeded",
            page_number=1,
            page_size=10,
        )
        self.assertEqual(result, [{"ID": "sub1"}])

    def test_generate_inputs_json_template_handle(self):
        args = SimpleNamespace(workspace_name="ws", workflow_name="wf")
        workflow = MagicMock()
        workflow.get_input_template.return_value = {"wf.input": "File"}
        ws = MagicMock()
        ws.workflow.return_value = workflow
        with patch("bioos.cli.generate_inputs_json_template_bioos.workspace_context_from_args", return_value=("wid", ws)):
            result = generate_inputs_json_template_bioos.handle(args)
        self.assertEqual(result, {"wf.input": "File"})

    def test_create_workspace_bioos_handle(self):
        args = SimpleNamespace(workspace_name="new", workspace_description="desc", ak="ak", sk="sk", endpoint="ep")
        ws = MagicMock()
        with patch("bioos.cli.create_workspace_bioos.login_with_args"), \
                patch("bioos.bioos.create_workspace", return_value={"ID": "wid"}) as create_mock, \
                patch("bioos.bioos.workspace", return_value=ws):
            result = create_workspace_bioos.handle(args)
        create_mock.assert_called_once_with(name="new", description="desc")
        self.assertTrue(result["success"])
        self.assertEqual(ws.bind_cluster.call_count, 2)

    def test_list_files_from_workspace_handle(self):
        args = SimpleNamespace(workspace_name="ws", prefix="analysis/", recursive=True)
        ws = MagicMock()
        ws.files.list.return_value = MagicMock()
        with patch("bioos.cli.list_files_from_workspace.workspace_context_from_args", return_value=("wid", ws)), \
                patch("bioos.cli.list_files_from_workspace.dataframe_records", return_value=[{"key": "a.txt"}]):
            result = list_files_from_workspace.handle(args)
        ws.files.list.assert_called_once_with(prefix="analysis/", recursive=True)
        self.assertEqual(result, [{"key": "a.txt"}])

    def test_download_files_from_workspace_handle(self):
        args = SimpleNamespace(workspace_name="ws", source=["a.txt", "b.txt"], target="/tmp/out", flatten=True)
        ws = MagicMock()
        ws.files.download.return_value = True
        with patch("bioos.cli.download_files_from_workspace.workspace_context_from_args", return_value=("wid", ws)):
            result = download_files_from_workspace.handle(args)
        ws.files.download.assert_called_once_with(sources=["a.txt", "b.txt"], target="/tmp/out", flatten=True)
        self.assertTrue(result["success"])

    def test_upload_files_to_workspace_handle(self):
        args = SimpleNamespace(
            workspace_name="ws",
            source=["a.txt", "b.txt"],
            target="input_provision/",
            flatten=True,
            skip_existing=True,
            checkpoint_dir="/tmp/ckpt",
            max_retries=5,
            task_num=8,
            ak="ak",
            sk="sk",
            endpoint="ep",
        )
        with patch("bioos.cli.upload_files_to_workspace.upload_local_files_to_workspace", return_value={"success": True}) as mocked:
            result = upload_files_to_workspace.handle(args)
        self.assertEqual(result, {"success": True})
        mocked.assert_called_once_with(
            workspace_name="ws",
            sources=["a.txt", "b.txt"],
            target="input_provision/",
            flatten=True,
            skip_existing=True,
            checkpoint_dir="/tmp/ckpt",
            max_retries=5,
            task_num=8,
            access_key="ak",
            secret_key="sk",
            endpoint="ep",
        )

    def test_create_iesapp_handle(self):
        args = SimpleNamespace(
            workspace_name="ws",
            ies_name="ies",
            ies_desc="desc",
            ies_resource="2c-4gib",
            ies_storage=1,
            ies_image="img",
            ies_ssh=True,
            ies_run_limit=10,
            ies_idle_timeout=20,
            ies_auto_start=False,
        )
        ws = MagicMock()
        ws.webinstanceapps.create_new_instance.return_value = {"ID": "ies-id"}
        with patch("bioos.cli.create_iesapp.workspace_context_from_args", return_value=("wid", ws)):
            result = create_iesapp.handle(args)
        self.assertTrue(result["success"])
        ws.webinstanceapps.create_new_instance.assert_called_once()

    def test_check_ies_status_handle(self):
        args = SimpleNamespace(workspace_name="ws", ies_name="ies")
        app = MagicMock(
            status="Running",
            status_detail={"State": "Running"},
            access_urls={"web": "https://example"},
            endpoint="endpoint",
            resource_size="2c-4gib",
            storage_capacity=123,
            ssh_info={"host": "x"},
        )
        ws = MagicMock()
        ws.webinstanceapp.return_value = app
        with patch("bioos.cli.check_ies_status.workspace_context_from_args", return_value=("wid", ws)), \
                patch("bioos.cli.check_ies_status.dataframe_records", return_value=[{"Name": "ies"}]):
            result = check_ies_status.handle(args)
        self.assertEqual(result["status"], "Running")
        self.assertEqual(result["list_record"], {"Name": "ies"})

    def test_get_ies_events_handle(self):
        args = SimpleNamespace(workspace_name="ws", ies_name="ies")
        ws = MagicMock()
        ws.webinstanceapps.get_events.return_value = [{"Level": "Info"}]
        with patch("bioos.cli.get_ies_events.workspace_context_from_args", return_value=("wid", ws)):
            result = get_ies_events.handle(args)
        self.assertEqual(result["events"], [{"Level": "Info"}])

    def test_get_workspace_profile_handle(self):
        args = SimpleNamespace(
            workspace_name="ws",
            submission_limit=5,
            artifact_limit_per_submission=10,
            sample_rows_per_data_model=3,
            include_artifacts=True,
            include_failure_details=False,
            include_ies=True,
            include_signed_urls=False,
            endpoint="ep",
            ak="ak",
            sk="sk",
        )
        with patch("bioos.cli.get_workspace_profile.get_workspace_profile_data", return_value={"success": True}) as mocked:
            result = get_workspace_profile.handle(args)
        self.assertEqual(result, {"success": True})
        options = mocked.call_args.args[0]
        self.assertEqual(options.workspace_name, "ws")
        self.assertFalse(options.include_failure_details)

    def test_validate_wdl_handle(self):
        args = SimpleNamespace(wdl_path="test.wdl")
        with patch("bioos.cli.validate_wdl.validate_wdl_file", return_value={"success": True}) as mocked:
            result = validate_wdl.handle(args)
        mocked.assert_called_once_with("test.wdl")
        self.assertEqual(result["success"], True)

    def test_delete_submission_handle(self):
        args = SimpleNamespace(workspace_name="ws", submission_id="sub1")
        submission = MagicMock()
        with patch("bioos.cli.delete_submission.workspace_context_from_args", return_value=("wid", object())), \
                patch("bioos.resource.workflows.Submission", return_value=submission):
            result = delete_submission.handle(args)
        submission.delete.assert_called_once_with()
        self.assertTrue(result["success"])

    def test_export_bioos_workspace_handle(self):
        args = SimpleNamespace(workspace_name="ws", export_path="/tmp/export.json")
        ws = MagicMock()
        ws.export_workspace_v2.return_value = {"status": "succeeded"}
        with patch("bioos.cli.export_bioos_workspace.workspace_context_from_args", return_value=("wid", ws)):
            result = export_bioos_workspace.handle(args)
        self.assertTrue(result["success"])
        ws.export_workspace_v2.assert_called_once()

    def test_upload_dashboard_file_handle(self):
        args = SimpleNamespace(workspace_name="ws", local_file_path="__dashboard__.md", ak="ak", sk="sk", endpoint="ep")
        with patch("bioos.cli.upload_dashboard_file.upload_dashboard_file_to_workspace", return_value={"success": True}) as mocked:
            result = upload_dashboard_file.handle(args)
        self.assertEqual(result, {"success": True})
        mocked.assert_called_once()

    def test_list_workspace_members_handle(self):
        args = SimpleNamespace(
            workspace_name="ws",
            page_number=2,
            page_size=50,
            in_workspace=True,
            role=["Admin"],
            keyword="alice",
        )
        ws = MagicMock()
        ws.list_members.return_value = [{"Name": "alice", "Role": "Admin"}]
        with patch("bioos.cli.list_workspace_members.workspace_context_from_args", return_value=("wid", ws)):
            result = list_workspace_members.handle(args)
        ws.list_members.assert_called_once_with(
            page_number=2,
            page_size=50,
            in_workspace=True,
            roles=["Admin"],
            keyword="alice",
        )
        self.assertEqual(result["members"], [{"Name": "alice", "Role": "Admin"}])

    def test_add_workspace_members_handle(self):
        args = SimpleNamespace(workspace_name="ws", name=["alice", "bob"], role="User")
        ws = MagicMock()
        ws.add_members.return_value = {"updated": 2}
        with patch("bioos.cli.add_workspace_members.workspace_context_from_args", return_value=("wid", ws)):
            result = add_workspace_members.handle(args)
        ws.add_members.assert_called_once_with(names=["alice", "bob"], role="User")
        self.assertEqual(result["result"], {"updated": 2})

    def test_update_workspace_members_handle(self):
        args = SimpleNamespace(workspace_name="ws", name=["alice"], role="Admin")
        ws = MagicMock()
        ws.update_members.return_value = {"updated": 1}
        with patch("bioos.cli.update_workspace_members.workspace_context_from_args", return_value=("wid", ws)):
            result = update_workspace_members.handle(args)
        ws.update_members.assert_called_once_with(names=["alice"], role="Admin")
        self.assertEqual(result["result"], {"updated": 1})

    def test_delete_workspace_members_handle(self):
        args = SimpleNamespace(workspace_name="ws", name=["alice"])
        ws = MagicMock()
        ws.delete_members.return_value = {"deleted": 1}
        with patch("bioos.cli.delete_workspace_members.workspace_context_from_args", return_value=("wid", ws)):
            result = delete_workspace_members.handle(args)
        ws.delete_members.assert_called_once_with(names=["alice"])
        self.assertEqual(result["result"], {"deleted": 1})

    def test_usage_asset_usage_data_handle(self):
        args = SimpleNamespace(start_time=1, end_time=2, type="WorkspaceVisit", ak="ak", sk="sk", endpoint="ep")
        usage = MagicMock()
        usage.get_asset_usage_data.return_value = {"Items": []}
        with patch("bioos.cli.usage_metrics.login_with_args"), \
                patch("bioos.bioos.usage", return_value=usage):
            result = usage_metrics.handle_asset_usage_data(args)
        usage.get_asset_usage_data.assert_called_once_with(1, 2, "WorkspaceVisit")
        self.assertEqual(result["result"], {"Items": []})

    def test_usage_user_resource_usage_handle(self):
        args = SimpleNamespace(start_time=1, end_time=2, ak="ak", sk="sk", endpoint="ep")
        usage = MagicMock()
        usage.list_user_resource_usage.return_value = {"Items": []}
        with patch("bioos.cli.usage_metrics.login_with_args"), \
                patch("bioos.bioos.usage", return_value=usage):
            result = usage_metrics.handle_user_resource_usage(args)
        usage.list_user_resource_usage.assert_called_once_with(1, 2)
        self.assertEqual(result["result"], {"Items": []})

    def test_search_dockstore_handle(self):
        args = SimpleNamespace(
            query=[["description", "AND", "RNA-seq"]],
            top_n=3,
            query_type="match_phrase",
            sentence=False,
            output_full=True,
        )
        with patch("bioos.cli.search_dockstore.search_dockstore_workflows", return_value={"success": True}) as mocked:
            result = search_dockstore.handle(args)
        self.assertEqual(result, {"success": True})
        mocked.assert_called_once()

    def test_fetch_wdl_from_dockstore_handle(self):
        args = SimpleNamespace(url="https://dockstore/workflows/x/y/z", output_path=".")
        with patch("bioos.cli.fetch_wdl_from_dockstore.fetch_wdl_from_dockstore_url", return_value={"success": True}) as mocked:
            result = fetch_wdl_from_dockstore.handle(args)
        mocked.assert_called_once_with("https://dockstore/workflows/x/y/z", ".")
        self.assertEqual(result, {"success": True})

    def test_get_docker_image_url_handle(self):
        args = SimpleNamespace(repo_name="repo", tag="v1", registry="reg", namespace_name="ns")
        result = get_docker_image_url.handle(args)
        self.assertEqual(result, {"image_url": "reg/ns/repo:v1"})

    def test_build_docker_image_handle(self):
        args = SimpleNamespace(repo_name="repo", tag="v1", source_path="Dockerfile", registry="reg", namespace_name="ns")
        with patch("bioos.cli.build_docker_image.build_docker_image_request", return_value={"TaskID": "1"}) as mocked:
            result = build_docker_image.handle(args)
        mocked.assert_called_once()
        self.assertEqual(result, {"TaskID": "1"})

    def test_check_build_status_handle(self):
        args = SimpleNamespace(task_id="task-1")
        with patch("bioos.cli.check_build_status.check_build_status_request", return_value={"Status": "Running"}) as mocked:
            result = check_build_status.handle(args)
        mocked.assert_called_once_with("task-1")
        self.assertEqual(result, {"Status": "Running"})


class TestCliRootAndAuth(unittest.TestCase):
    def test_resolve_auth_settings_reads_config_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                'client:\n'
                '  MIRACLE_ACCESS_KEY: "cfg-ak"\n'
                '  MIRACLE_SECRET_KEY: "cfg-sk"\n'
                '  serveraddr: "https://cfg-endpoint"\n'
                '  region: "cfg-region"\n',
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"BIOOS_CONFIG_PATH": str(config_path)}, clear=True):
                settings = auth_ops.resolve_auth_settings()

        self.assertEqual(settings["access_key"], "cfg-ak")
        self.assertEqual(settings["secret_key"], "cfg-sk")
        self.assertEqual(settings["endpoint"], "https://cfg-endpoint")
        self.assertEqual(settings["region"], "cfg-region")
        self.assertEqual(settings["access_key_source"], "config")

    def test_resolve_auth_settings_prefers_cli_over_env_and_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                'client:\n'
                '  MIRACLE_ACCESS_KEY: "cfg-ak"\n'
                '  MIRACLE_SECRET_KEY: "cfg-sk"\n'
                '  serveraddr: "https://cfg-endpoint"\n',
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "BIOOS_CONFIG_PATH": str(config_path),
                    "MIRACLE_ACCESS_KEY": "env-ak",
                    "MIRACLE_SECRET_KEY": "env-sk",
                    "BIOOS_ENDPOINT": "https://env-endpoint",
                },
                clear=True,
            ):
                settings = auth_ops.resolve_auth_settings(
                    access_key="cli-ak",
                    secret_key="cli-sk",
                    endpoint="https://cli-endpoint",
                )

        self.assertEqual(settings["access_key"], "cli-ak")
        self.assertEqual(settings["secret_key"], "cli-sk")
        self.assertEqual(settings["endpoint"], "https://cli-endpoint")
        self.assertEqual(settings["access_key_source"], "cli")
        self.assertEqual(settings["secret_key_source"], "cli")
        self.assertEqual(settings["endpoint_source"], "cli")

    def test_resolve_auth_settings_ignores_legacy_env_names(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            empty_config_path = Path(tmpdir) / "missing-config.yaml"
            with patch.dict(
                os.environ,
                {
                    "VOLC_ACCESSKEY": "legacy-ak",
                    "VOLC_SECRETKEY": "legacy-sk",
                    "BIOOS_CONFIG_PATH": str(empty_config_path),
                },
                clear=True,
            ):
                settings = auth_ops.resolve_auth_settings()

        self.assertIsNone(settings["access_key"])
        self.assertIsNone(settings["secret_key"])
        self.assertEqual(settings["access_key_source"], "missing")
        self.assertEqual(settings["secret_key_source"], "missing")

    def test_resolve_auth_settings_ignores_legacy_config_key_names(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                'client:\n'
                '  accesskey: "legacy-ak"\n'
                '  secretkey: "legacy-sk"\n'
                '  serveraddr: "https://cfg-endpoint"\n',
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"BIOOS_CONFIG_PATH": str(config_path)}, clear=True):
                settings = auth_ops.resolve_auth_settings()

        self.assertIsNone(settings["access_key"])
        self.assertIsNone(settings["secret_key"])
        self.assertEqual(settings["access_key_source"], "missing")
        self.assertEqual(settings["secret_key_source"], "missing")
        self.assertEqual(settings["endpoint"], "https://cfg-endpoint")

    def test_root_workspace_list_dispatches_to_existing_handler(self):
        with patch("bioos.cli.list_bioos_workspaces.handle", return_value=[{"Name": "ws1"}]) as mocked:
            exit_code = cli_main.main(["workspace", "list", "--output", "json"])

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_workspace_export_dispatches_to_existing_handler(self):
        with patch("bioos.cli.export_bioos_workspace.handle", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                ["workspace", "export", "--workspace-name", "ws", "--export-path", "/tmp/x", "--output", "json"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_workspace_profile_dispatches_to_existing_handler(self):
        with patch("bioos.cli.get_workspace_profile.handle", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                ["workspace", "profile", "--workspace-name", "ws", "--output", "json"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_workspace_dashboard_upload_dispatches_to_existing_handler(self):
        with patch("bioos.cli.upload_dashboard_file.handle", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                [
                    "workspace",
                    "dashboard-upload",
                    "--workspace-name",
                    "ws",
                    "--local-file-path",
                    "__dashboard__.md",
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_workspace_member_list_dispatches_to_existing_handler(self):
        with patch("bioos.cli.list_workspace_members.handle", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                ["workspace", "member", "list", "--workspace-name", "ws", "--output", "json"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_workspace_member_add_dispatches_to_existing_handler(self):
        with patch("bioos.cli.add_workspace_members.handle", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                [
                    "workspace",
                    "member",
                    "add",
                    "--workspace-name",
                    "ws",
                    "--name",
                    "alice",
                    "--role",
                    "User",
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_workspace_member_update_dispatches_to_existing_handler(self):
        with patch("bioos.cli.update_workspace_members.handle", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                [
                    "workspace",
                    "member",
                    "update",
                    "--workspace-name",
                    "ws",
                    "--name",
                    "alice",
                    "--role",
                    "Admin",
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_workspace_member_delete_dispatches_to_existing_handler(self):
        with patch("bioos.cli.delete_workspace_members.handle", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                [
                    "workspace",
                    "member",
                    "delete",
                    "--workspace-name",
                    "ws",
                    "--name",
                    "alice",
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_usage_asset_data_dispatches_to_existing_handler(self):
        with patch("bioos.cli.usage_metrics.handle_asset_usage_data", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                ["usage", "asset-data", "--start-time", "1", "--end-time", "2", "--type", "WorkspaceVisit", "--output", "json"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_usage_resource_user_list_dispatches_to_existing_handler(self):
        with patch("bioos.cli.usage_metrics.handle_user_resource_usage", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                ["usage", "resource-user-list", "--start-time", "1", "--end-time", "2", "--output", "json"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_file_upload_dispatches_to_existing_handler(self):
        with patch("bioos.cli.upload_files_to_workspace.handle", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                [
                    "file",
                    "upload",
                    "--workspace-name",
                    "ws",
                    "--source",
                    "a.txt",
                    "--checkpoint-dir",
                    "/tmp/ckpt",
                    "--max-retries",
                    "5",
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_config_path_returns_success(self):
        exit_code = cli_main.main(["config", "path", "--output", "json"])
        self.assertEqual(exit_code, 0)

    def test_root_workflow_list_dispatches_to_existing_handler(self):
        with patch("bioos.cli.list_workflows_from_workspace.handle", return_value=[{"ID": "wf1"}]) as mocked:
            exit_code = cli_main.main(
                ["workflow", "list", "--workspace-name", "ws", "--output", "json"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_workflow_input_template_dispatches_to_existing_handler(self):
        with patch(
            "bioos.cli.generate_inputs_json_template_bioos.handle",
            return_value={"wf.input": "File"},
        ) as mocked:
            exit_code = cli_main.main(
                [
                    "workflow",
                    "input-template",
                    "--workspace-name",
                    "ws",
                    "--workflow-name",
                    "wf",
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_workflow_import_uses_legacy_adapter(self):
        with patch("bioos.bw_import.handle", return_value="ok") as mocked:
            exit_code = cli_main.main(
                [
                    "workflow",
                    "import",
                    "--workspace-name",
                    "ws",
                    "--workflow-name",
                    "wf",
                    "--workflow-source",
                    "main.wdl",
                ]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_workflow_import_status_uses_legacy_adapter(self):
        with patch("bioos.bw_import_status_check.handle", return_value="Status: Succeeded") as mocked:
            exit_code = cli_main.main(
                ["workflow", "import-status", "--workspace-name", "ws", "--workflow-id", "wfid"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_workflow_run_status_uses_legacy_adapter(self):
        with patch("bioos.bw_status_check.handle", return_value="Runs Status") as mocked:
            exit_code = cli_main.main(
                ["workflow", "run-status", "--workspace-name", "ws", "--submission-id", "sub1"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_workflow_submit_uses_legacy_adapter(self):
        with patch("bioos.bioos_workflow.handle", return_value="submitted") as mocked:
            exit_code = cli_main.main(
                [
                    "workflow",
                    "submit",
                    "--workspace-name",
                    "ws",
                    "--workflow-name",
                    "wf",
                    "--input-json",
                    "inputs.json",
                ]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_workflow_validate_dispatches_to_existing_handler(self):
        with patch("bioos.cli.validate_wdl.handle", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(["workflow", "validate", "--wdl-path", "test.wdl", "--output", "json"])

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_submission_list_dispatches_to_existing_handler(self):
        with patch("bioos.cli.list_submissions_from_workspace.handle", return_value=[{"ID": "sub1"}]) as mocked:
            exit_code = cli_main.main(
                ["submission", "list", "--workspace-name", "ws", "--output", "json"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_submission_logs_uses_legacy_adapter(self):
        with patch("bioos.get_submission_logs.handle", return_value="downloaded") as mocked:
            exit_code = cli_main.main(
                ["submission", "logs", "--workspace-name", "ws", "--submission-id", "sub1"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_legacy_workflow_import_handle_uses_unified_login(self):
        args = bw_import.build_parser().parse_args(
            ["--workspace_name", "ws", "--workflow_name", "wf", "--workflow_source", "main.wdl"]
        )
        resource = MagicMock()
        resource.import_workflow.return_value = {"ID": "wf1"}
        resource.list.return_value = MagicMock()
        with patch("bioos.bw_import.login_to_bioos") as login_mock, \
                patch("bioos.bw_import.resolve_workspace", return_value=("wid", {})), \
                patch("bioos.bw_import.WorkflowResource", return_value=resource):
            result = bw_import.handle(args)

        login_mock.assert_called_once()
        self.assertIn("still validating", result)

    def test_legacy_workflow_submit_handle_uses_unified_login(self):
        args = bioos_workflow.build_parser().parse_args(
            ["--workspace_name", "ws", "--workflow_name", "wf", "--input_json", "inputs.json"]
        )
        fake_run = SimpleNamespace(id="run1", submission="sub1", status="Submitted")
        fake_bw = MagicMock()
        fake_bw.runs = [fake_run]
        with patch("bioos.bioos_workflow.login_to_bioos") as login_mock, \
                patch("bioos.bioos_workflow.Bioos_workflow", return_value=fake_bw):
            result = bioos_workflow.handle(args)

        login_mock.assert_called_once()
        self.assertIn("Submission ID: sub1", result)

    def test_preprocess2_uses_workspace_upload_helper(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_json = Path(tmpdir) / "inputs.json"
            local_file = Path(tmpdir) / "sample.txt"
            local_file.write_text("data", encoding="utf-8")
            input_json.write_text(
                '{"wf.input":"' + str(local_file) + '","wf.name":"sample"}',
                encoding="utf-8",
            )

            class FakeSeries:
                def __len__(self):
                    return 1

                def to_list(self):
                    return ["wid"]

            class FakeWorkspaces:
                Name = "ws"
                ID = FakeSeries()

                def __getitem__(self, key):
                    return self

            with patch("bioos.bioos_workflow.bioos.list_workspaces") as list_mock, \
                    patch("bioos.bioos_workflow.bioos.workspace") as workspace_mock, \
                    patch("bioos.bioos_workflow._upload_local_files_with_workspace") as upload_mock:
                list_mock.return_value = FakeWorkspaces()

                ws = MagicMock()
                workspace_mock.return_value = ws
                bw = bioos_workflow.Bioos_workflow(workspace_name="ws", workflow_name="wf")
                upload_mock.return_value = {
                    "uploaded_files": [{
                        "source": str(local_file),
                        "key": "input_provision/sample.txt",
                        "s3_url": "s3://bioos-wid/input_provision/sample.txt",
                    }],
                    "skipped_files": [],
                }

                result = bw.preprocess2(
                    input_json_file=str(input_json),
                    data_model_name="dm",
                    submission_desc="desc",
                    call_caching=True,
                    force_reupload=False,
                    mount_tos=False,
                )

        upload_mock.assert_called_once()
        self.assertIn("s3://bioos-wid/input_provision/sample.txt", result["inputs"])

    def test_dataframe_map_compat_falls_back_to_applymap(self):
        class FakeDataFrame:
            def __init__(self):
                self.called = None

            def applymap(self, func):
                self.called = ("applymap", func("x"))
                return "applymap-result"

        fake_df = FakeDataFrame()
        result = bioos_workflow.dataframe_map_compat(fake_df, str)
        self.assertEqual(result, "applymap-result")
        self.assertEqual(fake_df.called, ("applymap", "x"))

    def test_preprocess2_replaces_chinese_local_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            chinese_dir = Path(tmpdir) / "广实上交模型"
            chinese_dir.mkdir()
            local_file = chinese_dir / "data.xlsx"
            local_file.write_text("data", encoding="utf-8")
            input_json = Path(tmpdir) / "inputs.json"
            input_json.write_text(
                json.dumps({
                    "wf.input_excel": str(local_file),
                    "wf.region": "north",
                }, ensure_ascii=False),
                encoding="utf-8",
            )

            class FakeSeries:
                def __len__(self):
                    return 1

                def to_list(self):
                    return ["wid"]

            class FakeWorkspaces:
                Name = "ws"
                ID = FakeSeries()

                def __getitem__(self, key):
                    return self

            with patch("bioos.bioos_workflow.bioos.list_workspaces", return_value=FakeWorkspaces()), \
                    patch("bioos.bioos_workflow.bioos.workspace", return_value=MagicMock()), \
                    patch("bioos.bioos_workflow._upload_local_files_with_workspace") as upload_mock:
                upload_mock.return_value = {
                    "uploaded_files": [{
                        "source": str(local_file),
                        "key": "input_provision/data.xlsx",
                        "s3_url": "s3://bioos-wid/input_provision/data.xlsx",
                    }],
                    "skipped_files": [],
                }
                bw = bioos_workflow.Bioos_workflow(workspace_name="ws", workflow_name="wf")
                result = bw.preprocess2(
                    input_json_file=str(input_json),
                    data_model_name="dm",
                    submission_desc="desc",
                    call_caching=True,
                    force_reupload=False,
                    mount_tos=False,
                )

        upload_mock.assert_called_once()
        self.assertIn("s3://bioos-wid/input_provision/data.xlsx", result["inputs"])

    def test_preprocess2_batch_replaces_multiple_local_paths(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            file_a = Path(tmpdir) / "a.txt"
            file_b = Path(tmpdir) / "b.txt"
            file_a.write_text("a", encoding="utf-8")
            file_b.write_text("b", encoding="utf-8")
            input_json = Path(tmpdir) / "inputs.json"
            input_json.write_text(
                json.dumps([
                    {"wf.input": str(file_a), "wf.region": "north"},
                    {"wf.input": str(file_b), "wf.region": "south"},
                ], ensure_ascii=False),
                encoding="utf-8",
            )

            class FakeSeries:
                def __len__(self):
                    return 1

                def to_list(self):
                    return ["wid"]

            class FakeWorkspaces:
                Name = "ws"
                ID = FakeSeries()

                def __getitem__(self, key):
                    return self

            ws = MagicMock()
            ws.data_models.write = MagicMock()
            with patch("bioos.bioos_workflow.bioos.list_workspaces", return_value=FakeWorkspaces()), \
                    patch("bioos.bioos_workflow.bioos.workspace", return_value=ws), \
                    patch("bioos.bioos_workflow._upload_local_files_with_workspace") as upload_mock:
                upload_mock.return_value = {
                    "uploaded_files": [
                        {
                            "source": str(file_a),
                            "key": "input_provision/a.txt",
                            "s3_url": "s3://bioos-wid/input_provision/a.txt",
                        },
                        {
                            "source": str(file_b),
                            "key": "input_provision/b.txt",
                            "s3_url": "s3://bioos-wid/input_provision/b.txt",
                        },
                    ],
                    "skipped_files": [],
                }
                bw = bioos_workflow.Bioos_workflow(workspace_name="ws", workflow_name="wf")
                result = bw.preprocess2(
                    input_json_file=str(input_json),
                    data_model_name="batch_dm",
                    submission_desc="desc",
                    call_caching=True,
                    force_reupload=False,
                    mount_tos=False,
                )

        upload_mock.assert_called_once()
        written_payload = ws.data_models.write.call_args.args[0]
        written_df = written_payload["batch_dm"]
        self.assertIn("s3://bioos-wid/input_provision/a.txt", written_df["input"].tolist())
        self.assertIn("s3://bioos-wid/input_provision/b.txt", written_df["input"].tolist())
        self.assertIn('"this.input"', result["inputs"])
        self.assertEqual(result["data_model_name"], "batch_dm")
        self.assertEqual(len(result["row_ids"]), 2)

    def test_preprocess2_without_local_files_skips_upload_helper(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_json = Path(tmpdir) / "inputs.json"
            input_json.write_text(
                json.dumps({"wf.input": "drs://bucket/object", "wf.region": "north"}, ensure_ascii=False),
                encoding="utf-8",
            )

            class FakeSeries:
                def __len__(self):
                    return 1

                def to_list(self):
                    return ["wid"]

            class FakeWorkspaces:
                Name = "ws"
                ID = FakeSeries()

                def __getitem__(self, key):
                    return self

            with patch("bioos.bioos_workflow.bioos.list_workspaces", return_value=FakeWorkspaces()), \
                    patch("bioos.bioos_workflow.bioos.workspace", return_value=MagicMock()), \
                    patch("bioos.bioos_workflow._upload_local_files_with_workspace") as upload_mock:
                bw = bioos_workflow.Bioos_workflow(workspace_name="ws", workflow_name="wf")
                result = bw.preprocess2(
                    input_json_file=str(input_json),
                    data_model_name="dm",
                    submission_desc="desc",
                    call_caching=True,
                    force_reupload=False,
                    mount_tos=False,
                )

        upload_mock.assert_not_called()
        self.assertIn("drs://bucket/object", result["inputs"])

    def test_preprocess2_force_reupload_disables_skip_existing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            local_file = Path(tmpdir) / "sample.txt"
            local_file.write_text("x", encoding="utf-8")
            input_json = Path(tmpdir) / "inputs.json"
            input_json.write_text(
                json.dumps({"wf.input": str(local_file)}, ensure_ascii=False),
                encoding="utf-8",
            )

            class FakeSeries:
                def __len__(self):
                    return 1

                def to_list(self):
                    return ["wid"]

            class FakeWorkspaces:
                Name = "ws"
                ID = FakeSeries()

                def __getitem__(self, key):
                    return self

            with patch("bioos.bioos_workflow.bioos.list_workspaces", return_value=FakeWorkspaces()), \
                    patch("bioos.bioos_workflow.bioos.workspace", return_value=MagicMock()), \
                    patch("bioos.bioos_workflow._upload_local_files_with_workspace") as upload_mock:
                upload_mock.return_value = {
                    "uploaded_files": [{
                        "source": str(local_file),
                        "key": "input_provision/sample.txt",
                        "s3_url": "s3://bioos-wid/input_provision/sample.txt",
                    }],
                    "skipped_files": [],
                }
                bw = bioos_workflow.Bioos_workflow(workspace_name="ws", workflow_name="wf")
                bw.preprocess2(
                    input_json_file=str(input_json),
                    data_model_name="dm",
                    submission_desc="desc",
                    call_caching=True,
                    force_reupload=True,
                    mount_tos=False,
                )

        self.assertFalse(upload_mock.call_args.kwargs["skip_existing"])

    def test_collect_local_file_paths_recursively_filters_and_deduplicates(self):
        existing_paths = {
            "/tmp/project/广实上交模型/data.xlsx",
            "/tmp/project/with space/report final.csv",
            "/tmp/project/a.txt",
        }
        payload = {
            "single": "/tmp/project/a.txt",
            "nested": {
                "batch": [
                    "/tmp/project/广实上交模型/data.xlsx",
                    "s3://bucket/already-uploaded.txt",
                    "registry-vpc://image",
                    "/tmp/project/missing.txt",
                    {
                        "again": "/tmp/project/a.txt",
                        "with_space": "/tmp/project/with space/report final.csv",
                    },
                ]
            },
            "number": 123,
            "boolean": True,
            "none": None,
        }

        with patch("bioos.bioos_workflow.os.path.isfile", side_effect=lambda value: value in existing_paths):
            result = bioos_workflow.collect_local_file_paths(payload)

        self.assertEqual(
            result,
            {
                "/tmp/project/a.txt",
                "/tmp/project/广实上交模型/data.xlsx",
                "/tmp/project/with space/report final.csv",
            },
        )

    def test_collect_local_file_paths_supports_batch_input_list(self):
        existing_paths = {
            "/tmp/batch/a.fastq.gz",
            "/tmp/batch/b.fastq.gz",
        }
        payload = [
            {"wf.input": "/tmp/batch/a.fastq.gz", "wf.region": "north"},
            {"wf.input": "/tmp/batch/b.fastq.gz", "wf.region": "south"},
            {"wf.input": "drs://archive/object", "wf.region": "west"},
        ]

        with patch("bioos.bioos_workflow.os.path.isfile", side_effect=lambda value: value in existing_paths):
            result = bioos_workflow.collect_local_file_paths(payload)

        self.assertEqual(
            result,
            {
                "/tmp/batch/a.fastq.gz",
                "/tmp/batch/b.fastq.gz",
            },
        )

    def test_replace_local_paths_with_s3_recursively_rewrites_exact_matches(self):
        payload = {
            "wf.input": "/tmp/project/a.txt",
            "nested": [
                "/tmp/project/广实上交模型/data.xlsx",
                {
                    "keep": "drs://archive/object",
                    "replace": "/tmp/project/a.txt",
                },
            ],
            "message": "prefix /tmp/project/a.txt should not be partially replaced inside longer text",
        }
        source_to_s3 = {
            "/tmp/project/a.txt": "s3://bioos-wid/input_provision/a.txt",
            "/tmp/project/广实上交模型/data.xlsx": "s3://bioos-wid/input_provision/data.xlsx",
        }

        result = bioos_workflow.replace_local_paths_with_s3(payload, source_to_s3)

        self.assertEqual(
            result,
            {
                "wf.input": "s3://bioos-wid/input_provision/a.txt",
                "nested": [
                    "s3://bioos-wid/input_provision/data.xlsx",
                    {
                        "keep": "drs://archive/object",
                        "replace": "s3://bioos-wid/input_provision/a.txt",
                    },
                ],
                "message": "prefix /tmp/project/a.txt should not be partially replaced inside longer text",
            },
        )

    def test_legacy_workflow_submit_entrypoint_exits_cleanly(self):
        parser = MagicMock()
        parser.parse_args.return_value = SimpleNamespace()
        with patch("bioos.bioos_workflow.build_parser", return_value=parser), \
                patch("bioos.bioos_workflow.handle", return_value="submitted"), \
                patch("builtins.print") as print_mock, \
                patch("bioos.bioos_workflow.sys.exit", side_effect=SystemExit(0)) as exit_mock:
            with self.assertRaises(SystemExit) as raised:
                bioos_workflow.bioos_workflow()

        self.assertEqual(raised.exception.code, 0)
        exit_mock.assert_called_once_with(0)
        print_mock.assert_any_call("submitted")

    def test_legacy_workflow_import_status_handle_uses_unified_login(self):
        args = bw_import_status_check.build_parser().parse_args(
            ["--workspace_name", "ws", "--workflow_id", "wf1"]
        )
        workflow_df = MagicMock()
        workflow_info = MagicMock()
        workflow_df.__getitem__.return_value = workflow_info
        workflow_df.ID = MagicMock()
        workflow_info.__len__.return_value = 1
        workflow_info.iloc = [{"Status": {"Phase": "Succeeded", "Message": "ok"}}]
        resource = MagicMock()
        resource.list.return_value = workflow_df
        with patch("bioos.bw_import_status_check.login_to_bioos") as login_mock, \
                patch("bioos.bw_import_status_check.resolve_workspace", return_value=("wid", {})), \
                patch("bioos.bw_import_status_check.WorkflowResource", return_value=resource):
            result = bw_import_status_check.handle(args)

        login_mock.assert_called_once()
        self.assertIn("Status: Succeeded", result)

    def test_legacy_workflow_run_status_handle_uses_unified_login(self):
        args = bw_status_check.build_parser().parse_args(
            ["--workspace_name", "ws", "--submission_id", "sub1"]
        )
        with patch("bioos.bw_status_check.login_to_bioos") as login_mock, \
                patch("bioos.bw_status_check.resolve_workspace", return_value=("wid", {})), \
                patch("bioos.bw_status_check.Config.service") as service_mock:
            service_mock.return_value.list_runs.return_value = {
                "Items": [{"ID": "run1", "Status": "Succeeded", "Message": "", "Outputs": "{}"}]
            }
            result = bw_status_check.handle(args)

        login_mock.assert_called_once()
        self.assertIn("Submission ID: sub1", result)

    def test_legacy_submission_logs_handle_uses_unified_login(self):
        args = submission_logs_module.build_parser().parse_args(
            ["--workspace_name", "ws", "--submission_id", "sub1"]
        )
        ws = MagicMock()
        ws.files.list.return_value = SimpleNamespace(key=["sub1/log/stdout"])
        with tempfile.TemporaryDirectory() as tmpdir, \
                patch("bioos.get_submission_logs.login_to_bioos") as login_mock, \
                patch("bioos.get_submission_logs.resolve_workspace", return_value=("wid", {})), \
                patch("bioos.get_submission_logs.bioos.workspace", return_value=ws), \
                patch("bioos.get_submission_logs.os.walk", return_value=[(tmpdir, [], ["stdout"])]):
            args.output_dir = tmpdir
            result = submission_logs_module.handle(args)

        login_mock.assert_called_once()
        self.assertIn("Downloaded files", result)

    def test_root_submission_delete_dispatches_to_existing_handler(self):
        with patch("bioos.cli.delete_submission.handle", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                [
                    "submission",
                    "delete",
                    "--workspace-name",
                    "ws",
                    "--submission-id",
                    "sub1",
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_file_list_dispatches_to_existing_handler(self):
        with patch("bioos.cli.list_files_from_workspace.handle", return_value=[{"key": "a.txt"}]) as mocked:
            exit_code = cli_main.main(
                ["file", "list", "--workspace-name", "ws", "--output", "json"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_file_download_dispatches_to_existing_handler(self):
        with patch("bioos.cli.download_files_from_workspace.handle", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                [
                    "file",
                    "download",
                    "--workspace-name",
                    "ws",
                    "--source",
                    "a.txt",
                    "--target",
                    "/tmp/out",
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_ies_create_dispatches_to_existing_handler(self):
        with patch("bioos.cli.create_iesapp.handle", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                [
                    "ies",
                    "create",
                    "--workspace-name",
                    "ws",
                    "--ies-name",
                    "ies",
                    "--ies-desc",
                    "desc",
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_ies_status_dispatches_to_existing_handler(self):
        with patch("bioos.cli.check_ies_status.handle", return_value={"status": "Running"}) as mocked:
            exit_code = cli_main.main(
                ["ies", "status", "--workspace-name", "ws", "--ies-name", "ies", "--output", "json"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_ies_events_dispatches_to_existing_handler(self):
        with patch("bioos.cli.get_ies_events.handle", return_value={"events": []}) as mocked:
            exit_code = cli_main.main(
                ["ies", "events", "--workspace-name", "ws", "--ies-name", "ies", "--output", "json"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_dockstore_search_dispatches_to_existing_handler(self):
        with patch("bioos.cli.search_dockstore.handle", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                [
                    "dockstore",
                    "search",
                    "--query",
                    "description",
                    "AND",
                    "RNA-seq",
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_dockstore_fetch_dispatches_to_existing_handler(self):
        with patch("bioos.cli.fetch_wdl_from_dockstore.handle", return_value={"success": True}) as mocked:
            exit_code = cli_main.main(
                ["dockstore", "fetch", "--url", "https://dockstore/workflows/x", "--output", "json"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_docker_build_dispatches_to_existing_handler(self):
        with patch("bioos.cli.build_docker_image.handle", return_value={"TaskID": "1"}) as mocked:
            exit_code = cli_main.main(
                [
                    "docker",
                    "build",
                    "--repo-name",
                    "repo",
                    "--tag",
                    "v1",
                    "--source-path",
                    "Dockerfile",
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_docker_status_dispatches_to_existing_handler(self):
        with patch("bioos.cli.check_build_status.handle", return_value={"Status": "Running"}) as mocked:
            exit_code = cli_main.main(["docker", "status", "--task-id", "task-1", "--output", "json"])

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()

    def test_root_docker_url_dispatches_to_existing_handler(self):
        with patch("bioos.cli.get_docker_image_url.handle", return_value={"image_url": "reg/ns/repo:v1"}) as mocked:
            exit_code = cli_main.main(
                ["docker", "url", "--repo-name", "repo", "--tag", "v1", "--output", "json"]
            )

        self.assertEqual(exit_code, 0)
        mocked.assert_called_once()


if __name__ == "__main__":
    unittest.main()
