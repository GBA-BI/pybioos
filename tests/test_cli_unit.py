import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from bioos.cli import (
    build_docker_image,
    check_build_status,
    check_ies_status,
    create_iesapp,
    create_workspace_bioos,
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
    list_submissions_from_workspace,
    list_workflows_from_workspace,
    search_dockstore,
    upload_dashboard_file,
    validate_wdl,
)


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


if __name__ == "__main__":
    unittest.main()
