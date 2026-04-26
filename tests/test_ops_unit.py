import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from bioos.ops import docker_build, dockstore, womtool, workspace_files
from bioos.internal.tos import TOSHandler
from bioos.resource.files import FileResource
from bioos.resource.usage import UsageResource
from bioos.resource.workspaces import Workspace


class TestOpsHelpers(unittest.TestCase):
    def test_validate_wdl_file(self):
        with tempfile.NamedTemporaryFile(suffix=".wdl") as handle, \
                patch("bioos.ops.womtool.subprocess.run") as run_mock:
            run_mock.return_value = MagicMock(stdout="ok", stderr="")
            result = womtool.validate_wdl_file(handle.name)
        self.assertTrue(result["success"])
        run_mock.assert_called_once()

    def test_validate_workflow_input_json_file(self):
        with tempfile.NamedTemporaryFile(suffix=".wdl") as wdl, tempfile.NamedTemporaryFile(suffix=".json") as inputs, \
                patch("bioos.ops.womtool.subprocess.run") as run_mock:
            run_mock.return_value = MagicMock(stdout="ok", stderr="")
            result = womtool.validate_workflow_input_json_file(wdl.name, inputs.name)
        self.assertTrue(result["success"])
        run_mock.assert_called_once()

    def test_get_docker_image_url(self):
        self.assertEqual(
            docker_build.get_docker_image_url("reg", "ns", "repo", "v1"),
            "reg/ns/repo:v1",
        )

    def test_build_docker_image_request(self):
        with tempfile.NamedTemporaryFile() as handle, \
                patch("bioos.ops.docker_build.requests.post") as post_mock:
            post_mock.return_value = MagicMock(
                raise_for_status=MagicMock(),
                json=MagicMock(return_value={"TaskID": "123"}),
            )
            result = docker_build.build_docker_image_request("repo", "v1", handle.name, "reg", "ns")
        self.assertEqual(result["ImageURL"], "reg/ns/repo:v1")
        post_mock.assert_called_once()

    def test_check_build_status_request(self):
        with patch("bioos.ops.docker_build.requests.get") as get_mock:
            get_mock.return_value = MagicMock(
                raise_for_status=MagicMock(),
                json=MagicMock(return_value={"Status": "Running"}),
            )
            result = docker_build.check_build_status_request("task-1")
        self.assertEqual(result, {"Status": "Running"})

    def test_parse_workflow_url(self):
        org, workflow = dockstore.parse_workflow_url(
            "https://dockstore.miracle.ac.cn/workflows/git.miracle.ac.cn/gzlab/mrnaseq/mRNAseq"
        )
        self.assertEqual((org, workflow), ("gzlab", "mRNAseq"))

    def test_search_dockstore_workflows(self):
        payload = {
            "hits": {
                "hits": [
                    {
                        "_score": 7.0,
                        "_source": {
                            "workflowName": "RNASeq",
                            "full_workflow_path": "git.miracle.ac.cn/gzlab/mrnaseq/mRNAseq",
                            "description": "RNA workflow",
                            "descriptorType": "WDL",
                        },
                    }
                ]
            }
        }
        with patch("bioos.ops.dockstore.requests.post") as post_mock:
            post_mock.return_value = MagicMock(
                raise_for_status=MagicMock(),
                json=MagicMock(return_value=payload),
            )
            result = dockstore.search_dockstore_workflows(
                query=[["description", "AND", "RNA"]],
                top_n=1,
                query_type="match_phrase",
                sentence=False,
                output_full=False,
            )
        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 1)

    def test_fetch_wdl_from_dockstore_url(self):
        with tempfile.TemporaryDirectory() as tmpdir, \
                patch("bioos.ops.dockstore._get_published_workflows", return_value=[{
                    "id": 1,
                    "workflowName": "mRNAseq",
                    "full_workflow_path": "git.miracle.ac.cn/gzlab/mrnaseq/mRNAseq",
                    "workflowVersions": [{"id": 2, "name": "main", "valid": True}],
                    "descriptorType": "WDL",
                }]), \
                patch("bioos.ops.dockstore._get_source_files", return_value=[{
                    "path": "/main.wdl",
                    "content": "workflow test {}",
                }]):
            result = dockstore.fetch_wdl_from_dockstore_url(
                "https://dockstore.miracle.ac.cn/workflows/git.miracle.ac.cn/gzlab/mrnaseq/mRNAseq",
                tmpdir,
            )
            self.assertTrue(result["success"])
            self.assertTrue(result["files"][0].endswith("main.wdl"))
            metadata = json.loads(Path(result["metadata_path"]).read_text(encoding="utf-8"))
            self.assertEqual(metadata["workflow_name"], "mRNAseq")

    def test_upload_dashboard_file_to_workspace(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dashboard = Path(tmpdir) / "__dashboard__.md"
            dashboard.write_text("# dashboard", encoding="utf-8")
            ws = MagicMock()
            ws.files.s3_urls.return_value = ["s3://bioos-wid/__dashboard__.md"]
            ws.files.tos_handler.object_exists.return_value = False
            ws.files.tos_handler.upload_objects.return_value = []
            with patch("bioos.ops.workspace_files.login_to_bioos"), \
                    patch("bioos.ops.workspace_files.resolve_workspace", return_value=("wid", {})), \
                    patch("bioos.bioos.workspace", return_value=ws):
                result = workspace_files.upload_dashboard_file_to_workspace("ws", str(dashboard), "ak", "sk", "ep")
        self.assertTrue(result["success"])
        self.assertEqual(result["workspace_id"], "wid")

    def test_upload_local_files_to_workspace(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            local_a = Path(tmpdir) / "a.txt"
            local_b = Path(tmpdir) / "b.txt"
            local_a.write_text("a", encoding="utf-8")
            local_b.write_text("b", encoding="utf-8")
            ws = MagicMock()
            ws.files.s3_urls.side_effect = lambda keys: [f"s3://bioos-wid/{keys[0]}"]
            ws.files.tos_handler.object_exists.side_effect = [True, False]
            ws.files.tos_handler.upload_objects.return_value = []

            with patch("bioos.ops.workspace_files.login_to_bioos"), \
                    patch("bioos.ops.workspace_files.resolve_workspace", return_value=("wid", {})), \
                    patch("bioos.bioos.workspace", return_value=ws):
                result = workspace_files.upload_local_files_to_workspace(
                    workspace_name="ws",
                    sources=[str(local_a), str(local_b)],
                    target="input_provision/",
                    flatten=True,
                    skip_existing=True,
                    checkpoint_dir=str(Path(tmpdir) / "checkpoints"),
                    max_retries=5,
                    task_num=8,
                    access_key="ak",
                    secret_key="sk",
                    endpoint="ep",
                )

        self.assertTrue(result["success"])
        self.assertEqual(result["workspace_id"], "wid")
        self.assertEqual(result["uploaded_count"], 1)
        self.assertEqual(result["skipped_count"], 1)
        ws.files.tos_handler.upload_objects.assert_called_once_with(
            files_to_upload=[str(local_b)],
            target_path="input_provision/",
            flatten=True,
            checkpoint_dir=str(Path(tmpdir) / "checkpoints"),
            max_retries=5,
            task_num=8,
        )

    def test_upload_local_files_with_workspace(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            local_a = Path(tmpdir) / "a.txt"
            local_a.write_text("a", encoding="utf-8")
            ws = MagicMock()
            ws.files.s3_urls.side_effect = lambda keys: [f"s3://bioos-wid/{keys[0]}"]
            ws.files.tos_handler.object_exists.return_value = False
            ws.files.tos_handler.upload_objects.return_value = []

            result = workspace_files._upload_local_files_with_workspace(
                ws=ws,
                workspace_id="wid",
                workspace_name="ws",
                sources=[str(local_a)],
                target="input_provision/",
                flatten=True,
                skip_existing=False,
                checkpoint_dir=str(Path(tmpdir) / "checkpoints"),
                max_retries=2,
                task_num=4,
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["uploaded_count"], 1)
        ws.files.tos_handler.upload_objects.assert_called_once()

    def test_tos_handler_upload_objects_uses_checkpoint_and_retries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            local_file = Path(tmpdir) / "large.bin"
            local_file.write_text("placeholder", encoding="utf-8")
            checkpoint_dir = Path(tmpdir) / "ckpt"
            client = MagicMock()
            client.upload_file.side_effect = [Exception("temporary"), None]
            handler = TOSHandler(client=client, bucket="bucket")

            with patch("bioos.internal.tos.os.path.getsize", return_value=1024 * 1024 * 101):
                failed = handler.upload_objects(
                    [str(local_file)],
                    "target",
                    flatten=True,
                    checkpoint_dir=str(checkpoint_dir),
                    max_retries=2,
                    task_num=4,
                )

        self.assertEqual(failed, [])
        self.assertEqual(client.upload_file.call_count, 2)
        first_call = client.upload_file.call_args_list[0].kwargs
        self.assertEqual(first_call["bucket"], "bucket")
        self.assertEqual(first_call["key"], "target/large.bin")
        self.assertEqual(first_call["task_num"], 4)
        self.assertTrue(first_call["enable_checkpoint"])
        self.assertTrue(first_call["checkpoint_file"].endswith(".upload.ckpt"))

    def test_file_resource_download_accepts_workspace_s3_url(self):
        resource = FileResource.__new__(FileResource)
        resource.bucket = "bioos-wid"
        resource.tos_handler = MagicMock()
        resource.tos_handler.download_objects.return_value = []

        success = resource.download(
            sources="s3://bioos-wid/input_provision/a.txt",
            target="/tmp/out",
            flatten=False,
        )

        self.assertTrue(success)
        resource.tos_handler.download_objects.assert_called_once_with(
            ["input_provision/a.txt"],
            "/tmp/out",
            False,
        )

    def test_file_resource_download_rejects_other_workspace_s3_url(self):
        resource = FileResource.__new__(FileResource)
        resource.bucket = "bioos-wid"
        resource.tos_handler = MagicMock()

        with self.assertRaisesRegex(
            ValueError,
            "S3 URL bucket mismatch: expected bioos-wid, got bioos-other",
        ):
            resource.download(
                sources="s3://bioos-other/input_provision/a.txt",
                target="/tmp/out",
                flatten=False,
            )

    def test_file_resource_download_accepts_internal_key(self):
        resource = FileResource.__new__(FileResource)
        resource.bucket = "bioos-wid"
        resource.tos_handler = MagicMock()
        resource.tos_handler.download_objects.return_value = []

        success = resource.download(
            sources="input_provision/a.txt",
            target="/tmp/out",
            flatten=True,
        )

        self.assertTrue(success)
        resource.tos_handler.download_objects.assert_called_once_with(
            ["input_provision/a.txt"],
            "/tmp/out",
            True,
        )

    def test_usage_resource_rejects_invalid_asset_type(self):
        usage = UsageResource.__new__(UsageResource)
        with self.assertRaisesRegex(ValueError, "Invalid asset usage type"):
            usage.get_asset_usage_data(1, 2, "BadType")

    def test_usage_resource_rejects_invalid_resource_type(self):
        usage = UsageResource.__new__(UsageResource)
        with self.assertRaisesRegex(ValueError, "Invalid resource usage type"):
            usage.get_resource_usage_data(1, 2, "BadType")

    def test_usage_resource_translates_account_owner_permission_error(self):
        usage = UsageResource.__new__(UsageResource)
        with patch("bioos.resource.usage.Config.service") as service_mock:
            service_mock.return_value.list_user_resource_usage.side_effect = Exception(
                "b'{\"ResponseMetadata\":{\"Error\":{\"Code\":\"ForbiddenErr\",\"Message\":\"only account owner can access\"}}}'"
            )
            with self.assertRaisesRegex(
                PermissionError,
                "This usage API is only available to the account owner",
            ):
                usage.list_user_resource_usage(1, 2)

    def test_workspace_list_members_defaults_to_in_workspace_filter(self):
        workspace = Workspace.__new__(Workspace)
        workspace._id = "wid"

        with patch("bioos.resource.workspaces.Config.service") as service_mock:
            service_mock.return_value.list_members.return_value = {"Items": []}
            result = workspace.list_members()

        self.assertEqual(result, {"Items": []})
        service_mock.return_value.list_members.assert_called_once_with(
            {
                "WorkspaceID": "wid",
                "Filter": {"InWorkspace": True},
            }
        )

    def test_workspace_list_members_supports_pagination(self):
        workspace = Workspace.__new__(Workspace)
        workspace._id = "wid"

        with patch("bioos.resource.workspaces.Config.service") as service_mock:
            service_mock.return_value.list_members.return_value = {"Items": []}
            result = workspace.list_members(page_number=2, page_size=50)

        self.assertEqual(result, {"Items": []})
        service_mock.return_value.list_members.assert_called_once_with(
            {
                "WorkspaceID": "wid",
                "Filter": {"InWorkspace": True},
                "PageNumber": 2,
                "PageSize": 50,
            }
        )

    def test_workspace_list_members_supports_filters(self):
        workspace = Workspace.__new__(Workspace)
        workspace._id = "wid"

        with patch("bioos.resource.workspaces.Config.service") as service_mock:
            service_mock.return_value.list_members.return_value = {"Items": []}
            result = workspace.list_members(
                in_workspace=False,
                roles=["Admin", "User"],
                keyword="alice",
            )

        self.assertEqual(result, {"Items": []})
        service_mock.return_value.list_members.assert_called_once_with(
            {
                "WorkspaceID": "wid",
                "Filter": {
                    "InWorkspace": False,
                    "Roles": ["Admin", "User"],
                    "Keyword": "alice",
                },
            }
        )


if __name__ == "__main__":
    unittest.main()
