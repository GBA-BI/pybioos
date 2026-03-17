import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from bioos.ops import docker_build, dockstore, womtool, workspace_files


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
            ws.files.upload.return_value = True
            ws.files.s3_urls.return_value = ["s3://bioos-wid/__dashboard__.md"]
            with patch("bioos.ops.workspace_files.login_to_bioos"), \
                    patch("bioos.ops.workspace_files.resolve_workspace", return_value=("wid", {})), \
                    patch("bioos.bioos.workspace", return_value=ws):
                result = workspace_files.upload_dashboard_file_to_workspace("ws", str(dashboard), "ak", "sk", "ep")
        self.assertTrue(result["success"])
        self.assertEqual(result["workspace_id"], "wid")


if __name__ == "__main__":
    unittest.main()
