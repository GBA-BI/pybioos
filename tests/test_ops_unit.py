import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from unittest.mock import MagicMock, patch

from requests.exceptions import SSLError

from bioos.ops import docker_build, dockstore, formatters, workspace_files
from bioos.internal.tos import TOSHandler
from bioos.errors import ParameterError
from bioos.resource.files import FileResource
from bioos.resource.usage import UsageResource
from bioos.resource.workflows import Run, WorkflowResource
from bioos.resource.workspaces import Workspace
from bioos.service.BioOsService import BioOsService
from network import config as repository_internal
from network.auth import RepositoryPassportProvider, passport_token_subject
from network.internal.http import RepositoryRestClient
from network.resource.data_libraries import DataLibrary, DataLibraryResource
from network.resource.datasets import DataSet, DataSetResource
from network.resource.drs import DRSResource
from network.resource.network import NetworkResource
from network.resource.repository import RepositoryResource


class TestOpsHelpers(unittest.TestCase):
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

    def test_workflow_import_rejects_git_url_by_default(self):
        workflows = WorkflowResource("workspace-id")

        with patch.object(BioOsService, "check_workflow") as check_mock, \
                self.assertRaisesRegex(ParameterError, "Git URL workflow import is currently disabled"):
            workflows.import_workflow(
                source="https://github.com/example/workflow.git",
                name="wf",
                description="desc",
            )

        check_mock.assert_not_called()

    def test_workflow_import_directory_preserves_nested_main_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir)
            workflows_dir = source / "workflows"
            tasks_dir = source / "tasks"
            workflows_dir.mkdir()
            tasks_dir.mkdir()
            (workflows_dir / "main.wdl").write_text(
                'version 1.0\nimport "../tasks/echo.wdl" as echo_tasks\nworkflow wf {}\n'
            )
            (tasks_dir / "echo.wdl").write_text("version 1.0\ntask echo_name { command <<< echo hi >>> }\n")

            cases = [
                ("absolute", str(workflows_dir / "main.wdl")),
                ("relative", "workflows/main.wdl"),
            ]
            for label, main_path in cases:
                with self.subTest(label=label), \
                        patch("bioos.resource.workflows.Config.service") as service_mock:
                    service = service_mock.return_value
                    service.check_workflow.return_value = {"IsNameExist": False}
                    service.create_workflow.return_value = "wf-id"

                    result = WorkflowResource(f"workspace-{label}").import_workflow(
                        source=str(source),
                        name=f"wf-{label}",
                        description="nested main path",
                        main_workflow_path=main_path,
                    )

                    self.assertEqual(result, "wf-id")
                    params = service.create_workflow.call_args.args[0]
                    self.assertEqual(params["SourceType"], "file")
                    self.assertEqual(params["MainWorkflowPath"], "workflows/main.wdl")

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

    def test_dataframe_records_converts_missing_values_to_none(self):
        import pandas as pd

        df = pd.DataFrame.from_records([{
            "Name": "ies",
            "Description": float("nan"),
            "Status": {
                "State": "Running",
                "Message": float("nan"),
            },
        }])

        self.assertEqual(formatters.dataframe_records(df), [{
            "Name": "ies",
            "Description": None,
            "Status": {
                "State": "Running",
                "Message": None,
            },
        }])

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
            ws.files.tos_handler.build_upload_plan.return_value = [{
                "source": str(dashboard),
                "key": "__dashboard__.md",
                "from_directory": False,
            }]
            ws.files.tos_handler.upload_planned_objects.return_value = []
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
            ws.files.tos_handler.build_upload_plan.return_value = [
                {
                    "source": str(local_a),
                    "key": "input_provision/a.txt",
                    "from_directory": False,
                },
                {
                    "source": str(local_b),
                    "key": "input_provision/b.txt",
                    "from_directory": False,
                },
            ]
            ws.files.tos_handler.upload_planned_objects.return_value = []

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
        ws.files.tos_handler.upload_planned_objects.assert_called_once_with(
            upload_plan=[{
                "source": str(local_b),
                "key": "input_provision/b.txt",
                "s3_url": "s3://bioos-wid/input_provision/b.txt",
            }],
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
            ws.files.tos_handler.build_upload_plan.return_value = [{
                "source": str(local_a),
                "key": "input_provision/a.txt",
                "from_directory": False,
            }]
            ws.files.tos_handler.upload_planned_objects.return_value = []

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
        ws.files.tos_handler.upload_planned_objects.assert_called_once()

    def test_upload_local_directory_with_workspace_preserves_structure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "data"
            nested = root / "nested"
            nested.mkdir(parents=True)
            local_a = root / "a.txt"
            local_b = nested / "b.txt"
            local_a.write_text("a", encoding="utf-8")
            local_b.write_text("b", encoding="utf-8")

            ws = MagicMock()
            ws.files.s3_urls.side_effect = lambda keys: [f"s3://bioos-wid/{keys[0]}"]
            ws.files.tos_handler = TOSHandler(client=MagicMock(), bucket="bucket")
            ws.files.tos_handler.upload_planned_objects = MagicMock(return_value=[])

            result = workspace_files._upload_local_files_with_workspace(
                ws=ws,
                workspace_id="wid",
                workspace_name="ws",
                sources=[str(root)],
                target="input_provision/",
                flatten=False,
                skip_existing=False,
                checkpoint_dir=str(Path(tmpdir) / "checkpoints"),
                max_retries=2,
                task_num=4,
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["uploaded_count"], 2)
        uploaded_keys = [item["key"] for item in result["uploaded_files"]]
        self.assertEqual(uploaded_keys, [
            "input_provision/data/a.txt",
            "input_provision/data/nested/b.txt",
        ])
        ws.files.tos_handler.upload_planned_objects.assert_called_once()

    def test_upload_local_directory_flatten_detects_conflicting_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "data"
            nested_a = root / "one"
            nested_b = root / "two"
            nested_a.mkdir(parents=True)
            nested_b.mkdir(parents=True)
            (nested_a / "sample.txt").write_text("a", encoding="utf-8")
            (nested_b / "sample.txt").write_text("b", encoding="utf-8")

            ws = MagicMock()
            ws.files.tos_handler = TOSHandler(client=MagicMock(), bucket="bucket")

            with self.assertRaisesRegex(
                ValueError,
                "Multiple local files map to target key 'input_provision/sample.txt'",
            ):
                workspace_files._upload_local_files_with_workspace(
                    ws=ws,
                    workspace_id="wid",
                    workspace_name="ws",
                    sources=[str(root)],
                    target="input_provision/",
                    flatten=True,
                    skip_existing=False,
                )

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

    def test_file_resource_upload_accepts_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "data"
            root.mkdir()
            local_file = root / "a.txt"
            local_file.write_text("a", encoding="utf-8")

            resource = FileResource.__new__(FileResource)
            resource.tos_handler = TOSHandler(client=MagicMock(), bucket="bucket")
            resource.tos_handler.upload_planned_objects = MagicMock(return_value=[])

            success = resource.upload(
                sources=str(root),
                target="input_provision/",
                flatten=False,
            )

        self.assertTrue(success)
        resource.tos_handler.upload_planned_objects.assert_called_once_with(
            upload_plan=[{
                "source": str(local_file),
                "key": "input_provision/data/a.txt",
                "from_directory": True,
            }],
            checkpoint_dir="",
            max_retries=3,
            task_num=10,
        )

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

    def test_repository_passport_provider_caches_token(self):
        service = MagicMock()
        service.get_repository_passport.return_value = {"AAIPassport": "passport-1", "ExpiresIn": 3600}
        login_info = SimpleNamespace(endpoint="https://bioos", region="cn-north-1", access_key="ak")
        provider = RepositoryPassportProvider(expires_in=3600, refresh_margin=60)

        with patch("network.auth.Config.service", return_value=service), \
                patch("network.auth.Config.login_info", return_value=login_info):
            self.assertEqual(provider.get_token(), "passport-1")
            self.assertEqual(provider.get_token(), "passport-1")

        service.get_repository_passport.assert_called_once_with({"ExpiresIn": 3600})

    def test_bioos_service_registers_search_drs(self):
        service = BioOsService.__new__(BioOsService)
        params = {
            "RepositoryEndpoint": "https://network.example",
            "DRSPath": "drs://drs.example/object-1",
            "AAIPassport": "passport-1",
        }

        self.assertIn("SearchDRS", BioOsService.get_api_info())
        with patch.object(BioOsService, "_BioOsService__request", return_value={"DataSetID": "ds1"}) as request_mock:
            result = service.search_drs(params)

        self.assertEqual(result, {"DataSetID": "ds1"})
        request_mock.assert_called_once_with("SearchDRS", params)

    def test_bioos_service_registers_delete_workspace(self):
        service = BioOsService.__new__(BioOsService)
        params = {"ID": "wid"}

        self.assertIn("DeleteWorkspace", BioOsService.get_api_info())
        with patch.object(BioOsService, "_BioOsService__request", return_value={}) as request_mock:
            result = service.delete_workspace(params)

        self.assertEqual(result, {})
        request_mock.assert_called_once_with("DeleteWorkspace", params)

    def test_repository_passport_provider_requires_token(self):
        service = MagicMock()
        service.get_repository_passport.return_value = {"ExpiresIn": 3600}
        login_info = SimpleNamespace(endpoint="https://bioos", region="cn-north-1", access_key="ak")
        provider = RepositoryPassportProvider()

        with patch("network.auth.Config.service", return_value=service), \
                patch("network.auth.Config.login_info", return_value=login_info):
            with self.assertRaisesRegex(RuntimeError, "did not return a passport token"):
                provider.get_token()

    def test_repository_passport_provider_translates_backend_error(self):
        service = MagicMock()
        service.get_repository_passport.side_effect = Exception(
            b'{"ResponseMetadata":{"RequestId":"req-1","Action":"GetRepositoryPassport",'
            b'"Error":{"Code":"InternalError","Message":"unknown error"}}}'
        )
        login_info = SimpleNamespace(endpoint="https://bioos", region="cn-north-1", access_key="ak")
        provider = RepositoryPassportProvider()

        with patch("network.auth.Config.service", return_value=service), \
                patch("network.auth.Config.login_info", return_value=login_info):
            with self.assertRaisesRegex(RuntimeError, "current BioOS account is associated with a BioOS Network account"):
                provider.get_token()

    def test_passport_token_subject_reads_jwt_subject(self):
        token = "e30.eyJzdWIiOiJ1c2VyLTEifQ.signature"

        self.assertEqual(passport_token_subject(token), "user-1")

    def test_repository_endpoint_defaults_are_network_endpoints(self):
        with patch.dict("os.environ", {}, clear=True), \
                patch("network.config._load_client_config", return_value={}):
            self.assertEqual(
                repository_internal.resolve_repository_endpoint(),
                "https://network.miracle.ac.cn",
            )

    def test_repository_rest_client_sends_bearer_and_repeated_query_params(self):
        response = MagicMock()
        response.content = b'{"ok": true}'
        response.json.return_value = {"ok": True}
        session = MagicMock()
        session.request.return_value = response
        provider = MagicMock()
        provider.get_token.return_value = "passport-token"
        login_info = SimpleNamespace(
            endpoint="https://bioos",
            region="cn-north-1",
            access_key="ak",
            secret_key="sk",
        )
        client = RepositoryRestClient(
            endpoint="https://repo.example/base",
            passport_provider=provider,
            sign_requests=True,
            session=session,
        )

        with patch("network.internal.http.Config.login_info", return_value=login_info):
            result = client.get("/api/repository/data_set", params={"id": ["ds1", "ds2"], "page": 1})

        self.assertEqual(result, {"ok": True})
        _, url = session.request.call_args.args
        headers = session.request.call_args.kwargs["headers"]
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["id"], ["ds1", "ds2"])
        self.assertEqual(query["page"], ["1"])
        self.assertIn("X-Signature", query)
        self.assertEqual(headers["Authorization"], "Bearer passport-token")

    def test_repository_rest_client_download_url_writes_stream(self):
        response = MagicMock()
        response.iter_content.return_value = [b"ab", b"", b"cd"]
        session = MagicMock()
        session.request.return_value = response
        client = RepositoryRestClient(
            endpoint="https://repo.example",
            passport_provider=MagicMock(),
            sign_requests=False,
            session=session,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "file.txt"
            result = client.download_url(
                "https://download.example/file",
                str(target),
                headers={"X-Test": "yes"},
                chunk_size=2,
            )
            downloaded_text = Path(result["target"]).read_text(encoding="utf-8")

        self.assertEqual(result["bytes_written"], 4)
        self.assertEqual(downloaded_text, "abcd")
        session.request.assert_called_once_with(
            "GET",
            "https://download.example/file",
            headers={"X-Test": "yes"},
            stream=True,
            timeout=60,
        )

    def test_dataset_resource_list_maps_params_and_returns_dataframe(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.repository_client = MagicMock()
        resource.repository_client.get.return_value = {"Items": [{"id": "ds1", "name": "dataset"}]}

        result = resource.list(
            ids=["ds1"],
            order_by="createTime:desc",
            search_word="rna",
            project_data_type=["omics"],
        )

        self.assertEqual(result.to_dict(orient="records"), [{"id": "ds1", "name": "dataset"}])
        resource.repository_client.get.assert_called_once_with(
            "/api/repository/data_set",
            params={
                "orderBy": "createTime:desc",
                "searchWord": "rna",
                "id": ["ds1"],
                "projectDataType": ["omics"],
            },
        )

    def test_dataset_resource_raw_returns_payload(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.repository_client = MagicMock()
        resource.repository_client.get.return_value = {"Items": [{"id": "ds1"}], "Total": 1}

        result = resource.list(raw=True)

        self.assertEqual(result, {"Items": [{"id": "ds1"}], "Total": 1})

    def test_dataset_resource_get_uses_list_filter_and_returns_record(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.repository_client = MagicMock()
        resource.repository_client.get.return_value = {
            "items": [{"id": "ds1", "name": "dataset"}],
            "total": 1,
        }

        result = resource.get("ds1")

        self.assertEqual(result, {"id": "ds1", "name": "dataset"})
        resource.repository_client.get.assert_called_once_with(
            "/api/repository/data_set",
            params={"id": ["ds1"], "displayLevel": "Full"},
        )

    def test_dataset_files_maps_params_and_requires_library(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.repository_client = MagicMock()
        resource.repository_client.get.return_value = {"Items": [{"id": "file1"}]}
        dataset = DataSet.__new__(DataSet)
        dataset.data_set_id = "ds/1"
        dataset.data_library_id = "lib1"
        dataset.resource = resource

        result = dataset.files(ids=["file1"], search_scope=["name"], file_type=["bam"])

        self.assertEqual(result.to_dict(orient="records"), [{"id": "file1"}])
        resource.repository_client.get.assert_called_once_with(
            "/api/repository/data_set/ds%2F1/data_file",
            params={
                "dataLibraryID": "lib1",
                "searchScope": ["name"],
                "id": ["file1"],
                "fileType": ["bam"],
            },
        )

    def test_library_dataset_files_use_data_site_endpoint_without_library_query(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.data_library_id = "lib1"
        resource.data_site_client = MagicMock()
        resource.data_site_client.get.return_value = {"Items": [{"id": "file1"}]}
        resource.repository_client = MagicMock()
        dataset = DataSet.__new__(DataSet)
        dataset.data_set_id = "ds1"
        dataset.data_library_id = "lib1"
        dataset.resource = resource

        result = dataset.files(ids=["file1"], search_scope=["name"])

        self.assertEqual(result.to_dict(orient="records"), [{"id": "file1"}])
        resource.data_site_client.get.assert_called_once_with(
            "/api/data-library/data_set/ds1/data_file",
            params={
                "searchScope": ["name"],
                "id": ["file1"],
            },
        )
        resource.repository_client.get.assert_not_called()

    def test_library_dataset_files_tries_next_data_site_endpoint_before_repository(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.data_library_id = "lib1"
        primary_client = MagicMock()
        primary_client.get.side_effect = SSLError("tls failed")
        secondary_client = MagicMock()
        secondary_client.get.return_value = {"Items": [{"id": "file1"}]}
        resource.data_site_client = primary_client
        resource.data_site_clients = [primary_client, secondary_client]
        resource.repository_client = MagicMock()
        dataset = DataSet.__new__(DataSet)
        dataset.data_set_id = "ds1"
        dataset.data_library_id = "lib1"
        dataset.resource = resource

        result = dataset.files(ids=["file1"], search_scope=["name"])

        self.assertEqual(result.to_dict(orient="records"), [{"id": "file1"}])
        primary_client.get.assert_called_once_with(
            "/api/data-library/data_set/ds1/data_file",
            params={
                "searchScope": ["name"],
                "id": ["file1"],
            },
        )
        secondary_client.get.assert_called_once_with(
            "/api/data-library/data_set/ds1/data_file",
            params={
                "searchScope": ["name"],
                "id": ["file1"],
            },
        )
        resource.repository_client.get.assert_not_called()

    def test_library_dataset_files_falls_back_to_repository_when_data_site_unreachable(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.data_library_id = "lib1"
        resource.data_site_client = MagicMock()
        resource.data_site_client.get.side_effect = SSLError("tls failed")
        resource.repository_client = MagicMock()
        resource.repository_client.get.return_value = {"Items": [{"id": "file1"}]}
        dataset = DataSet.__new__(DataSet)
        dataset.data_set_id = "ds1"
        dataset.data_library_id = "lib1"
        dataset.resource = resource

        result = dataset.files(ids=["file1"], search_scope=["name"])

        self.assertEqual(result.to_dict(orient="records"), [{"id": "file1"}])
        resource.repository_client.get.assert_called_once_with(
            "/api/repository/data_set/ds1/data_file",
            params={
                "searchScope": ["name"],
                "id": ["file1"],
                "dataLibraryID": "lib1",
            },
        )

    def test_dataset_file_ids_maps_params_and_returns_ids(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.repository_client = MagicMock()
        resource.repository_client.get.return_value = {"ids": ["file1", "file2"]}
        dataset = DataSet.__new__(DataSet)
        dataset.data_set_id = "ds1"
        dataset.data_library_id = "lib1"
        dataset.resource = resource

        result = dataset.file_ids(search_scope=["name"], file_type=["fastq"])

        self.assertEqual(result, ["file1", "file2"])
        resource.repository_client.get.assert_called_once_with(
            "/api/repository/data_set/ds1/data_file/ids",
            params={
                "dataLibraryID": "lib1",
                "searchScope": ["name"],
                "fileType": ["fastq"],
            },
        )

    def test_dataset_drs_object_uses_drs_resource(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.drs = MagicMock()
        resource.drs.object.return_value = {"id": "object-1"}

        result = resource.drs_object("object/1")

        self.assertEqual(result, {"id": "object-1"})
        resource.drs.object.assert_called_once_with("object/1")

    def test_dataset_drs_access_uses_drs_resource(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.drs = MagicMock()
        resource.drs.access.return_value = {"url": "https://download"}

        result = resource.drs_access("drs://imc-drs.miracle.ac.cn/object/1", access_id="https")

        self.assertEqual(result, {"url": "https://download"})
        resource.drs.access.assert_called_once_with(
            "drs://imc-drs.miracle.ac.cn/object/1",
            access_id="https",
        )

    def test_dataset_download_drs_object_uses_drs_resource(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.drs = MagicMock()
        resource.drs.download_object.return_value = {
            "success": True,
            "object_id": "object-1",
            "target": "/tmp/object.txt",
            "bytes_written": 4,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            result = resource.download_drs_object(
                "drs://drs.example/object-1",
                target=tmpdir,
                object_info={"name": "object.txt"},
                object_name="object.txt",
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["object_id"], "object-1")
        resource.drs.download_object.assert_called_once_with(
            "drs://drs.example/object-1",
            target=tmpdir,
            access_id="https",
            overwrite=False,
            chunk_size=1024 * 1024,
            object_info={"name": "object.txt"},
            object_name="object.txt",
        )

    def test_dataset_download_files_uses_file_records_and_drs_download(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.repository_client = MagicMock()
        record = {"id": "file1", "name": "reads.fastq.gz", "drsURL": "drs://imc-drs.miracle.ac.cn/file1"}
        resource.repository_client.get.return_value = {"items": [record]}
        resource.download_drs_object = MagicMock(
            return_value={
                "success": True,
                "object_id": "file1",
                "target": "/tmp/reads.fastq.gz",
            }
        )
        dataset = DataSet.__new__(DataSet)
        dataset.data_set_id = "ds1"
        dataset.data_library_id = "lib1"
        dataset.resource = resource

        with tempfile.TemporaryDirectory() as tmpdir:
            result = dataset.download_files(target=tmpdir)
            expected_target = str(Path(tmpdir) / "reads.fastq.gz")

        self.assertTrue(result["success"])
        self.assertEqual(result["downloaded_count"], 1)
        resource.download_drs_object.assert_called_once_with(
            "drs://imc-drs.miracle.ac.cn/file1",
            target=expected_target,
            access_id="https",
            overwrite=False,
            object_info=record,
            object_name="reads.fastq.gz",
        )

    def test_network_resource_exposes_dataset_and_drs_helpers(self):
        network = NetworkResource.__new__(NetworkResource)
        network.repository_endpoint = "https://network.miracle.ac.cn"
        network.passport_provider = MagicMock()

        with patch("network.resource.network.RepositoryResource") as repository_factory, \
                patch("network.resource.network.DRSResource") as drs_factory:
            data_sets = network.datasets
            libraries = network.libraries
            library = network.library("lib1")
            data_set = network.dataset("ds1", data_library_id="lib1")
            drs_result = network.drs_object("drs://drs.example/object-1")
            access_result = network.drs_access("drs://drs.example/object-1", access_id="https")
            locate_result = network.drs_locate("drs://drs.example/object-1")
            download_result = network.download_drs_object(
                "drs://drs.example/object-1",
                target="/tmp/object.txt",
                overwrite=True,
            )

        repository_factory.assert_called_with(
            repository_endpoint="https://network.miracle.ac.cn",
            passport_provider=network.passport_provider,
        )
        self.assertEqual(data_sets, repository_factory.return_value.datasets)
        self.assertEqual(libraries, repository_factory.return_value.libraries)
        repository_factory.return_value.library.assert_called_once_with("lib1")
        repository_factory.return_value.dataset.assert_called_once_with("ds1", data_library_id="lib1")
        drs_factory.assert_called_with(
            repository_endpoint="https://network.miracle.ac.cn",
            passport_provider=network.passport_provider,
        )
        drs_factory.return_value.object.assert_called_once_with("drs://drs.example/object-1")
        drs_factory.return_value.access.assert_called_once_with("drs://drs.example/object-1", access_id="https")
        drs_factory.return_value.locate.assert_called_once_with("drs://drs.example/object-1")
        drs_factory.return_value.download_object.assert_called_once_with(
            "drs://drs.example/object-1",
            target="/tmp/object.txt",
            access_id="https",
            overwrite=True,
        )
        self.assertEqual(library, repository_factory.return_value.library.return_value)
        self.assertEqual(data_set, repository_factory.return_value.dataset.return_value)
        self.assertEqual(drs_result, drs_factory.return_value.object.return_value)
        self.assertEqual(access_result, drs_factory.return_value.access.return_value)
        self.assertEqual(locate_result, drs_factory.return_value.locate.return_value)
        self.assertEqual(download_result, drs_factory.return_value.download_object.return_value)

    def test_network_resources_are_not_singletons(self):
        provider = MagicMock()
        repository_client = MagicMock()
        data_site_client = MagicMock()
        library_resource = DataLibraryResource(
            repository_endpoint="https://network.example",
            repository_client=repository_client,
            passport_provider=provider,
        )
        dataset_resource = DataSetResource(
            repository_endpoint="https://network.example",
            data_library_id="lib1",
            data_site_client=data_site_client,
            repository_client=repository_client,
            passport_provider=provider,
        )

        self.assertIsNot(
            NetworkResource(repository_endpoint="https://network.example", passport_provider=provider),
            NetworkResource(repository_endpoint="https://network.example", passport_provider=provider),
        )
        self.assertIsNot(
            RepositoryResource(repository_endpoint="https://network.example", passport_provider=provider),
            RepositoryResource(repository_endpoint="https://network.example", passport_provider=provider),
        )
        self.assertIsNot(
            DataLibraryResource(
                repository_endpoint="https://network.example",
                repository_client=repository_client,
                passport_provider=provider,
            ),
            DataLibraryResource(
                repository_endpoint="https://network.example",
                repository_client=repository_client,
                passport_provider=provider,
            ),
        )
        self.assertIsNot(
            DataLibrary("lib1", resource=library_resource),
            DataLibrary("lib1", resource=library_resource),
        )
        self.assertIsNot(
            DataSetResource(
                repository_endpoint="https://network.example",
                data_library_id="lib1",
                data_site_client=data_site_client,
                repository_client=repository_client,
                passport_provider=provider,
            ),
            DataSetResource(
                repository_endpoint="https://network.example",
                data_library_id="lib1",
                data_site_client=data_site_client,
                repository_client=repository_client,
                passport_provider=provider,
            ),
        )
        self.assertIsNot(
            DataSet("ds1", data_library_id="lib1", resource=dataset_resource),
            DataSet("ds1", data_library_id="lib1", resource=dataset_resource),
        )
        self.assertIsNot(
            DRSResource(endpoint="https://drs.example", passport_provider=provider),
            DRSResource(endpoint="https://drs.example", passport_provider=provider),
        )

    def test_data_library_resource_list_and_library_context(self):
        resource = DataLibraryResource.__new__(DataLibraryResource)
        resource.repository_endpoint = "https://network.example"
        resource.repository_client = MagicMock()
        resource.repository_client.get.return_value = {
            "items": [
                {
                    "id": "lib1",
                    "APIEndpoint": "https://site.example/api/data-library",
                    "DRSHost": "https://drs.example/ga4gh/drs/v1",
                    "webEndpoint": "https://web.example",
                }
            ]
        }
        resource.passport_provider = MagicMock()

        result = resource.list(ids=["lib1"], display_name=["Guangzhou"])
        library = resource.data_library("lib1", record=result.to_dict(orient="records")[0])
        data_sets = library.datasets

        self.assertEqual(result.to_dict(orient="records")[0]["id"], "lib1")
        resource.repository_client.get.assert_called_once_with(
            "/api/repository/data_library",
            params={"id": ["lib1"], "displayName": ["Guangzhou"]},
        )
        self.assertEqual(library.api_endpoint, "https://site.example/api/data-library")
        self.assertEqual(library.web_endpoint, "https://web.example")
        self.assertEqual(library.drs_host, "https://drs.example/ga4gh/drs/v1")
        self.assertEqual(data_sets.data_library_id, "lib1")
        self.assertEqual(data_sets.data_site_client.endpoint, "https://web.example")
        self.assertEqual(
            [client.endpoint for client in data_sets.data_site_clients],
            ["https://web.example", "https://site.example"],
        )
        self.assertEqual(library.drs.endpoint, "https://drs.example")

    def test_drs_resource_uses_host_from_drs_uri(self):
        drs = DRSResource.__new__(DRSResource)
        drs.endpoint = None
        drs.fallback_endpoint = None
        drs.passport_provider = MagicMock()
        drs._clients = {}

        with patch("network.resource.drs.NetworkHttpClient") as client_factory:
            client_factory.return_value.get.return_value = {"id": "object/1"}
            result = drs.object("drs://drs.example/object/1")

        self.assertEqual(result, {"id": "object/1"})
        client_factory.assert_called_once_with(
            endpoint="http://drs.example",
            passport_provider=drs.passport_provider,
            sign_requests=False,
        )
        client_factory.return_value.get.assert_called_once_with("/ga4gh/drs/v1/objects/object%2F1")

    def test_drs_resource_locate_uses_search_drs_bridge(self):
        drs = DRSResource.__new__(DRSResource)
        drs.repository_endpoint = "https://network.example"
        drs.passport_provider = MagicMock()
        drs.passport_provider.get_token.return_value = "passport-1"

        with patch("network.resource.drs.Config.service") as service_mock:
            service_mock.return_value.search_drs.return_value = {
                "DataSetID": "ds1",
                "WebEndpoint": "https://library.example",
            }
            result = drs.locate("drs://drs.example/object-1")

        self.assertEqual(
            result,
            {
                "DataSetID": "ds1",
                "WebEndpoint": "https://library.example",
            },
        )
        service_mock.return_value.search_drs.assert_called_once_with(
            {
                "RepositoryEndpoint": "https://network.example",
                "DRSPath": "drs://drs.example/object-1",
                "AAIPassport": "passport-1",
            }
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

    def test_bioos_service_registers_get_task_metric_data(self):
        api_info = BioOsService.get_api_info()
        self.assertIn("GetTaskMetricData", api_info)

    def test_run_list_runs_builds_request(self):
        response = {"Items": []}

        with patch("bioos.resource.workflows.Config.service") as service_mock:
            service_mock.return_value.list_runs.return_value = response
            result = Run.list_runs(
                workspace_id="wid",
                submission_id="sid",
                page_number=2,
                page_size=50,
                filter_={"Status": ["Running"]},
            )

        self.assertEqual(result, response)
        service_mock.return_value.list_runs.assert_called_once_with(
            {
                "SubmissionID": "sid",
                "WorkspaceID": "wid",
                "PageNumber": 2,
                "PageSize": 50,
                "Filter": {"Status": ["Running"]},
            }
        )

    def test_run_list_tasks_builds_request(self):
        response = {"Items": []}

        with patch("bioos.resource.workflows.Config.service") as service_mock:
            service_mock.return_value.list_tasks.return_value = response
            result = Run.list_tasks(
                workspace_id="wid",
                run_id="rid",
                page_number=1,
                page_size=0,
            )

        self.assertEqual(result, response)
        service_mock.return_value.list_tasks.assert_called_once_with(
            {
                "RunID": "rid",
                "WorkspaceID": "wid",
                "PageNumber": 1,
                "PageSize": 0,
            }
        )

    def test_run_get_task_metric_data_builds_request(self):
        run = Run.__new__(Run)
        run.workspace_id = "wid"
        run.id = "rid"
        response = {
            "Status": "Running",
            "DataPointsCPUUsage": [{"Timestamp": 1, "Value": 0.5}],
        }

        with patch("bioos.resource.workflows.Config.service") as service_mock:
            service_mock.return_value.get_task_metric_data.return_value = response
            result = run.get_task_metric_data(
                name=" task.name ",
                period="60s",
                start_time="100",
                end_time=200,
                top={"RequestId": "req-1"},
            )

        self.assertEqual(result, response)
        service_mock.return_value.get_task_metric_data.assert_called_once_with(
            {
                "Name": "task.name",
                "RunID": "rid",
                "Period": "60s",
                "StartTime": 100,
                "EndTime": 200,
                "WorkspaceID": "wid",
                "Top": {"RequestId": "req-1"},
            }
        )

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

    def test_workspace_delete_calls_service(self):
        workspace = Workspace.__new__(Workspace)
        workspace._id = "wid"

        with patch("bioos.resource.workspaces.Config.service") as service_mock:
            service_mock.return_value.list_workspaces.return_value = {"Items": [{"ID": "wid"}]}
            service_mock.return_value.delete_workspace.return_value = {}
            result = workspace.delete()

        self.assertEqual(result, {})
        service_mock.return_value.list_workspaces.assert_called_once_with(
            {"Filter": {"IDs": ["wid"]}}
        )
        service_mock.return_value.delete_workspace.assert_called_once_with({"ID": "wid"})

    def test_workspace_delete_resolves_workspace_name(self):
        workspace = Workspace.__new__(Workspace)
        workspace._id = "test1"

        with patch("bioos.resource.workspaces.Config.service") as service_mock:
            service_mock.return_value.list_workspaces.side_effect = [
                {"Items": []},
                {"Items": [{"ID": "wid", "Name": "test1"}, {"ID": "other", "Name": "other"}]},
            ]
            service_mock.return_value.delete_workspace.return_value = {}
            result = workspace.delete()

        self.assertEqual(result, {})
        service_mock.return_value.list_workspaces.assert_has_calls(
            [
                unittest.mock.call({"Filter": {"IDs": ["test1"]}}),
                unittest.mock.call({"PageSize": 0}),
            ]
        )
        service_mock.return_value.delete_workspace.assert_called_once_with({"ID": "wid"})

    def test_workspace_delete_rejects_duplicate_workspace_name(self):
        workspace = Workspace.__new__(Workspace)
        workspace._id = "test1"

        with patch("bioos.resource.workspaces.Config.service") as service_mock:
            service_mock.return_value.list_workspaces.side_effect = [
                {"Items": []},
                {"Items": [{"ID": "wid1", "Name": "test1"}, {"ID": "wid2", "Name": "test1"}]},
            ]
            with self.assertRaisesRegex(ValueError, "Multiple workspaces"):
                workspace.delete()

        service_mock.return_value.delete_workspace.assert_not_called()

    def test_workspace_delete_rejects_missing_workspace(self):
        workspace = Workspace.__new__(Workspace)
        workspace._id = "missing"

        with patch("bioos.resource.workspaces.Config.service") as service_mock:
            service_mock.return_value.list_workspaces.side_effect = [
                {"Items": []},
                {"Items": []},
            ]
            with self.assertRaisesRegex(ValueError, "Workspace not found"):
                workspace.delete()

        service_mock.return_value.delete_workspace.assert_not_called()


if __name__ == "__main__":
    unittest.main()
