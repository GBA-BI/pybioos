import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from unittest.mock import MagicMock, patch

from bioos.internal.repository import RepositoryPassportProvider, RepositoryRestClient
from bioos.internal import repository as repository_internal
from bioos.ops import docker_build, dockstore, womtool, workspace_files
from bioos.internal.tos import TOSHandler
from bioos.resource.files import FileResource
from bioos.resource.datasets import DataSet, DataSetResource
from bioos.resource.network import NetworkResource
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

    def test_repository_passport_provider_caches_token(self):
        service = MagicMock()
        service.get_repository_passport.return_value = {"AAIPassport": "passport-1", "ExpiresIn": 3600}
        login_info = SimpleNamespace(endpoint="https://bioos", region="cn-north-1", access_key="ak")
        provider = RepositoryPassportProvider(expires_in=3600, refresh_margin=60)

        with patch("bioos.internal.repository.Config.service", return_value=service), \
                patch("bioos.internal.repository.Config.login_info", return_value=login_info):
            self.assertEqual(provider.get_token(), "passport-1")
            self.assertEqual(provider.get_token(), "passport-1")

        service.get_repository_passport.assert_called_once_with({"ExpiresIn": 3600})

    def test_repository_passport_provider_requires_token(self):
        service = MagicMock()
        service.get_repository_passport.return_value = {"ExpiresIn": 3600}
        login_info = SimpleNamespace(endpoint="https://bioos", region="cn-north-1", access_key="ak")
        provider = RepositoryPassportProvider()

        with patch("bioos.internal.repository.Config.service", return_value=service), \
                patch("bioos.internal.repository.Config.login_info", return_value=login_info):
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

        with patch("bioos.internal.repository.Config.service", return_value=service), \
                patch("bioos.internal.repository.Config.login_info", return_value=login_info):
            with self.assertRaisesRegex(RuntimeError, "current BioOS account is associated with a BioOS Network account"):
                provider.get_token()

    def test_repository_endpoint_defaults_are_network_endpoints(self):
        with patch.dict("os.environ", {}, clear=True), \
                patch("bioos.internal.repository._load_client_config", return_value={}):
            self.assertEqual(
                repository_internal.resolve_repository_endpoint(),
                "https://network.miracle.ac.cn",
            )
            self.assertEqual(
                repository_internal.resolve_drs_endpoint(),
                "http://imc-drs.miracle.ac.cn",
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

        with patch("bioos.internal.repository.Config.login_info", return_value=login_info):
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

    def test_dataset_drs_object_uses_drs_client(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.drs_client = MagicMock()
        resource.drs_client.get.return_value = {"id": "object-1"}

        result = resource.drs_object("object/1")

        self.assertEqual(result, {"id": "object-1"})
        resource.drs_client.get.assert_called_once_with("/ga4gh/drs/v1/objects/object%2F1")

    def test_dataset_drs_access_uses_access_endpoint(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.drs_client = MagicMock()
        resource.drs_client.get.side_effect = [
            {"id": "object-1", "access_methods": [{"type": "https", "access_id": "https"}]},
            {"url": "https://download"},
        ]

        result = resource.drs_access("drs://imc-drs.miracle.ac.cn/object/1", access_id="https")

        self.assertEqual(result, {"url": "https://download"})
        self.assertEqual(resource.drs_client.get.call_count, 2)
        resource.drs_client.get.assert_any_call("/ga4gh/drs/v1/objects/object%2F1")
        resource.drs_client.get.assert_any_call(
            "/ga4gh/drs/v1/objects/object%2F1/access/https"
        )

    def test_dataset_drs_access_uses_object_access_methods_first(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.drs_client = MagicMock()
        resource.drs_client.get.return_value = {
            "id": "object-1",
            "access_methods": [
                {"type": "https", "access_id": "https", "access_url": {"url": "https://download"}},
                {"type": "tos", "access_id": "tos", "access_url": {"url": "s3://bucket/key"}},
            ],
        }

        result = resource.drs_access("object-1", access_id="https")

        self.assertEqual(result, {"url": "https://download"})
        resource.drs_client.get.assert_called_once_with("/ga4gh/drs/v1/objects/object-1")

    def test_dataset_download_drs_object_gets_access_url_and_writes_file(self):
        resource = DataSetResource.__new__(DataSetResource)
        resource.drs_client = MagicMock()
        resource.drs_client.get.return_value = {
            "name": "object.txt",
            "access_methods": [
                {"type": "https", "access_id": "https", "access_url": {"url": "https://download", "headers": ["X-Test: yes"]}},
            ],
        }
        resource.drs_client.download_url.return_value = {
            "target": "/tmp/object.txt",
            "bytes_written": 4,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            result = resource.download_drs_object("object-1", target=tmpdir)

        self.assertTrue(result["success"])
        self.assertEqual(result["object_id"], "object-1")
        resource.drs_client.download_url.assert_called_once()
        self.assertEqual(resource.drs_client.download_url.call_args.args[0], "https://download")
        self.assertEqual(resource.drs_client.download_url.call_args.kwargs["headers"], {"X-Test": "yes"})

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
            "file1",
            target=expected_target,
            access_id="https",
            overwrite=False,
            object_info=record,
            object_name="reads.fastq.gz",
        )

    def test_network_resource_exposes_dataset_and_drs_helpers(self):
        network = NetworkResource.__new__(NetworkResource)
        network.repository_endpoint = "https://network.miracle.ac.cn"
        network.drs_endpoint = "http://imc-drs.miracle.ac.cn"
        network.passport_provider = MagicMock()

        with patch("bioos.resource.network.DataSetResource") as resource_factory:
            data_sets = network.datasets
            data_set = network.dataset("ds1", data_library_id="lib1")
            drs_result = network.drs_object("object-1")
            access_result = network.drs_access("object-1", access_id="https")
            download_result = network.download_drs_object("object-1", target="/tmp/object.txt", overwrite=True)

        self.assertEqual(data_sets, resource_factory.return_value)
        resource_factory.assert_called_with(
            repository_endpoint="https://network.miracle.ac.cn",
            drs_endpoint="http://imc-drs.miracle.ac.cn",
            passport_provider=network.passport_provider,
        )
        resource_factory.return_value.data_set.assert_called_once_with("ds1", data_library_id="lib1")
        resource_factory.return_value.drs_object.assert_called_once_with("object-1")
        resource_factory.return_value.drs_access.assert_called_once_with("object-1", access_id="https")
        resource_factory.return_value.download_drs_object.assert_called_once_with(
            "object-1",
            target="/tmp/object.txt",
            access_id="https",
            overwrite=True,
        )
        self.assertEqual(data_set, resource_factory.return_value.data_set.return_value)
        self.assertEqual(drs_result, resource_factory.return_value.drs_object.return_value)
        self.assertEqual(access_result, resource_factory.return_value.drs_access.return_value)
        self.assertEqual(download_result, resource_factory.return_value.download_drs_object.return_value)

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
