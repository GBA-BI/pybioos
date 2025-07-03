import base64
import os
import zipfile
from datetime import datetime
from io import BytesIO
from typing import List, Dict, Optional, Any

import pandas as pd
from cachetools import TTLCache, cached
from pandas import DataFrame
from typing_extensions import Literal

from bioos.config import Config
from bioos.errors import ConflictError, NotFoundError, ParameterError
from bioos.resource.data_models import DataModelResource
from bioos.utils import workflows
from bioos.utils.common_tools import SingletonType, dict_str, is_json

UNKNOWN = "Unknown"
SUBMISSION_STATUS = Literal["Succeeded", "Failed", "Running", "Pending"]
RUN_STATUS = Literal["Succeeded", "Failed", "Running", "Pending"]
WORKFLOW_LANGUAGE = Literal["WDL"]


def zip_files(source_files, zip_type='base64'):
    # 创建一个内存中的字节流对象
    buffer = BytesIO()

    # 使用 zipfile 模块创建一个 ZIP 文件对象
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED,
                         compresslevel=9) as zip_file:
        for f in source_files:
            # 将文件添加到 ZIP 中
            # 假设每个 f 是一个字典，包含 'name' 和 'originFile' 键
            zip_file.writestr(f['name'], f['originFile'])

    # 获取 ZIP 数据
    zip_data = buffer.getvalue()

    if zip_type == 'base64':
        # 将 ZIP 数据编码为 base64
        return base64.b64encode(zip_data).decode('utf-8')
    elif zip_type == 'blob':
        # 直接返回二进制数据
        return zip_data
    else:
        raise ValueError("zip_type must be 'base64' or 'blob'")


class Run(metaclass=SingletonType):  # 单例模式，why
    """Represents a specific run of submission .
    """

    def __repr__(self):
        info_dict = dict_str({
            "id": self.id,
            "workspace_id": self.workspace_id,
            "submission_id": self.submission,
            "engine_run_id": self.engine_run_id,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "log_path": self.log,
            "error_message": self.error,
            "duration": self.duration,
            "start_time": self.start_time,
            "finish_time": self.finish_time,
            "status": self.status,
        })
        # task info的输出格式待优化
        return f"RunInfo:\n[\n{info_dict}]\n" \
               f"TasksInfo:\n{self.tasks if self.tasks is not None else []}"

    def __init__(self, workspace_id: str, id_: str, submission_id: str):
        """
        :param workspace_id: Workspace id
        :type workspace_id: str
        :param id_: Run id
        :type id_: str
        :param submission_id: Submission ID
        :type submission_id: str
        """

        self.workspace_id = workspace_id
        self.id = id_
        self.submission = submission_id
        self._engine_run_id = UNKNOWN
        self.inputs = UNKNOWN
        self.outputs = UNKNOWN
        self.start_time = 0
        self._log: str = UNKNOWN
        self._error: str = UNKNOWN
        self._duration = 0
        self._finish_time = 0
        self._status = UNKNOWN
        self._tasks: pd.DataFrame = None
        self.sync()  # 这里会初始化上方的UNKNOWN

    @property
    def status(self) -> RUN_STATUS:
        """Returns the Run status.

        :return: Run status
        :rtype: Literal["Succeeded", "Failed", "Running", "Pending"]
        """
        if self._status in ("Succeeded", "Failed"):  #判断是否已结束流程，只有在结束前才会触发查询
            return self._status
        self.sync()
        return self._status

    @property
    def finish_time(self) -> int:
        """Returns the finish time of the Run.

        :return: The finish time of the run
        :rtype: int
        """
        if self._finish_time:
            return self._finish_time
        self.sync()
        return self._finish_time

    @property
    def duration(self) -> int:
        """Returns the running duration of the Run.

        :return: The running duration of the run
        :rtype: int
        """
        if self._duration:
            return self._duration
        self.sync()
        return self._duration

    @property
    def log(self) -> str:
        """Returns the log s3 path of the Run.

        :return: The log s3 path of the run
        :rtype: str
        """
        if self._log != UNKNOWN:
            return self._log
        self.sync()
        return self._log

    @property
    def error(self) -> str:
        """Returns the error message of the Run.

        :return: The error message of the run
        :rtype: str
        """
        if self._error != UNKNOWN:
            return self._error
        self.sync()
        return self._error

    @property
    def tasks(self) -> pd.DataFrame:
        """Returns the information of the tasks bound to the Run.

        :return: The Information of the tasks bound to the Run
        :rtype: str
        """
        if self._tasks is not None:
            res = self._tasks.query("Status=='Running'")
            if res.empty:
                return self._tasks
        tasks = Config.service().list_tasks({
            "RunID": self.id,
            "WorkspaceID": self.workspace_id
        }).get("Items")
        if len(tasks) == 0:
            return None
        self._tasks = pd.DataFrame.from_records(tasks)
        return self._tasks

    @property
    def engine_run_id(self) -> str:
        """Returns the workflow engine id of the Run.

        :return: The workflow engine id of the Run
        :rtype: str
        """
        if self._engine_run_id != UNKNOWN:
            return self._engine_run_id
        self.sync()
        return self._engine_run_id

    @cached(cache=TTLCache(maxsize=100, ttl=1))
    def sync(self):
        """Synchronizes with the remote end
        """
        resp = Config.service().list_runs({
            "SubmissionID": self.submission,
            "WorkspaceID": self.workspace_id,
            "Filter": {
                "IDs": [self.id]
            },
        })
        # not found runs
        if len(resp.get("Items")) != 1:
            return
        item = resp.get("Items")[0]
        self._status = item.get("Status")
        self.start_time = item.get("StartTime")
        self.inputs = item.get("Inputs")
        self.outputs = item.get("Outputs")
        if not item.get("Status") == "Running":
            self._engine_run_id = item.get("EngineRunID")
            self._finish_time = item.get("FinishTime")
            self._duration = item.get("Duration")
            self._log = item.get("Log")
            self._error = item.get("Message")


class Submission(metaclass=SingletonType):  # 与run class行为相同
    """Represents a submission of a workflow .
    """

    def __repr__(self):
        info_dict = dict_str({
            "id": self.id,
            "workspace_id": self.workspace_id,
            "name": self.name,
            "owner": self.owner,
            "description": self.description,
            "data_model": self.data_model,
            "data_model_rows": self.data_model_rows,
            "call_cache": self.call_cache,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "start_time": self.start_time,
            "finish_time": self.finish_time,
            "status": self.status,
        })
        return f"SubmissionInfo:\n{info_dict}\n"

    def __init__(self, workspace_id: str, id_: str):
        """
        :param workspace_id: Workspace id
        :type workspace_id: str
        :param id_: Submission id
        :type id_: str
        """
        self.workspace_id = workspace_id
        self.id = id_
        self.data_model_rows: List[str] = []
        self.name = UNKNOWN
        self.data_model = UNKNOWN
        self.call_cache = False
        self.outputs = UNKNOWN
        self.inputs = UNKNOWN
        self.description = UNKNOWN
        self.start_time = 0
        self._finish_time = 0
        self._status = UNKNOWN
        self.owner = UNKNOWN
        runs = Config.service().list_runs({
            "WorkspaceID": self.workspace_id,
            "SubmissionID": self.id,
            'PageSize': 0
        }).get("Items")
        self.runs = [
            Run(self.workspace_id, run.get("ID"), self.id) for run in runs
        ]
        self.sync()

    @property
    def finish_time(self) -> int:
        """Returns the finish time of the submission.

        :return: The finish time of submission
        :rtype: int
        """
        if self._finish_time:
            return self._finish_time
        self.sync()
        return self._finish_time

    @property
    def status(self) -> SUBMISSION_STATUS:  #Literal 在这里的作用是做类型标注
        """Returns the Submission status.

        :return: Submission status
        :rtype: Literal["Succeeded", "Failed", "Running", "Pending"]
        """
        if self._status in ("Succeeded", "Failed"):
            return self._status
        self.sync()
        return self._status

    @cached(cache=TTLCache(maxsize=100, ttl=1))
    def sync(self):
        """Synchronizes with the remote end
        """
        resp = Config.service().list_submissions({
            "WorkspaceID": self.workspace_id,
            "Filter": {
                "IDs": [self.id]
            },
            # "ID": self.id,
        })
        # not found submission
        if len(resp.get("Items")) != 1:
            return
        item = resp.get("Items")[0]

        # list data entity rows by call list runs
        runs = Config.service().list_runs({
            'WorkspaceID': self.workspace_id,
            "SubmissionID": self.id,
        }).get("Items")
        data_entity_row_ids = set()
        for run in runs:
            if run.get("DataEntityRowID") != "":
                data_entity_row_ids.add(run.get("DataEntityRowID"))
        self.data_model_rows = list(data_entity_row_ids)
        # get data model name by call list data models
        models = Config.service().list_data_models({
            'WorkspaceID':
            self.workspace_id,
        }).get("Items")
        if "DataModelID" in item.keys():
            for model in models:
                if model["ID"] == item["DataModelID"]:
                    self.data_model = model.get("Name")
                    break

        self.call_cache = item.get("ExposedOptions").get("ReadFromCache")
        self.outputs = item.get("Outputs")
        self.inputs = item.get("Inputs")
        self.owner = item.get("OwnerName")
        self.name = item.get("Name")
        self.description = item.get("Description")
        self.start_time = item.get("StartTime")
        self._status = item.get("Status")

        if not item.get("Status") in ("Running", "Pending"):
            self._finish_time = item.get("FinishTime")


class WorkflowResource(metaclass=SingletonType):

    def __init__(self, workspace_id: str):
        self.workspace_id = workspace_id

    def __repr__(self):
        info_dict = dict_str({
            "cluster": self.get_cluster,
        })
        return f"WorkflowsInfo:\n{info_dict}\n{self.list()}"

    @property
    def get_cluster(self) -> str:  # 查看在运行的机器？
        """Gets the bound cluster id supporting running workflow

        :return: The bound cluster id
        :rtype: str
        """
        workflow_env_info = Config.service().list_cluster(
            params={
                'Type': "workflow",
                "ID": self.workspace_id
            })
        for cluster in workflow_env_info.get('Items'):
            info = cluster["ClusterInfo"]
            if info['Status'] == "Running":
                # one workspace will only be bind to one cluster so far
                return info['ID']
        raise NotFoundError("cluster", "workflow")

    # 这里需要有线下的简易，WDL文件或者压缩包的import逻辑
    def import_workflow(self,
                        source: str,
                        name: str,
                        description: str,
                        language: WORKFLOW_LANGUAGE = "WDL",
                        tag: str = "",
                        main_workflow_path: str = "",
                        token: str = "") -> dict:
        """Imports a workflow .

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.workflows.import_workflow(source = "http://foo.git", name = "bar",  description = "",
            language = "WDL", tag = "baz", token = "xxxxxxxx", main_workflow_path = "aaa/bbb.wdl")

        :param source: git source of workflow
        :type source: str
        :param name: The name of specified workflow
        :type name: str
        :param description: The description of specified workflow
        :type description: str
        :param language: The language of specified workflow
        :type language: str
        :param tag: The tag of specified workflow
        :type tag: str
        :param token: The token of specified workflow
        :type token: str
        :param main_workflow_path: Main path of specified workflow
        :type main_workflow_path: str
        :return: Workflow ID
        :rtype: str
        """
        if name:  # 流程是否存在
            exist = Config.service().check_workflow({
                "WorkspaceID": self.workspace_id,
                "Name": name,
            }).get("IsNameExist")

            if exist:
                raise ConflictError("name", f"{name} already exists")
        else:
            raise ParameterError("name", name)

        if language != "WDL":
            raise ParameterError("language", f"Unsupported language: '{language}'. Only 'WDL' is supported.")

        if source.startswith("http://") or source.startswith("https://"):
            params = {
                "WorkspaceID": self.workspace_id,
                "Name": name,
                "Description": description,
                "Language": language,
                "Source": source,
                "Tag": tag,
                "MainWorkflowPath": main_workflow_path,
                "SourceType": "git",
            }
            if token:
                params["Token"] = token
            return Config.service().create_workflow(params)
        elif os.path.isdir(source):
            # 扫描文件夹中的所有 WDL 文件，并构建相对路径
            # 用 source 来检验上传的是否是文件夹
            source_files = []
            for root, _, files in os.walk(source):
                for file in files:
                    if file.endswith('.wdl'):
                        full_path = os.path.join(root, file)
                        relative_path = os.path.relpath(full_path, source)  # 获取文件相对于source的相对路径。
                        source_files.append({
                            "name": relative_path,  # 使用相对路径
                            "originFile": open(full_path, "rb").read()
                        })

            if not source_files:
                raise ParameterError("source", "No WDL files found in the specified folder")

            # 确保主工作流路径是相对路径
            if main_workflow_path:
                if not os.path.exists(main_workflow_path):
                    raise ParameterError("main_workflow_path", f"Main workflow file {main_workflow_path} not found")
                main_relative = os.path.relpath(main_workflow_path, source)
            else:
                main_relative = None

            zip_base64 = zip_files(source_files, "base64")

            params = {
                "WorkspaceID": self.workspace_id,
                "Name": name,
                "Description": description,
                "Language": language,
                "SourceType": "file",
                "Content": zip_base64,
            }
            if main_relative:
                params["MainWorkflowPath"] = os.path.basename(main_relative)
            if token:
                params["Token"] = token

            return Config.service().create_workflow(params)
        #单文件上传
        elif os.path.isfile(source) and source.endswith('.wdl'):
            source_files = [{
                "name": os.path.basename(source),
                "originFile": open(source, "rb").read()
            }]
            zip_base64 = zip_files(source_files, 'base64')

            main_workflow_path = os.path.basename(source)
            params = {
                "WorkspaceID": self.workspace_id,
                "Name": name,
                "Description": description,
                "Language": language,
                # "Source": source,
                "SourceType": "file",
                "Content": zip_base64,
                "MainWorkflowPath": main_workflow_path,
            }
            return Config.service().create_workflow(params)
        else:
            raise ParameterError("source",f"Workflow source '{source}' does not exist.")

    def list(self) -> DataFrame:
        """Lists all workflows' information .

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.workflows.list()

        :return: all workflows information
        :rtype: DataFrame
        """
        content = Config.service().list_workflows({
            'WorkspaceID': self.workspace_id,
            'SortBy': 'CreateTime',
            'PageSize': 0,
        })
        res_df = pd.DataFrame.from_records(content['Items'])
        if res_df.empty:
            return res_df
        res_df['CreateTime'] = pd.to_datetime(
            res_df['CreateTime'], unit='ms', origin=pd.Timestamp('2018-07-01'))
        res_df['UpdateTime'] = pd.to_datetime(
            res_df['UpdateTime'], unit='ms', origin=pd.Timestamp('2018-07-01'))

        return res_df

    def delete(self, target: str):
        """Deletes a workflow from the workspace .
        Considering security issues, user can only delete a single workflow currently

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.workflows.delete(target = "bar")

        :param target: workflow name
        :type target: str
        """
        res = self.list().query(f"Name=='{target}'")
        if res.empty:
            raise ParameterError("target")

        Config.service().delete_workflow({
            "WorkspaceID": self.workspace_id,
            "ID": res["ID"].iloc[0]
        })


class Workflow(metaclass=SingletonType):
    """Represents a workflow in Bio-OS.
    
    This class encapsulates all the information and operations related to a workflow,
    including its metadata, inputs, outputs, and execution capabilities.
    """

    def __init__(self,
                 name: str,
                 workspace_id: str,
                 bucket: str,
                 check: bool = False):
        """Initialize a workflow instance.
        
        Args:
            name: The name of the workflow
            workspace_id: The ID of the workspace containing this workflow
            bucket: The S3 bucket associated with this workflow
            check: Whether to check the workflow existence immediately
        """
        self.name = name
        self.workspace_id = workspace_id
        self.bucket = bucket
        self._description: str = ""
        self._create_time: int = 0
        self._update_time: int = 0
        self._language: str = "WDL"
        self._source: str = ""
        self._tag: str = ""
        self._token: Optional[str] = None
        self._main_workflow_path: str = ""
        self._status: Dict[str, Optional[str]] = {"Phase": "", "Message": None}
        self._inputs: List[Dict[str, Any]] = []
        self._outputs: List[Dict[str, Any]] = []
        self._owner_name: str = ""
        self._graph: str = ""
        self._source_type: str = ""
        
        if check:
            self.sync()

    def __repr__(self):
        """Return a string representation of the workflow."""
        info_dict = dict_str({
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "language": self.language,
            "source": self.source,
            "tag": self.tag,
            "main_workflow_path": self.main_workflow_path,
            "status": self.status,
            "owner_name": self.owner_name,
            "create_time": self.create_time,
            "update_time": self.update_time
        })
        return f"WorkflowInfo:\n{info_dict}"

    @property
    @cached(cache=TTLCache(maxsize=100, ttl=1))
    def id(self) -> str:
        """Gets the id of workflow itself

        :return: the id of workflow itself
        :rtype: str
        """
        res = WorkflowResource(self.workspace_id). \
            list().query(f"Name=='{self.name}'")
        if res.empty:
            raise ParameterError("name")
        return res["ID"].iloc[0]

    @property
    def description(self) -> str:
        """Get the workflow description."""
        if not self._description:
            self.sync()
        return self._description

    @property
    def create_time(self) -> int:
        """Get the workflow creation timestamp."""
        if not self._create_time:
            self.sync()
        return self._create_time

    @property
    def update_time(self) -> int:
        """Get the workflow last update timestamp."""
        if not self._update_time:
            self.sync()
        return self._update_time

    @property
    def language(self) -> str:
        """Get the workflow language (e.g., WDL)."""
        if not self._language:
            self.sync()
        return self._language

    @property
    def source(self) -> str:
        """Get the workflow source location."""
        if not self._source:
            self.sync()
        return self._source

    @property
    def tag(self) -> str:
        """Get the workflow version tag."""
        if not self._tag:
            self.sync()
        return self._tag

    @property
    def token(self) -> Optional[str]:
        """Get the workflow access token if any."""
        if not self._token:
            self.sync()
        return self._token

    @property
    def main_workflow_path(self) -> str:
        """Get the main workflow file path."""
        if not self._main_workflow_path:
            self.sync()
        return self._main_workflow_path

    @property
    def status(self) -> Dict[str, Optional[str]]:
        """Get the workflow status information."""
        if not self._status["Phase"]:
            self.sync()
        return self._status
    @property
    def inputs(self) -> List[Dict[str, Any]]:
        """Get the workflow input parameters."""
        if not self._inputs:
            self.sync()
        return self._inputs

    @property
    def outputs(self) -> List[Dict[str, Any]]:
        """Get the workflow output parameters."""
        if not self._outputs:
            self.sync()
        return self._outputs

    @property
    def owner_name(self) -> str:
        """Get the workflow owner's name."""
        if not self._owner_name:
            self.sync()
        return self._owner_name

    @property
    def graph(self) -> str:
        """Get the workflow graph representation."""
        if not self._graph:
            self.sync()
        return self._graph

    @property
    def source_type(self) -> str:
        """Get the workflow source type."""
        if not self._source_type:
            self.sync()
        return self._source_type

    @cached(cache=TTLCache(maxsize=100, ttl=1))
    def sync(self):
        """Synchronize workflow information with the remote service."""
        res = WorkflowResource(self.workspace_id). \
            list().query(f"Name=='{self.name}'")
        if res.empty:
            raise ParameterError("name")
            
        # Get detailed workflow information
        params = {
            'WorkspaceID': self.workspace_id,
            'Filter': {
                'IDs': [res["ID"].iloc[0]]
            }
        }
        workflows = Config.service().list_workflows(params).get('Items')
        if len(workflows) != 1:
            raise NotFoundError("workflow", self.name)
            
        detail = workflows[0]
        
        # Update all properties
        self._description = detail.get("Description", "")
        self._create_time = detail.get("CreateTime", 0)
        self._update_time = detail.get("UpdateTime", 0)
        self._language = detail.get("Language", "WDL")
        self._source = detail.get("Source", "")
        self._tag = detail.get("Tag", "")
        self._token = detail.get("Token")
        self._main_workflow_path = detail.get("MainWorkflowPath", "")
        self._status = detail.get("Status", {"Phase": "", "Message": None})
        self._inputs = detail.get("Inputs", [])
        self._outputs = detail.get("Outputs", [])
        self._owner_name = detail.get("OwnerName", "")
        self._graph = detail.get("Graph", "")
        self._source_type = detail.get("SourceType", "")

    @property
    @cached(cache=TTLCache(maxsize=100, ttl=1))
    def get_cluster(self):
        """Gets the bound cluster id supporting running workflow

        :return: The bound cluster id
        :rtype: str
        """
        workflow_env_info = Config.service().list_cluster(
            params={
                'Type': "workflow",
                "ID": self.workspace_id
            })
        for cluster in workflow_env_info.get('Items'):
            info = cluster["ClusterInfo"]
            if info['Status'] == "Running":
                # one workspace will only be bind to one cluster so far
                return info['ID']
        raise NotFoundError("cluster", "workflow")

    def query_data_model_id(self, name: str) -> str:
        """Gets the id of given data_models among those accessible

        Args:
            name: The name of the data model

        Returns:
            str: The ID of the data model, or empty string if not found
        """
        res = DataModelResource(self.workspace_id).list(). \
            query(f"Name=='{name}'")
        if res.empty:
            return ""
        return res["ID"].iloc[0]

    def submit(self,
               inputs: str,
               outputs: str,
               submission_desc: str,
               call_caching: bool,
               submission_name_suffix: str = "",
               row_ids: List[str] = [],
               data_model_name: str = '') -> List[Run]:
        """Submit an existed workflow.

        *Example*:
        ::

            ws = bioos.workspace("foo")
            wf = ws.workflow(name="123456788")
            wf.submit(inputs = "{\"aaa\":\"bbb\"}",
                      outputs = "{\"ccc\": \"ddd\"}",
                      data_model_name = "bar",
                      row_ids = ["1a","2b"],
                      submission_desc = "baz",
                      call_caching = True)

        :param data_model_name: The name of data_model to be used
        :type data_model_name: str
        :param row_ids: Rows to be used of specified data_model
        :type row_ids: List[str]
        :param inputs: Workflow inputs
        :type inputs: str
        :param outputs: Workflow outputs
        :type outputs: str
        :param submission_name_suffix: The suffix of this submission's name, defaults to yyyy-mm-dd-HH-MM-ss. The name format is {workflow_name}-history-{submission_name_suffix}
        :type submission_name_suffix: str
        :param submission_desc: The description of this submission
        :type submission_desc: str
        :param call_caching: CallCaching searches in the cache of previously running tasks with exactly the same commands and exactly the same input tasks. If the cache hit, the results of the previous task will be used instead of reorganizing, thereby saving time and resources.
        :type call_caching: bool
        :return: Result Runs corresponding to submitted workflows
        :rtype: List[Run]
        """

        if not inputs and not is_json(inputs):
            raise ParameterError('inputs')
        if not outputs and not is_json(outputs):
            raise ParameterError('outputs')
        if not submission_name_suffix:
            submission_name_suffix = datetime.now().strftime(
                '%Y-%m-%d-%H-%M-%S')

        params = {
            "ClusterID": self.get_cluster,
            'WorkspaceID': self.workspace_id,
            'WorkflowID': self.id,
            'Name': workflows.submission_name(self.name,
                                              submission_name_suffix),
            'Description': submission_desc,
            'Inputs': inputs,
            'ExposedOptions': {
                "ReadFromCache": call_caching,
                # TODO this may change in the future
                "ExecutionRootDir": f"s3://{self.bucket}"
            },
            'Outputs': outputs,
        }

        # It is batch mode when data_model_name and row_ids are specified.
        if data_model_name and row_ids:
            data_model_id = self.query_data_model_id(data_model_name)
            if not data_model_id:
                raise ParameterError("data_model_name")

            params['DataModelID'] = data_model_id
            params['DataModelRowIDs'] = row_ids

        submission_id = Config.service().create_submission(params).get("ID")

        return Submission(self.workspace_id, submission_id).runs

