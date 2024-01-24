from datetime import datetime
from typing import List

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
                        language: WORKFLOW_LANGUAGE,
                        tag: str,
                        main_workflow_path: str,
                        description: str = "",
                        token: str = "") -> str:
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

        if not (source.startswith("http://") or source.startswith("https://")):
            raise ParameterError("source", source)
        if language != "WDL":
            raise ParameterError("language", language)

        params = {
            "WorkspaceID": self.workspace_id,
            "Name": name,
            "Description": description,
            "Language": language,
            "Source": source,
            "Tag": tag,
            "MainWorkflowPath": main_workflow_path,
        }
        if token:
            params["Token"] = token
        return Config.service().create_workflow(params).get("ID")

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

        return res_df.drop("Status", axis=1)

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

    def __init__(self,
                 name: str,
                 workspace_id: str,
                 bucket: str,
                 check: bool = False):
        self.name = name
        self.workspace_id = workspace_id
        self.bucket = bucket
        if check:
            self.id

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

    def query_data_model_id(self, name: str) -> "":
        """Gets the id of given data_models among those accessible

        :param name:
        :return:
        """
        res = DataModelResource(self.workspace_id).list(). \
            query(f"Name=='{name}'")
        if res.empty:
            return ""
        return res["ID"].iloc[0]

    def submit(self, data_model_name: str, row_ids: List[str], inputs: str, outputs: str,
               submission_desc: str, call_caching: bool, submission_name_suffix: str = "") \
            -> List[Run]:
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
        if not row_ids:
            raise ParameterError("row_ids")
        if not inputs and not is_json(inputs):
            raise ParameterError('inputs')
        if not outputs and not is_json(outputs):
            raise ParameterError('outputs')

        data_model_id = self.query_data_model_id(data_model_name)
        if not data_model_id:
            raise ParameterError("data_model_name")

        if not submission_name_suffix:
            submission_name_suffix = datetime.now().strftime(
                '%Y-%m-%d-%H-%M-%S')
        submission_id = Config.service().create_submission({
            "ClusterID":
            self.get_cluster,
            'WorkspaceID':
            self.workspace_id,
            'WorkflowID':
            self.id,
            'Name':
            workflows.submission_name(self.name, submission_name_suffix),
            'Description':
            submission_desc,
            'DataModelID':
            data_model_id,
            'DataModelRowIDs':
            row_ids,
            'Inputs':
            inputs,
            'ExposedOptions': {
                "ReadFromCache": call_caching,
                # TODO this may change in the future
                "ExecutionRootDir": f"s3://{self.bucket}"
            },
            'Outputs':
            outputs,
        }).get("ID")

        return Submission(self.workspace_id, submission_id).runs
