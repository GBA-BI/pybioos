from datetime import datetime

import pandas as pd
from cachetools import TTLCache, cached

from bioos.config import Config
from bioos.resource.data_models import DataModelResource
from bioos.resource.files import FileResource
from bioos.resource.workflows import Workflow, WorkflowResource
from bioos.utils.common_tools import SingletonType, dict_str


class Workspace(metaclass=SingletonType):

    def __init__(self, id_: str):
        self._id = id_
        self._bucket = None

    def __repr__(self) -> str:
        return f"WorkspaceID: {self._id}\n\n" \
               f"BasicInfo:\n{dict_str(self.basic_info)}\n\n" \
               f"EnvInfo:\n{self.env_info}"

    @property
    @cached(cache=TTLCache(maxsize=10, ttl=1))  #这里为了做缓存，同时定义property，调用时生效
    def basic_info(self) -> dict:
        """ Returns basic info of the workspace

        :return: Basic info
        :rtype: dict
        """
        workspace_infos = Config.service().list_workspaces(
            {  #这里是BioOsService对象的list_workspaces方法，非对外暴露的全局方法
                "Filter": {
                    "IDs": [self._id]
                }
            }).get("Items")
        if len(workspace_infos) != 1:
            return {}
        workspace_info = workspace_infos[0]
        s3_bucket = workspace_info.get("S3Bucket")
        create_time = datetime.fromtimestamp(workspace_info.get("CreateTime"))
        owner = workspace_info.get("OwnerName")
        description = workspace_info.get("Description")
        name = workspace_info.get("Name")
        return {
            "name": name,
            "description": description,
            "s3_bucket": s3_bucket,
            "owner": owner,
            "create_time": create_time
        }

    @property
    @cached(cache=TTLCache(maxsize=10, ttl=1))
    def env_info(self) -> pd.DataFrame:
        """ Returns cluster info of the workspace

        :return: Cluster info
        :rtype: pandas.DataFrame
        """
        notebook_env_info = Config.service().list_cluster(params={
            'Type': "notebook",
            "ID": self._id
        })
        workflow_env_info = Config.service().list_cluster(params={
            'Type': "workflow",
            "ID": self._id
        })
        res = []
        for cluster in notebook_env_info.get('Items') + workflow_env_info.get(
                'Items'):
            info = cluster["ClusterInfo"]
            if info['Status'] == "Running":
                res.append({
                    "cluster_id": info['ID'],
                    "name": info['Name'],
                    "description": info['Description'],
                    "type": cluster["Type"]
                })
        return pd.DataFrame(res)

    @property
    def data_models(self) -> DataModelResource:
        """Returns DataModelResource object .

        :return: DataModelResource object
        :rtype: DataModelResource
        """
        return DataModelResource(self._id)

    @property
    def workflows(self) -> WorkflowResource:
        """Returns WorkflowResource object.

        :return: WorkflowResource object
        :rtype: WorkflowResource
        """
        return WorkflowResource(self._id)

    @property
    def files(self) -> FileResource:
        """Returns FileResource object .

        :return: FileResource object
        :rtype: FileResource
        """
        if not self._bucket:
            self._bucket = self.basic_info.get("s3_bucket")

        return FileResource(self._id, self._bucket)

    def workflow(self, name: str) -> Workflow:  # 通过这里执行的选择workflow生成wf的操作
        """Returns the workflow for the given name

        :param name: Workflow name
        :type name: str
        :return: Specified workflow object
        :rtype: Workflow
        """
        if not self._bucket:
            self._bucket = self.basic_info.get("s3_bucket")
        return Workflow(name, self._id, self._bucket)
