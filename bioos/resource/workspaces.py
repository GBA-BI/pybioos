from datetime import datetime
from typing import Dict, Any, List

import pandas as pd
from cachetools import TTLCache, cached

from bioos.config import Config, DEFAULT_ENDPOINT
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

    def workflow(self, name: str) -> Workflow:
        if not self._bucket:
            self._bucket = self.basic_info.get("s3_bucket")
        return Workflow(name, self._id, self._bucket)

    # 静态方法：基础操作
    @staticmethod
    def get_workspace_id_by_name(workspace_name: str) -> str:
        """通过工作空间名称获取ID"""
        from bioos import bioos
        df = bioos.list_workspaces()
        ser = df[df.Name == workspace_name].ID
        if len(ser) != 1:
            if len(ser) == 0:
                raise ValueError(f"工作空间 '{workspace_name}' 未找到")
            else:
                raise ValueError(f"找到多个名为 '{workspace_name}' 的工作空间")
        return ser.to_list()[0]

    @staticmethod
    def list_all_workspaces() -> pd.DataFrame:
        """列出所有工作空间"""
        from bioos import bioos
        return bioos.list_workspaces()

    # 类方法：便捷工厂
    @classmethod
    def from_name(cls, workspace_name: str) -> 'Workspace':
        """通过名称创建工作空间实例"""
        workspace_id = cls.get_workspace_id_by_name(workspace_name)
        return cls(workspace_id)


class WorkspaceManager:

    def __init__(self, ak: str = None, sk: str = None, endpoint: str = DEFAULT_ENDPOINT):
        if ak and sk:
            Config.set_access_key(ak)
            Config.set_secret_key(sk)
            Config.set_endpoint(endpoint)

    def create_workspace(self, name: str, description: str) -> Dict[str, Any]:
        try:
            from bioos import bioos
            return bioos.create_workspace(name=name, description=description)
        except Exception as e:
            return {"error": str(e)}

    def list_workspaces(self) -> Dict[str, Any]:
        try:
            from bioos import bioos
            df = bioos.list_workspaces()
            return {"workspaces": df.to_dict('records')}
        except Exception as e:
            return {"error": str(e)}

    def get_workspace_id(self, workspace_name: str) -> Dict[str, Any]:
        try:
            from bioos import bioos
            df = bioos.list_workspaces()
            ser = df[df.Name == workspace_name].ID
            if len(ser) != 1:
                return {"error": f"工作空间 '{workspace_name}' 未找到"}
            return {"workspace_id": ser.to_list()[0]}
        except Exception as e:
            return {"error": str(e)}

    def get_workspace_info(self, workspace_name: str) -> Dict[str, Any]:
        try:
            workspace_id_result = self.get_workspace_id(workspace_name)
            if "error" in workspace_id_result:
                return workspace_id_result
            workspace_id = workspace_id_result["workspace_id"]
            ws = Workspace(workspace_id)
            return {
                "workspace_id": workspace_id,
                "basic_info": ws.basic_info,
                "env_info": ws.env_info.to_dict('records') if not ws.env_info.empty else []
            }
        except Exception as e:
            return {"error": str(e)}

    def get_workspace_workflows(self, workspace_name: str) -> Dict[str, Any]:
        try:
            workspace_id_result = self.get_workspace_id(workspace_name)
            if "error" in workspace_id_result:
                return workspace_id_result
            workspace_id = workspace_id_result["workspace_id"]
            ws = Workspace(workspace_id)
            return {"workflows": ws.workflows.list()}
        except Exception as e:
            return {"error": str(e)}

    def get_workspace_data_models(self, workspace_name: str) -> Dict[str, Any]:
        try:
            workspace_id_result = self.get_workspace_id(workspace_name)
            if "error" in workspace_id_result:
                return workspace_id_result
            workspace_id = workspace_id_result["workspace_id"]
            ws = Workspace(workspace_id)
            return {"data_models": ws.data_models.list().to_dict('records')}
        except Exception as e:
            return {"error": str(e)}

    def bind_cluster_to_workspace(self, workspace_id: str, cluster_id: str = "default") -> Dict[str, Any]:
        try:
            from bioos import bioos
            return bioos.bind_cluster_to_workspace(workspace_id=workspace_id, cluster_id=cluster_id)
        except Exception as e:
            return {"error": str(e)}