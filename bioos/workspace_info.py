from typing import Dict, Any, List
import pandas as pd

from bioos import bioos
from bioos.config import Config, DEFAULT_ENDPOINT
from bioos.errors import NotFoundError


class WorkspaceInfo:
    """Bio-OS 工作空间信息管理类"""

    def __init__(self, ak: str, sk: str, endpoint: str = DEFAULT_ENDPOINT):
        """
        初始化工作空间信息管理类

        Args:
            ak: Bio-OS 访问密钥
            sk: Bio-OS 私钥
            endpoint: Bio-OS API 端点，默认为 https://bio-top.miracle.ac.cn
        """
        self.ak = ak
        self.sk = sk
        self.endpoint = endpoint
        # 配置 Bio-OS
        Config.set_access_key(ak)
        Config.set_secret_key(sk)
        Config.set_endpoint(endpoint)

    def create_workspace(self, name: str, description: str) -> Dict[str, Any]:
        """
        创建工作空间

        Args:
            name: 工作空间名称
            description: 工作空间描述

        Returns:
            Dict[str, Any]: 创建结果，成功时包含工作空间信息，失败时包含错误信息
        """
        try:
            result = bioos.create_workspace(name=name, description=description)
            return result
        except Exception as e:
            return {"error": str(e)}

    def list_workspaces(self) -> Dict[str, Any]:
        """
        列出所有工作空间

        Returns:
            Dict[str, Any]: 工作空间列表，失败时包含错误信息
        """
        try:
            df = bioos.list_workspaces()
            return {"workspaces": df.to_dict('records')}
        except Exception as e:
            return {"error": str(e)}

    def get_workspace_id(self, workspace_name: str) -> Dict[str, Any]:
        """
        获取工作空间ID

        Args:
            workspace_name: 工作空间名称

        Returns:
            Dict[str, Any]: 包含工作空间ID的字典，或错误信息
        """
        try:
            df = bioos.list_workspaces()
            ser = df[df.Name == workspace_name].ID
            if len(ser) != 1:
                return {"error": f"工作空间 '{workspace_name}' 未找到"}
            return {"workspace_id": ser.to_list()[0]}
        except Exception as e:
            return {"error": str(e)}

    def get_workspace_info(self, workspace_name: str) -> Dict[str, Any]:
        """
        获取工作空间详细信息

        Args:
            workspace_name: 工作空间名称

        Returns:
            Dict[str, Any]: 工作空间详细信息，失败时包含错误信息
        """
        try:
            workspace_id_result = self.get_workspace_id(workspace_name)
            if "error" in workspace_id_result:
                return workspace_id_result

            workspace_id = workspace_id_result["workspace_id"]
            ws = bioos.workspace(workspace_id)

            return {
                "workspace_id": workspace_id,
                "basic_info": ws.basic_info,
                "env_info": ws.env_info.to_dict('records') if not ws.env_info.empty else []
            }
        except Exception as e:
            return {"error": str(e)}

    def get_workspace_workflows(self, workspace_name: str) -> Dict[str, Any]:
        """
        获取工作空间下的所有工作流

        Args:
            workspace_name: 工作空间名称

        Returns:
            Dict[str, Any]: 工作流列表，失败时包含错误信息
        """
        try:
            workspace_id_result = self.get_workspace_id(workspace_name)
            if "error" in workspace_id_result:
                return workspace_id_result

            workspace_id = workspace_id_result["workspace_id"]
            ws = bioos.workspace(workspace_id)
            workflows = ws.workflows.list()

            return {"workflows": workflows}
        except Exception as e:
            return {"error": str(e)}

    def get_workspace_data_models(self, workspace_name: str) -> Dict[str, Any]:
        """
        获取工作空间下的所有数据模型

        Args:
            workspace_name: 工作空间名称

        Returns:
            Dict[str, Any]: 数据模型列表，失败时包含错误信息
        """
        try:
            workspace_id_result = self.get_workspace_id(workspace_name)
            if "error" in workspace_id_result:
                return workspace_id_result

            workspace_id = workspace_id_result["workspace_id"]
            ws = bioos.workspace(workspace_id)
            data_models = ws.data_models.list().to_dict('records')

            return {"data_models": data_models}
        except Exception as e:
            return {"error": str(e)}

    def bind_cluster_to_workspace(self, workspace_id: str, cluster_id: str = "default") -> Dict[str, Any]:
        """
        将集群绑定到工作空间

        Args:
            workspace_id: 工作空间ID
            cluster_id: 集群ID，默认为 "default"

        Returns:
            Dict[str, Any]: 绑定结果，直接返回API响应
        """
        try:
            return bioos.bind_cluster_to_workspace(workspace_id=workspace_id, cluster_id=cluster_id)
        except Exception as e:
            return {"error": str(e)}