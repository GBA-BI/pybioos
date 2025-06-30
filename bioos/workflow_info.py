from typing import Dict, Any, List, Optional
import pandas as pd

from bioos import bioos
from bioos.config import Config
from bioos.errors import NotFoundError


class WorkflowInfo:
    """Bio-OS 工作流信息查询类"""

    def __init__(self, ak: str, sk: str, endpoint: str = "https://bio-top.miracle.ac.cn"):
        """
        初始化工作流信息查询类

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

    @staticmethod
    def _fmt_default(raw: Any) -> str | None:
        """
        将 Default 字段格式化成人类可读形式：
        - None        → None
        - int/float   → 123 / 1.23
        - bool        → true / false
        - 其余字符串  → "value"（保留双引号）
        """
        if raw is None:
            return None
        if isinstance(raw, (int, float, bool)):
            return str(raw).lower()  # bool 转成 'true' / 'false'
        if isinstance(raw, str):
            lo = raw.lower()
            # 尝试将字符串视为数字或布尔
            if lo in {"true", "false"}:
                return lo
            try:
                int(raw);   return raw
            except ValueError:
                pass
            try:
                float(raw); return raw
            except ValueError:
                pass
            return f"\"{raw}\""
        return str(raw)

    def get_workspace_id(self, workspace_name: str) -> str:
        """
        获取工作区ID

        Args:
            workspace_name: 工作区名称

        Returns:
            str: 工作区ID

        Raises:
            NotFoundError: 未找到指定工作区
        """
        df = bioos.list_workspaces()
        ser = df[df.Name == workspace_name].ID
        if len(ser) != 1:
            raise NotFoundError("Workspace", workspace_name)
        return ser.to_list()[0]

    def get_workflow(self, workspace_name: str, workflow_name: str):
        """
        获取工作流对象

        Args:
            workspace_name: 工作区名称
            workflow_name: 工作流名称

        Returns:
            Workflow: 工作流对象

        Raises:
            NotFoundError: 未找到指定工作区或工作流
        """
        workspace_id = self.get_workspace_id(workspace_name)
        ws = bioos.workspace(workspace_id)
        return ws.workflow(name=workflow_name)

    def list_workflows(self, workspace_name: str) -> List[Dict[str, Any]]:
        """
        列出工作区下的所有工作流

        Args:
            workspace_name: 工作区名称

        Returns:
            List[Dict[str, Any]]: 工作流列表
        """
        workspace_id = self.get_workspace_id(workspace_name)
        ws = bioos.workspace(workspace_id)
        return ws.list_workflows()

    def get_workflow_inputs(self, workspace_name: str, workflow_name: str) -> Dict[str, str]:
        """
        获取工作流的输入参数模板

        Args:
            workspace_name: 工作区名称
            workflow_name: 工作流名称

        Returns:
            Dict[str, str]: 包含工作流输入参数模板的字典，格式为：
            {
                "param_name": "Type (optional, default = value)",  # 对于可选参数
                "param_name": "Type"                              # 对于必需参数
            }
            其中：
            - Type 为参数类型（如 String, Int, File 等）
            - optional 表示参数为可选
            - value 为默认值（数字和布尔值直接显示，字符串加引号）
        """
        try:
            wf = self.get_workflow(workspace_name, workflow_name)
            result = {}
            
            for item in wf.inputs:
                type_str = item.get("Type", "")
                optional = item.get("Optional", False)
                default = self._fmt_default(item.get("Default"))

                if optional:
                    value = f"{type_str} (optional" + (f", default = {default})" if default is not None else ")")
                else:
                    value = type_str

                result[item["Name"]] = value

            return result

        except NotFoundError as e:
            return {"error": str(e)}
        except Exception as e:
            return {"error": str(e)}

    def get_workflow_outputs(self, workspace_name: str, workflow_name: str) -> Dict[str, str]:
        """
        获取工作流的输出参数信息

        Args:
            workspace_name: 工作区名称
            workflow_name: 工作流名称

        Returns:
            Dict[str, str]: 包含工作流输出参数的字典
        """
        try:
            wf = self.get_workflow(workspace_name, workflow_name)
            return {output["Name"]: output["Type"] for output in wf.outputs}
        except NotFoundError as e:
            return {"error": str(e)}
        except Exception as e:
            return {"error": str(e)}

    def get_workflow_metadata(self, workspace_name: str, workflow_name: str) -> Dict[str, Any]:
        """
        获取工作流的元数据信息

        Args:
            workspace_name: 工作区名称
            workflow_name: 工作流名称

        Returns:
            Dict[str, Any]: 包含工作流元数据的字典，包括：
                - name: 工作流名称
                - description: 工作流描述
                - language: 工作流语言
                - source: 工作流源
                - tag: 版本标签
                - status: 工作流状态
                - owner_name: 所有者
                - create_time: 创建时间
                - update_time: 更新时间
                - main_workflow_path: 主工作流文件路径
                - source_type: 源类型
        """
        try:
            wf = self.get_workflow(workspace_name, workflow_name)
            return {
                "name": wf.name,
                "description": wf.description,
                "language": wf.language,
                "source": wf.source,
                "tag": wf.tag,
                "status": wf.status,
                "owner_name": wf.owner_name,
                "create_time": wf.create_time,
                "update_time": wf.update_time,
                "main_workflow_path": wf.main_workflow_path,
                "source_type": wf.source_type
            }
        except NotFoundError as e:
            return {"error": str(e)}
        except Exception as e:
            return {"error": str(e)} 