from datetime import datetime
import os
import time
import urllib.request

import pandas as pd
from cachetools import TTLCache, cached

from bioos.config import Config
from bioos.resource.data_models import DataModelResource
from bioos.resource.files import FileResource
from bioos.resource.workflows import Workflow, WorkflowResource
from bioos.resource.iesapp import WebInstanceApp, WebInstanceAppResource
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

    @property
    def webinstanceapps(self) -> WebInstanceAppResource:
        """Returns WebInstanceAppResource object.

        :return: WebInstanceAppResource object
        :rtype: WebInstanceAppResource
        """
        return WebInstanceAppResource(self._id)

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

    def webinstanceapp(self, name: str) -> WebInstanceApp:
        """Returns the webinstanceapp for the given name

        :param name: WebInstanceApp name
        :type name: str
        :return: Specified webinstanceapp object
        :rtype: WebInstanceApp
        """
        return WebInstanceApp(name, self._id)

    def bind_cluster(self, cluster_id: str, type_: str = "workflow") -> dict:
        """把当前 Workspace 绑定到指定集群"""
        params = {"ClusterID": cluster_id, "Type": type_, "ID": self._id}
        return Config.service().bind_cluster_to_workspace(params)


    def export_workspace_v2(self, 
                           download_path: str = "./", 
                           monitor: bool = True,
                           monitor_interval: int = 5,
                           max_retries: int = 60) -> dict:
        """导出当前 Workspace 的所有元信息并下载到本地
        
        :param download_path: 下载文件保存路径，默认当前目录
        :type download_path: str
        :param monitor: 是否监控导出状态直到完成，默认 True
        :type monitor: bool
        :param monitor_interval: 轮询间隔（秒），默认 5 秒
        :type monitor_interval: int
        :param max_retries: 最大重试次数，默认 60 次（5 分钟）
        :type max_retries: int
        :return: 导出结果信息，包含 status、schema_id、file_path 等
        :rtype: dict
        """
        params = {"WorkspaceID": self._id}
        result = Config.service().export_workspace_v2(params)
        schema_id = result.get('ID')
        
        if not schema_id:
            raise Exception("Failed to create export task: No schema ID returned")
        
        # 如果不监控，直接返回任务 ID
        if not monitor:
            return {
                "status": "submitted",
                "schema_id": schema_id,
                "message": "Export task submitted. Use list_schemas to check status."
            }
        
        # 步骤 2: 轮询查询导出状态
        Config.Logger.info(f"Export task created with schema ID: {schema_id}")
        Config.Logger.info(f"Monitoring export status (checking every {monitor_interval}s, max {max_retries} retries)...")
        
        retry_count = 0
        schema_key = None
        
        while retry_count < max_retries:
            # 查询所有 schemas
            schemas_result = Config.service().list_schemas({"Filter": {}})
            schemas = schemas_result.get("Items", [])
            
            # 查找当前导出任务
            for schema in schemas:
                if schema.get("ID") == schema_id:
                    phase = schema.get("Status", {}).get("Phase", "")
                    message = schema.get("Status", {}).get("Message", "")
                    
                    if phase == "Succeeded":
                        Config.Logger.info("Export task succeeded!")
                        schema_key = schema.get("SchemaKey")
                        break
                    elif phase == "Failed":
                        error_msg = f"Export task failed: {message}"
                        Config.Logger.error(error_msg)
                        raise Exception(error_msg)
                    else:
                        Config.Logger.info(f"Export task status: {phase}")
            
            # 如果成功找到文件，跳出循环
            if schema_key:
                break
            
            # 继续等待
            time.sleep(monitor_interval)
            retry_count += 1
        
        # 超时检查
        if not schema_key:
            raise Exception(f"Export task timeout after {max_retries * monitor_interval} seconds")
        
        # 步骤 3: 获取预签名 URL
        Config.Logger.info(f"Getting presigned URL for schema: {schema_id}")
        presigned_params = {
            "ID": schema_id,
            "WorkspaceID": self._id
        }
        presigned_result = Config.service().get_export_workspace_presigned_url(presigned_params)
        presigned_url = presigned_result.get("PreSignedURL")
        
        if not presigned_url:
            raise Exception("Failed to get presigned URL for export file")
        
        Config.Logger.info(f"Downloading export file from presigned URL...")
        
        # 步骤 4: 使用 HTTP 请求下载文件
        # 确保下载目录存在
        os.makedirs(download_path, exist_ok=True)
        
        # 从 URL 中提取文件名
        filename = os.path.basename(schema_key)
        local_file_path = os.path.join(download_path, filename)
        
        try:
            # 使用 urllib 下载文件
            urllib.request.urlretrieve(presigned_url, local_file_path)
            Config.Logger.info(f"Export file downloaded successfully to: {local_file_path}")
        except Exception as e:
            raise Exception(f"Failed to download export file from presigned URL: {str(e)}")
        
        return {
            "status": "success",
            "schema_id": schema_id,
            "schema_key": schema_key,
            "file_path": local_file_path,
            "message": f"Workspace exported and downloaded successfully"
        }