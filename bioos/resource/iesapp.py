from datetime import datetime
from typing import Optional, Dict, Any, List

import pandas as pd
from cachetools import TTLCache, cached
from pandas import DataFrame

from bioos.config import Config
from bioos.errors import ConflictError, NotFoundError, ParameterError
from bioos.utils.common_tools import SingletonType, dict_str


class WebInstanceApp(metaclass=SingletonType):
    """表示一个Web实例应用程序。
    
    这个类封装了所有与Web实例应用程序相关的信息和操作，
    包括其元数据、状态和创建能力。
    """

    def __init__(self,
                 name: str,
                 workspace_id: str,
                 check: bool = False):
        """初始化Web实例应用程序实例。
        
        Args:
            name: Web实例应用程序的名称
            workspace_id: 包含此Web实例应用程序的工作空间ID
            check: 是否立即检查Web实例应用程序的存在性
        """
        self.name = name
        self.workspace_id = workspace_id
        self._id: str = ""
        self._description: str = ""
        self._status: str = ""
        self._create_time: int = 0
        self._update_time: int = 0
        self._owner_name: str = ""
        self._endpoint: str = ""
        self._port: int = 0
        self._app_type: str = ""
        self._user_id: int = 0
        self._scope: str = ""
        self._resource_size: str = ""
        self._storage_capacity: int = 0
        self._ssh_info: Dict[str, Any] = {}
        self._image: str = ""
        self._termination: Dict[str, Any] = {}
        self._tos_mounts: List[Dict[str, Any]] = []
        self._status_detail: Dict[str, Any] = {}
        self._access_urls: Dict[str, str] = {}
        self._running_duration: int = 0
        self._start_time: int = 0
        
        if check:
            self.sync()

    def __repr__(self):
        """返回Web实例应用程序的字符串表示。"""
        info_dict = dict_str({
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "status": self.status_detail.get('State', 'Unknown'),
            "user_id": self.user_id,
            "scope": self.scope,
            "resource_size": self.resource_size,
            "storage_capacity": self.storage_capacity,
            "image": self.image,
            "running_duration": self.get_running_time_formatted(),
            "is_running": self.is_running(),
            "ssh_info": self.get_ssh_connection_info(),
            "access_urls": self.access_urls,
            "create_time": self.create_time,
            "update_time": self.update_time
        })
        return f"WebInstanceAppInfo:\n{info_dict}"

    @property
    @cached(cache=TTLCache(maxsize=100, ttl=1))
    def id(self) -> str:
        """获取Web实例应用程序的ID
        
        :return: Web实例应用程序的ID
        :rtype: str
        """
        res = WebInstanceAppResource(self.workspace_id).list().query(f"Name=='{self.name}'")
        if res.empty:
            raise ParameterError("name")
        return res["ID"].iloc[0]

    @property
    def description(self) -> str:
        """获取Web实例应用程序描述。"""
        if not self._description:
            self.sync()
        return self._description

    @property
    def status(self) -> str:
        """获取Web实例应用程序状态。"""
        if not self._status:
            self.sync()
        return self._status

    @property
    def create_time(self) -> int:
        """获取Web实例应用程序创建时间戳。"""
        if not self._create_time:
            self.sync()
        return self._create_time

    @property
    def update_time(self) -> int:
        """获取Web实例应用程序最后更新时间戳。"""
        if not self._update_time:
            self.sync()
        return self._update_time

    @property
    def owner_name(self) -> str:
        """获取Web实例应用程序所有者姓名。"""
        if not self._owner_name:
            self.sync()
        return self._owner_name

    @property
    def endpoint(self) -> str:
        """获取Web实例应用程序访问端点。"""
        if not self._endpoint:
            self.sync()
        return self._endpoint

    @property
    def port(self) -> int:
        """获取Web实例应用程序端口。"""
        if not self._port:
            self.sync()
        return self._port

    @property
    def app_type(self) -> str:
        """获取Web实例应用程序类型。"""
        if not self._app_type:
            self.sync()
        return self._app_type

    @property
    def user_id(self) -> int:
        """获取用户ID。"""
        if not self._user_id:
            self.sync()
        return self._user_id

    @property
    def scope(self) -> str:
        """获取实例范围。"""
        if not self._scope:
            self.sync()
        return self._scope

    @property
    def resource_size(self) -> str:
        """获取资源规格。"""
        if not self._resource_size:
            self.sync()
        return self._resource_size

    @property
    def storage_capacity(self) -> int:
        """获取存储容量。"""
        if not self._storage_capacity:
            self.sync()
        return self._storage_capacity

    @property
    def ssh_info(self) -> Dict[str, Any]:
        """获取SSH连接信息。"""
        if not self._ssh_info:
            self.sync()
        return self._ssh_info

    @property
    def image(self) -> str:
        """获取镜像地址。"""
        if not self._image:
            self.sync()
        return self._image

    @property
    def termination(self) -> Dict[str, Any]:
        """获取终止策略配置。"""
        if not self._termination:
            self.sync()
        return self._termination

    @property
    def tos_mounts(self) -> List[Dict[str, Any]]:
        """获取TOS挂载配置。"""
        if not self._tos_mounts:
            self.sync()
        return self._tos_mounts

    @property
    def status_detail(self) -> Dict[str, Any]:
        """获取详细状态信息。"""
        if not self._status_detail:
            self.sync()
        return self._status_detail

    @property
    def access_urls(self) -> Dict[str, str]:
        """获取访问URL。"""
        if not self._access_urls:
            self.sync()
        return self._access_urls

    @property
    def running_duration(self) -> int:
        """获取运行持续时间（秒）。"""
        self.sync()  # 运行时间需要实时获取
        return self._running_duration

    @property
    def start_time(self) -> int:
        """获取启动时间戳。"""
        if not self._start_time:
            self.sync()
        return self._start_time

    @cached(cache=TTLCache(maxsize=100, ttl=1))
    def sync(self):
        """与远程服务同步Web实例应用程序信息。"""
        res = WebInstanceAppResource(self.workspace_id).list().query(f"Name=='{self.name}'")
        if res.empty:
            raise ParameterError("name")
            
        # 获取详细的Web实例应用程序信息
        params = {
            'Filter': {
                'Application': 'ies',
                'WorkspaceID': self.workspace_id,
                'IDs': [res["ID"].iloc[0]]
            }
        }
        webinstanceapps = Config.service().list_webinstance_apps(params).get('Items')
        if len(webinstanceapps) != 1:
            raise NotFoundError("webinstanceapp", self.name)
            
        detail = webinstanceapps[0]
        
        # 更新所有属性
        self._id = detail.get("ID", "")
        self._description = detail.get("Description", "")
        self._status = detail.get("Status", "")
        self._create_time = detail.get("CreateTime", 0)
        self._update_time = detail.get("UpdateTime", 0)
        self._owner_name = detail.get("OwnerName", "")
        self._endpoint = detail.get("Endpoint", "")
        self._port = detail.get("Port", 0)
        self._app_type = detail.get("AppType", "")
        
        # 新增的属性
        self._user_id = detail.get("UserID", 0)
        self._scope = detail.get("Scope", "")
        self._resource_size = detail.get("ResourceSize", "")
        self._storage_capacity = detail.get("StorageCapacity", 0)
        self._ssh_info = detail.get("SSH", {})
        self._image = detail.get("Image", "")
        self._termination = detail.get("Termination", {})
        self._tos_mounts = detail.get("TOSMounts", [])
        
        # 状态详情处理
        status_detail = detail.get("Status", {})
        if isinstance(status_detail, dict):
            self._status_detail = status_detail
            self._access_urls = status_detail.get("AccessURLs", {})
            self._running_duration = status_detail.get("RunningDuration", 0)
            self._start_time = status_detail.get("StartTime", 0)
        else:
            self._status_detail = {"State": str(status_detail)}
            self._access_urls = {}
            self._running_duration = 0
            self._start_time = 0

    def check_name_exists(self) -> bool:
        """检查当前Web实例应用程序名称是否已存在。

        *Example*:
        ::

            ws = bioos.workspace("foo")
            app = ws.webinstanceapp("my-ies-instance")
            exists = app.check_name_exists()
            if exists:
                print("实例名称已存在")
            else:
                print("实例名称可用")

        :return: 名称是否已存在
        :rtype: bool
        """
        params = {
            "WorkspaceID": self.workspace_id,
            "Name": self.name,
            "Application": "ies"
        }

        result = Config.service().check_webinstance_app(params)
        return result.get("IsNameExist", False)

    def get_ssh_connection_info(self) -> Dict[str, str]:
        """获取SSH连接信息的便捷方法。

        *Example*:
        ::

            ws = bioos.workspace("foo")
            app = ws.webinstanceapp("my-ies-instance")
            ssh_info = app.get_ssh_connection_info()
            print(f"SSH地址: {ssh_info['ip']}:{ssh_info['port']}")
            print(f"用户名: {ssh_info['username']}")
            print(f"密码: {ssh_info['password']}")

        :return: SSH连接信息
        :rtype: Dict[str, str]
        """
        ssh_data = self.ssh_info
        return {
            'ip': ssh_data.get('IP', ''),
            'port': ssh_data.get('Port', ''),
            'username': ssh_data.get('Username', ''),
            'password': ssh_data.get('Passwd', '')
        }

    def get_notebook_url(self) -> str:
        """获取Jupyter Notebook访问URL。

        :return: Notebook访问URL
        :rtype: str
        """
        return self.access_urls.get('notebook', '')

    def get_rstudio_url(self) -> str:
        """获取RStudio访问URL。

        :return: RStudio访问URL
        :rtype: str
        """
        return self.access_urls.get('rstudio', '')

    def get_vscode_url(self) -> str:
        """获取VSCode访问URL。

        :return: VSCode访问URL
        :rtype: str
        """
        return self.access_urls.get('vscode', '')

    def is_running(self) -> bool:
        """检查实例是否正在运行。

        :return: 是否正在运行
        :rtype: bool
        """
        status_detail = self.status_detail
        return status_detail.get('State', '') == 'Running'

    def get_running_time_formatted(self) -> str:
        """获取格式化的运行时间。

        :return: 格式化的运行时间（如"1小时23分钟"）
        :rtype: str
        """
        duration = self.running_duration
        if duration < 60:
            return f"{duration}秒"
        elif duration < 3600:
            minutes = duration // 60
            seconds = duration % 60
            return f"{minutes}分钟{seconds}秒"
        else:
            hours = duration // 3600
            minutes = (duration % 3600) // 60
            return f"{hours}小时{minutes}分钟"

    def get_events(self, 
                   start_time: Optional[int] = None, 
                   end_time: Optional[int] = None, 
                   levels: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """获取Web实例应用程序的事件日志。

        *Example*:
        ::

            ws = bioos.workspace("foo")
            app = ws.webinstanceapp("my-ies-instance")
            
            # 获取所有事件
            events = app.get_events()
            
            # 获取指定时间范围的事件
            events = app.get_events(start_time=1640995200, end_time=1641081600)
            
            # 获取指定级别的事件
            events = app.get_events(levels=["Info", "Urgency"])

        :param start_time: 起始时间戳（可选）
        :type start_time: Optional[int]
        :param end_time: 结束时间戳（可选）
        :type end_time: Optional[int]
        :param levels: 事件级别列表，可选值: ["Info", "Urgency", "Priority"]
        :type levels: Optional[List[str]]
        :return: 事件列表
        :rtype: List[Dict[str, Any]]
        """
        params = {
            "ID": self.id,
            "WorkspaceID": self.workspace_id,
            "Filter": {}
        }
        
        # 构建过滤条件
        filter_params = {}
        if start_time is not None:
            filter_params["StartTime"] = start_time
        if end_time is not None:
            filter_params["EndTime"] = end_time
        if levels is not None:
            filter_params["Level"] = levels
            
        if filter_params:
            params["Filter"] = filter_params

        result = Config.service().list_webinstance_events(params)
        return result.get("Items", [])

    def commit_image(self, 
                     image_name: str, 
                     description: str = "", 
                     replace_image: bool = False) -> dict:
        """将当前实例状态保存为镜像。

        *Example*:
        ::

            ws = bioos.workspace("foo")
            app = ws.webinstanceapp("my-ies-instance")
            
            # 保存为新镜像
            result = app.commit_image(
                image_name="my-custom-image:v1.0",
                description="我的自定义开发环境",
                replace_image=False
            )

        :param image_name: 镜像名称
        :type image_name: str
        :param description: 镜像描述
        :type description: str
        :param replace_image: 是否自动替换当前实例的镜像
        :type replace_image: bool
        :return: 提交结果
        :rtype: dict
        """
        if not image_name:
            raise ParameterError("image_name", "image_name cannot be empty")

        params = {
            "WorkspaceID": self.workspace_id,
            "WebappInstanceID": self.id,
            "Image": image_name,
            "Description": description,
            "ImageReplace": replace_image
        }

        return Config.service().commit_ies_image(params)


class WebInstanceAppResource(metaclass=SingletonType):
    """Web实例应用程序资源管理类。"""

    def __init__(self, workspace_id: str):
        self.workspace_id = workspace_id

    def __repr__(self):
        return f"WebInstanceAppInfo:\n{self.list()}"

    def list(self) -> DataFrame:
        """列出所有Web实例应用程序信息。

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.webinstanceapps.list()

        :return: 所有Web实例应用程序信息
        :rtype: DataFrame
        """
        content = Config.service().list_webinstance_apps({
            'Filter': {
                'Application': 'ies',
                'WorkspaceID': self.workspace_id
            }
        })
        res_df = pd.DataFrame.from_records(content['Items'])
        if res_df.empty:
            return res_df
        res_df['CreateTime'] = pd.to_datetime(
            res_df['CreateTime'], unit='ms', origin=pd.Timestamp('2018-07-01'))
        res_df['UpdateTime'] = pd.to_datetime(
            res_df['UpdateTime'], unit='ms', origin=pd.Timestamp('2018-07-01'))

        return res_df

    def create_new_instance(self,
                           name: str,
                           description: str,
                           resource_size: str = "1c-2gib",
                           storage_capacity: int = 21474836480,
                           image: str = "",
                           ssh_enabled: bool = True,
                           running_time_limit_seconds: int = 10800,  # 3小时 = 3*60*60
                           idle_timeout_seconds: int = 10800,  # 3小时 = 3*60*60
                           auto_start: bool = True) -> dict:
        """创建一个新的IES Web实例应用程序。

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.webinstanceapps.create_new_instance(
                name="my-ies-instance",
                description="我的IES实例",
                resource_size="1c-2gib",
                storage_capacity=21474836480,
                image="your-image-url",
                ssh_enabled=True,
                running_time_limit_seconds=10800,
                idle_timeout_seconds=10800,
                auto_start=True
            )

        :param name: Web实例应用程序名称
        :type name: str
        :param description: Web实例应用程序描述
        :type description: str
        :param resource_size: 资源规格（如"1c-2gib"表示1核2GB内存）
        :type resource_size: str
        :param storage_capacity: 存储容量（字节，默认20GB）
        :type storage_capacity: int
        :param image: 镜像地址
        :type image: str
        :param ssh_enabled: 是否启用SSH
        :type ssh_enabled: bool
        :param running_time_limit_seconds: 运行时长限制（秒，5分钟-24小时，默认3小时）
        :type running_time_limit_seconds: int
        :param idle_timeout_seconds: 空闲超时时间（秒，5分钟-24小时，默认3小时）
        :type idle_timeout_seconds: int
        :param auto_start: 是否自动启动
        :type auto_start: bool
        :return: 创建结果
        :rtype: dict
        """
        if not name:
            raise ParameterError("name", "name cannot be empty")

        if not image:
            raise ParameterError("image", "image cannot be empty")

        # 检查名称是否已存在
        if self.check_name_exists(name):
            raise ConflictError("name", f"{name} already exists")

        # 验证时间限制范围（5分钟到24小时）
        min_time = 300  # 5分钟
        max_time = 86400  # 24小时
        
        if not (min_time <= running_time_limit_seconds <= max_time):
            raise ParameterError("running_time_limit_seconds", 
                               f"must be between {min_time} and {max_time} seconds")
        
        if not (min_time <= idle_timeout_seconds <= max_time):
            raise ParameterError("idle_timeout_seconds", 
                               f"must be between {min_time} and {max_time} seconds")

        params = {
            "WorkspaceID": self.workspace_id,
            "Name": name,
            "Scope": "Private",
            "Description": description,
            "Application": "ies",
            "ResourceSize": resource_size,
            "StorageCapacity": storage_capacity,
            "SSHEnabled": ssh_enabled,
            "Image": image,
            "Termination": {
                "RunningTimeLimitSeconds": running_time_limit_seconds,
                "IdleTimeoutSeconds": idle_timeout_seconds
            },
            "TOSMounts": [
                {
                    "BucketName": f"bioos-{self.workspace_id}",
                    "MountPath": f"/home/ies/bioos-{self.workspace_id}",
                    "ReadOnly": False
                }
            ],
            "AutoStart": auto_start
        }

        return Config.service().create_webinstance_app(params)

    def delete(self, target: str):
        """从工作空间删除一个Web实例应用程序。

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.webinstanceapps.delete(target="bar")

        :param target: Web实例应用程序名称
        :type target: str
        """
        res = self.list().query(f"Name=='{target}'")
        if res.empty:
            raise ParameterError("target")

        Config.service().delete_webinstance_app({
            "ID": res["ID"].iloc[0],
            "WorkspaceID": self.workspace_id
        })

    def start(self, target: str):
        """启动一个Web实例应用程序。

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.webinstanceapps.start(target="bar")

        :param target: Web实例应用程序名称
        :type target: str
        """
        res = self.list().query(f"Name=='{target}'")
        if res.empty:
            raise ParameterError("target")

        Config.service().start_webinstance_app({
            "ID": res["ID"].iloc[0],
            "WorkspaceID": self.workspace_id
        })

    def stop(self, target: str):
        """停止一个Web实例应用程序。

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.webinstanceapps.stop(target="bar")

        :param target: Web实例应用程序名称
        :type target: str
        """
        res = self.list().query(f"Name=='{target}'")
        if res.empty:
            raise ParameterError("target")

        Config.service().stop_webinstance_app({
            "ID": res["ID"].iloc[0],
            "WorkspaceID": self.workspace_id
        })

    def get_events(self, 
                   target: str,
                   start_time: Optional[int] = None, 
                   end_time: Optional[int] = None, 
                   levels: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """获取指定Web实例应用程序的事件日志。

        *Example*:
        ::

            ws = bioos.workspace("foo")
            
            # 获取所有事件
            events = ws.webinstanceapps.get_events("my-ies-instance")
            
            # 获取指定级别的事件
            events = ws.webinstanceapps.get_events("my-ies-instance", levels=["Info", "Urgency"])

        :param target: Web实例应用程序名称
        :type target: str
        :param start_time: 起始时间戳（可选）
        :type start_time: Optional[int]
        :param end_time: 结束时间戳（可选）
        :type end_time: Optional[int]
        :param levels: 事件级别列表
        :type levels: Optional[List[str]]
        :return: 事件列表
        :rtype: List[Dict[str, Any]]
        """
        res = self.list().query(f"Name=='{target}'")
        if res.empty:
            raise ParameterError("target")

        params = {
            "ID": res["ID"].iloc[0],
            "WorkspaceID": self.workspace_id
        }
        
        # 构建过滤条件
        filter_params = {}
        if start_time is not None:
            filter_params["StartTime"] = start_time
        if end_time is not None:
            filter_params["EndTime"] = end_time
        if levels is not None:
            filter_params["Level"] = levels
            
        if filter_params:
            params["Filter"] = filter_params

        result = Config.service().list_webinstance_events(params)
        return result.get("Items", [])

    def commit_image(self, 
                     target: str,
                     image_name: str, 
                     description: str = "", 
                     replace_image: bool = False) -> dict:
        """将指定实例状态保存为镜像。

        *Example*:
        ::

            ws = bioos.workspace("foo")
            
            # 保存为新镜像
            result = ws.webinstanceapps.commit_image(
                target="my-ies-instance",
                image_name="my-custom-image:v1.0",
                description="我的自定义开发环境",
                replace_image=False
            )

        :param target: Web实例应用程序名称
        :type target: str
        :param image_name: 镜像名称
        :type image_name: str
        :param description: 镜像描述
        :type description: str
        :param replace_image: 是否自动替换当前实例的镜像
        :type replace_image: bool
        :return: 提交结果
        :rtype: dict
        """
        if not image_name:
            raise ParameterError("image_name", "image_name cannot be empty")

        res = self.list().query(f"Name=='{target}'")
        if res.empty:
            raise ParameterError("target")

        params = {
            "WorkspaceID": self.workspace_id,
            "WebappInstanceID": res["ID"].iloc[0],
            "Image": image_name,
            "Description": description,
            "ImageReplace": replace_image
        }

        return Config.service().commit_ies_image(params)

    def check_name_exists(self, name: str) -> bool:
        """检查Web实例应用程序名称是否已存在。

        *Example*:
        ::

            ws = bioos.workspace("foo")
            exists = ws.webinstanceapps.check_name_exists("my-ies-instance")
            if exists:
                print("实例名称已存在")
            else:
                print("实例名称可用")

        :param name: Web实例应用程序名称
        :type name: str
        :return: 名称是否已存在
        :rtype: bool
        """
        if not name:
            raise ParameterError("name", "name cannot be empty")

        params = {
            "WorkspaceID": self.workspace_id,
            "Name": name,
            "Application": "ies"
        }

        result = Config.service().check_webinstance_app(params)
        return result.get("IsNameExist", False)
