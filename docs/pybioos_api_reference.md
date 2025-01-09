# pybioos API 参考文档

## 模块结构

```
pybioos/
├── bioos/
│   ├── __init__.py          # 包的主要入口点
│   ├── config.py            # 配置管理
│   ├── errors.py            # 异常定义
│   ├── log.py              # 日志管理
│   ├── resource/           # 资源管理模块
│   │   ├── workflows.py    # 工作流资源
│   │   ├── workspaces.py   # 工作空间资源
│   │   ├── files.py        # 文件资源
│   │   └── data_models.py  # 数据模型资源
│   └── service/            # 服务模块
│       └── BioOsService.py # Bio-OS 服务实现
```

## 核心模块

### bioos

主要的功能入口点，提供了基础的操作接口。

#### 函数

##### `login(endpoint: str, access_key: str, secret_key: str, region: str = "cn-north-1") -> None`
登录到 Bio-OS 平台。

参数:
- `endpoint`: Bio-OS 平台的 API 端点
- `access_key`: 访问密钥
- `secret_key`: 密钥
- `region`: 区域，默认为 "cn-north-1"

##### `status() -> LoginInfo`
获取当前登录状态。

返回:
- `LoginInfo` 对象，包含登录信息

##### `list_workspaces() -> DataFrame`
列出所有可访问的工作空间。

返回:
- 包含工作空间信息的 DataFrame

##### `workspace(id_: str) -> Workspace`
获取指定 ID 的工作空间。

参数:
- `id_`: 工作空间 ID

返回:
- `Workspace` 对象

### resource.workflows

工作流资源管理模块。

#### 类

##### `WorkflowResource`

###### 方法

`__init__(workspace_id: str)`
初始化工作流资源。

参数:
- `workspace_id`: 工作空间 ID

`import_workflow(source: str, name: str, description: str, language: str = "WDL", tag: str = "", main_workflow_path: str = "", token: str = "") -> dict`
导入工作流。

参数:
- `source`: 工作流源文件路径或 Git 仓库 URL
- `name`: 工作流名称
- `description`: 工作流描述
- `language`: 工作流语言，默认为 "WDL"
- `tag`: Git 标签（可选）
- `main_workflow_path`: 主工作流文件路径（可选）
- `token`: Git 访问令牌（可选）

返回:
- 包含导入结果的字典

##### `Workflow`

###### 方法

`__init__(name: str, workspace_id: str, bucket: str, check: bool = False)`
初始化工作流对象。

参数:
- `name`: 工作流名称
- `workspace_id`: 工作空间 ID
- `bucket`: 存储桶名称
- `check`: 是否检查工作流存在性

`submit(inputs: str, outputs: str, submission_desc: str, call_caching: bool = False) -> List[Run]`
提交工作流运行。

参数:
- `inputs`: 输入参数 JSON
- `outputs`: 输出参数 JSON
- `submission_desc`: 提交描述
- `call_caching`: 是否启用调用缓存

返回:
- 运行实例列表

##### `Run`

###### 属性

- `status`: 运行状态
- `log`: 运行日志
- `error`: 错误信息
- `outputs`: 运行输出
- `duration`: 运行时长
- `start_time`: 开始时间
- `finish_time`: 结束时间

###### 方法

`sync()`
同步运行状态。

### resource.files

文件资源管理模块。

#### 类

##### `FileResource`

###### 方法

`list(prefix: str = "") -> DataFrame`
列出文件。

参数:
- `prefix`: 文件前缀（可选）

返回:
- 包含文件信息的 DataFrame

`upload(source: str, target: str, flatten: bool = False) -> None`
上传文件。

参数:
- `source`: 源文件路径
- `target`: 目标路径
- `flatten`: 是否展平目录结构

`download(files: List[str], local_path: str, flatten: bool = False) -> None`
下载文件。

参数:
- `files`: 文件路径列表
- `local_path`: 本地保存路径
- `flatten`: 是否展平目录结构

`s3_urls(key: str) -> List[str]`
获取文件的 S3 URL。

参数:
- `key`: 文件键名

返回:
- S3 URL 列表

## 异常类

### `ConfigurationError`
配置相关错误。

### `ParameterError`
参数验证错误。

### `NotFoundError`
资源未找到错误。

### `ConflictError`
资源冲突错误。

## 配置

### `Config`

#### 类方法

`set_endpoint(endpoint: str) -> None`
设置 API 端点。

`set_access_key(access_key: str) -> None`
设置访问密钥。

`set_secret_key(secret_key: str) -> None`
设置密钥。

`set_region(region: str) -> None`
设置区域。

## 日志

### `PyLogger`

#### 类方法

`debug(content: str) -> None`
记录调试信息。

`info(content: str) -> None`
记录普通信息。

`warn(content: str) -> None`
记录警告信息。

`error(content: str) -> None`
记录错误信息。 