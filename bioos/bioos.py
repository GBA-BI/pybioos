from pandas import DataFrame
from volcengine.const.Const import REGION_CN_NORTH1

from bioos.config import Config
from bioos.resource.utility import UtilityResource
from bioos.resource.workspaces import Workspace


def status() -> Config.LoginInfo:
    """Get the current login information.

    *Example*:
    ::

        bioos.status()

    :return: Current login information
    :rtype: Config.LoginInfo
    """
    return Config.login_info()


def login(access_key: str,
          secret_key: str,
          endpoint: str = None,
          region: str = REGION_CN_NORTH1) -> bool:
    """Login to the given endpoint using specified account and password.

    **If bioos sdk runs inside the miracle private cloud env** such as on a notebook under a
    workspace, the login procedure will be finished automatically.

    **If bioos sdk runs outside the miracle cloud env** such as on user's local machine,
    the login procedure should be explicitly executed.

    *Example*:
    ::

        bioos.login(access_key="xxxxxxxx", secret_key="xxxxxxxx")
        # or specify endpoint explicitly:
        bioos.login(access_key="xxxxxxxx", secret_key="xxxxxxxx", endpoint="https://cloud.xxxxx.xxx.cn")

    :param access_key: The specified account's access key
    :type access_key: str
    :param secret_key: Corresponding secret key of the access key
    :type secret_key: str
    :param endpoint: The environment to be logged in (optional, defaults to Config._endpoint)
    :type endpoint: str
    :param region: The region to be logged in
    :type region: str
    :return: Login result
    :rtype: bool
    """
    Config.set_access_key(access_key)
    Config.set_secret_key(secret_key)
    if endpoint is not None:
        Config.set_endpoint(endpoint)
    Config.set_region(region)
    return Config.login_info().login_status == "Already logged in"


def list_workspaces() -> DataFrame:
    """Lists all workspaces in the login environment .

    *Example*:
    ::

        bioos.list_workspaces()

    """
    return DataFrame.from_records(Config.service().list_workspaces({
        "PageSize":
        0
    }).get("Items"))


def create_workspace(name: str, description: str) -> dict:
    """Creates a new workspace in the login environment.

    *Example*:
    ::

        bioos.create_workspace(name="My Workspace", description="This is my new workspace")

    :param name: Name of the workspace to create
    :type name: str
    :param description: Description of the workspace
    :type description: str
    :return: Creation result containing workspace information
    :rtype: dict
    """
    params = {
        "Name": name,
        "Description": description
    }
    return Config.service().create_workspace(params)


def workspace(id_: str) -> Workspace:  # 这里是workspace的入口
    """Returns the workspace for the given name .

    :param id_: Workspace id
    :type id_: str
    :return: Specified workspace object
    :rtype: Workspace
    """
    return Workspace(id_)



def bind_cluster_to_workspace(workspace_id: str, cluster_id: str = "default") -> dict:
    """Binds a cluster to the specified workspace.

    *Example*:
    ::

        bioos.bind_cluster_to_workspace(workspace_id="ws_123", cluster_id="default")

    :param workspace_id: ID of the workspace to bind to the cluster
    :type workspace_id: str
    :param cluster_id: ID of the cluster to bind (defaults to "default")
    :type cluster_id: str
    :return: Binding result
    :rtype: dict
    """
    params = {
        "ClusterID": cluster_id,
        "Type": "workflow",
        "ID": workspace_id
    }
    return Config.service().bind_cluster_to_workspace(params)


def utility() -> UtilityResource:
    """Returns Common tool collection Resource object

    :return: Tool collection Resource object
    :rtype: UtilityResource
    """
    return UtilityResource()