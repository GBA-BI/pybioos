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


def login(endpoint: str,
          access_key: str,
          secret_key: str,
          region: str = REGION_CN_NORTH1) -> bool:
    """Login to the given endpoint using specified account and password.

    **If bioos sdk runs inside the miracle private cloud env** such as on a notebook under a
    workspace, the login procedure will be finished automatically.

    **If bioos sdk runs outside the miracle cloud env** such as on user's local machine,
    the login procedure should be explicitly executed.

    *Example*:
    ::

        bioos.login(endpoint="https://cloud.xxxxx.xxx.cn",access_key="xxxxxxxx",secret_key="xxxxxxxx")

    :param endpoint: The environment to be logged in
    :type endpoint: str
    :param access_key: The specified account's access key
    :type access_key: str
    :param secret_key: Corresponding secret key of the access key
    :type secret_key: str
    :param region: The region to be logged in
    :type region: str
    :return: Login result
    :rtype: bool
    """
    Config.set_access_key(access_key)
    Config.set_secret_key(secret_key)
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


def workspace(id_: str) -> Workspace:  # 这里是workspace的入口
    """Returns the workspace for the given name .

    :param id_: Workspace id
    :type id_: str
    :return: Specified workspace object
    :rtype: Workspace
    """
    return Workspace(id_)


def utility() -> UtilityResource:
    """Returns Common tool collection Resource object

    :return: Tool collection Resource object
    :rtype: UtilityResource
    """
    return UtilityResource()
