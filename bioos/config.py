import os

from typing_extensions import Literal
from volcengine.const.Const import REGION_CN_NORTH1

from bioos.errors import ConfigurationError
from bioos.log import PyLogger
from bioos.service.BioOsService import BioOsService

LOGIN_STATUS = Literal['Already logged in', 'Not logged in']


class Config:
    _service: BioOsService = None
    _access_key: str = os.environ.get('VOLC_ACCESSKEY')
    _secret_key: str = os.environ.get('VOLC_SECRETKEY')
    _endpoint: str = os.environ.get('BIOOS_ENDPOINT')
    _region: str = REGION_CN_NORTH1
    Logger = PyLogger()  # 这里是把类赋给了Logger变量

    class LoginInfo:
        """[Only Read]Record the current login information .
        """

        @property
        def access_key(self):
            """Returns the Login AccessKey .
            """
            return Config._access_key

        @property
        def secret_key(self) -> str:
            """Returns the Login SecretKey .
            """
            return Config._secret_key

        @property
        def endpoint(self) -> str:
            """Returns the Login Endpoint .
            """
            return Config._endpoint

        @property
        def region(self) -> str:
            """Returns the Login Region .
            """
            return Config._region

        @property
        def login_status(self) -> LOGIN_STATUS:
            """Return the login status .

            :return: Login status: 'Already logged in' or 'Not logged in'
            :rtype: str
            """
            try:
                Config._ping_func()
            except ConfigurationError:
                return "Not logged in"
            except Exception as e:
                Config.Logger.error(e)  # 这里触发Logger的类函数
                return "Not logged in"
            return "Already logged in"

        def __repr__(self):
            return f"{self.login_status}\n" \
                   f"endpoint: {self.endpoint}\n" \
                   f"access_key: {self.access_key}\n" \
                   f"secret_key: {self.secret_key}\n" \
                   f"region: {self.region}"

    @classmethod
    def _same_endpoint(cls):
        return (cls._service.service_info.scheme + "://" +
                cls._service.service_info.host) == cls._endpoint

    @classmethod
    def _same_region(cls):
        return cls._service.service_info.credentials.region == cls._region

    @classmethod
    def service(cls):
        if cls._service:
            return cls._service
        cls._init_service()
        return cls._service

    @classmethod
    def _ping_func(cls):
        if not cls._service:
            cls._init_service()
        cls._service.list_workspaces({})  #通过该函数能否正常执行来判断是否登陆成功。

    @classmethod
    def login_info(cls):
        return Config.LoginInfo()

    @classmethod
    def set_access_key(cls, access_key: str):
        cls._access_key = access_key
        if cls._service:
            cls._service.set_ak(cls._access_key)

    @classmethod
    def set_secret_key(cls, secret_key: str):
        cls._secret_key = secret_key
        if cls._service:
            cls._service.set_sk(cls._secret_key)

    @classmethod
    def set_endpoint(cls, endpoint: str):
        cls._endpoint = endpoint
        if cls._service and cls._same_endpoint():
            return

        cls._init_service()

    @classmethod
    def set_region(cls, region: str):
        cls._region = region
        if cls._service and cls._same_region():
            return

        cls._init_service()

    @classmethod
    def _init_service(cls):
        if cls._service and cls._same_region() and cls._same_endpoint():
            return

        if not cls._endpoint:
            raise ConfigurationError('ENDPOINT')

        if not cls._region:
            raise ConfigurationError('REGION')

        if not cls._access_key:
            raise ConfigurationError('ACCESS_KEY')

        if not cls._secret_key:
            raise ConfigurationError('SECRET_KEY')

        cls._service = BioOsService(
            endpoint=cls._endpoint,
            region=cls._region)  #cls._service 属性保持登陆状态，并做为下游的调用入口
        cls._service.set_ak(cls._access_key)
        cls._service.set_sk(cls._secret_key)
