import os

from bioos.config import Config
from bioos.errors import EnvironmentConfigurationError


def get_endpoint_env():
    return os.environ.get('BIOOS_ENDPOINT')


def get_workspace_id_env():
    workspace_id = os.environ.get('BIOOS_WORKSPACE_ID')
    if not workspace_id:
        # use in MiracleCloud Notebook editor(jupyterhub)
        workspace_id = os.environ.get('JUPYTERHUB_SERVER_NAME')
    return workspace_id


class BioOsServiceConfig(Config):
    _workspace_id: str = None

    @classmethod
    def set_env(cls):
        if cls._service is None:
            endpoint = get_endpoint_env()
            if not endpoint:
                raise EnvironmentConfigurationError('BIOOS_ENDPOINT')
            cls.set_endpoint(endpoint)

        workspace_id = get_workspace_id_env()
        if not workspace_id:
            raise EnvironmentConfigurationError('BIOOS_WORKSPACE_ID')
        cls._workspace_id = workspace_id

    @classmethod
    def workspace_id(cls):
        return cls._workspace_id
