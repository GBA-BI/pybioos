from typing import Optional

from volcengine.const.Const import REGION_CN_NORTH1

from bioos.config import Config, DEFAULT_ENDPOINT
from network.auth import BioOSBridgePassportProvider, StaticPassportProvider, passport_subject
from network.resource.network import NetworkResource


_passport_provider = None


def login_with_bioos(
    access_key: str,
    secret_key: str,
    endpoint: str = DEFAULT_ENDPOINT,
    region: str = REGION_CN_NORTH1,
) -> bool:
    Config.set_access_key(access_key)
    Config.set_secret_key(secret_key)
    if endpoint is not None:
        Config.set_endpoint(endpoint)
    Config.set_region(region)
    set_passport_provider(BioOSBridgePassportProvider())
    return Config.login_info().login_status == "Already logged in"


def login_with_passport(passport: str) -> bool:
    set_passport_provider(StaticPassportProvider(passport))
    return True


def set_passport_provider(provider) -> None:
    global _passport_provider
    _passport_provider = provider


def passport_provider():
    return _passport_provider or BioOSBridgePassportProvider()


def current_user_id(force_refresh: bool = False) -> str:
    return passport_subject(passport_provider(), force_refresh=force_refresh)


def network(repository_endpoint: Optional[str] = None) -> NetworkResource:
    return NetworkResource(
        repository_endpoint=repository_endpoint,
        passport_provider=passport_provider(),
    )


def repository(repository_endpoint: Optional[str] = None):
    return network(repository_endpoint=repository_endpoint).repository


def libraries(repository_endpoint: Optional[str] = None):
    return network(repository_endpoint=repository_endpoint).libraries


def library(data_library_id: str, repository_endpoint: Optional[str] = None):
    return network(repository_endpoint=repository_endpoint).library(data_library_id)


def datasets(repository_endpoint: Optional[str] = None):
    return network(repository_endpoint=repository_endpoint).datasets


def dataset(data_set_id: str, data_library_id: Optional[str] = None, repository_endpoint: Optional[str] = None):
    return network(repository_endpoint=repository_endpoint).dataset(data_set_id, data_library_id=data_library_id)


def drs_object(object_id: str):
    return network().drs_object(object_id)


def drs_access(object_id: str, access_id: str = "https"):
    return network().drs_access(object_id, access_id=access_id)


def drs_locate(drs_path: str, repository_endpoint: Optional[str] = None):
    return network(repository_endpoint=repository_endpoint).drs_locate(drs_path)


def download_drs_object(
    object_id: str,
    target: str = ".",
    access_id: str = "https",
    overwrite: bool = False,
):
    return network().download_drs_object(
        object_id,
        target=target,
        access_id=access_id,
        overwrite=overwrite,
    )
