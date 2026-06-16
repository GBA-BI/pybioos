from typing import Optional

from network.auth import BioOSBridgePassportProvider
from network.config import resolve_repository_endpoint
from network.resource.data_libraries import DataLibraryResource
from network.resource.datasets import DataSet, DataSetResource
from network.resource.drs import DRSResource
from network.resource.repository import RepositoryResource
from bioos.utils.common_tools import dict_str


class NetworkResource:
    """Top-level BioOS Network resource domain.

    Network is a sibling service domain of BioOS. The default auth provider
    still uses BioOS AK/SK as a bridge to obtain a Network passport, but the
    Repository/Data Library/DRS resource model is independent of BioOS RPC.
    """

    def __init__(
        self,
        repository_endpoint: Optional[str] = None,
        passport_provider: Optional[BioOSBridgePassportProvider] = None,
    ):
        self.repository_endpoint = resolve_repository_endpoint(repository_endpoint)
        self.passport_provider = passport_provider or BioOSBridgePassportProvider()

    def __repr__(self) -> str:
        info = {
            "repository_endpoint": self.repository_endpoint,
        }
        return f"NetworkResource:\n{dict_str(info)}"

    @property
    def repository(self) -> RepositoryResource:
        return RepositoryResource(
            repository_endpoint=self.repository_endpoint,
            passport_provider=self.passport_provider,
        )

    @property
    def libraries(self) -> DataLibraryResource:
        return self.repository.libraries

    @property
    def datasets(self) -> DataSetResource:
        return self.repository.datasets

    @property
    def drs(self) -> DRSResource:
        return DRSResource(
            repository_endpoint=self.repository_endpoint,
            passport_provider=self.passport_provider,
        )

    def library(self, data_library_id: str):
        return self.repository.library(data_library_id)

    def dataset(self, data_set_id: str, data_library_id: Optional[str] = None) -> DataSet:
        return self.repository.dataset(data_set_id, data_library_id=data_library_id)

    def drs_object(self, object_id: str):
        return self.drs.object(object_id)

    def drs_access(self, object_id: str, access_id: str = "https"):
        return self.drs.access(object_id, access_id=access_id)

    def drs_locate(self, drs_path: str):
        return self.drs.locate(drs_path)

    def download_drs_object(
        self,
        object_id: str,
        target: str = ".",
        access_id: str = "https",
        overwrite: bool = False,
    ):
        return self.drs.download_object(
            object_id,
            target=target,
            access_id=access_id,
            overwrite=overwrite,
        )
