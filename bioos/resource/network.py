from typing import Optional

from bioos.internal.repository import (
    RepositoryPassportProvider,
    resolve_drs_endpoint,
    resolve_repository_endpoint,
)
from bioos.resource.datasets import DataSet, DataSetResource
from bioos.utils.common_tools import SingletonType, dict_str


class NetworkResource(metaclass=SingletonType):
    """Account-level BioOS Network resource domain.

    Network uses the existing BioOS AK/SK login to obtain a short-lived AAAI
    passport internally, similar to how workspace files obtain TOS temporary
    credentials before touching object storage.
    """

    def __init__(
        self,
        repository_endpoint: Optional[str] = None,
        drs_endpoint: Optional[str] = None,
        passport_provider: Optional[RepositoryPassportProvider] = None,
    ):
        self.repository_endpoint = resolve_repository_endpoint(repository_endpoint)
        self.drs_endpoint = resolve_drs_endpoint(drs_endpoint)
        self.passport_provider = passport_provider or RepositoryPassportProvider()

    def __repr__(self) -> str:
        info = {
            "repository_endpoint": self.repository_endpoint,
            "drs_endpoint": self.drs_endpoint,
        }
        return f"NetworkResource:\n{dict_str(info)}"

    @property
    def datasets(self) -> DataSetResource:
        """Returns the BioOS Network data set resource collection."""
        return DataSetResource(
            repository_endpoint=self.repository_endpoint,
            drs_endpoint=self.drs_endpoint,
            passport_provider=self.passport_provider,
        )

    def dataset(self, data_set_id: str, data_library_id: Optional[str] = None) -> DataSet:
        """Returns a single BioOS Network data set object."""
        return self.datasets.data_set(data_set_id, data_library_id=data_library_id)

    def drs_object(self, object_id: str):
        """Returns GA4GH DRS object information for a Network object ID."""
        return self.datasets.drs_object(object_id)

    def drs_access(self, object_id: str, access_id: str = "https"):
        """Returns a GA4GH DRS access URL object for a Network object ID."""
        return self.datasets.drs_access(object_id, access_id=access_id)

    def download_drs_object(
        self,
        object_id: str,
        target: str = ".",
        access_id: str = "https",
        overwrite: bool = False,
    ):
        """Download a GA4GH DRS object to a local path."""
        return self.datasets.download_drs_object(
            object_id,
            target=target,
            access_id=access_id,
            overwrite=overwrite,
        )
