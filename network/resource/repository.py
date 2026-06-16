from typing import Optional

from bioos.errors import ParameterError
from bioos.utils.common_tools import dict_str
from network.auth import BioOSBridgePassportProvider
from network.config import resolve_repository_endpoint
from network.internal.http import NetworkHttpClient
from network.resource.data_libraries import DataLibrary, DataLibraryResource
from network.resource.datasets import DataSetResource


class RepositoryResource:
    """Central BioOS Network repository/catalogue."""

    def __init__(
        self,
        repository_endpoint: Optional[str] = None,
        passport_provider: Optional[BioOSBridgePassportProvider] = None,
    ):
        self.repository_endpoint = resolve_repository_endpoint(repository_endpoint)
        self.passport_provider = passport_provider or BioOSBridgePassportProvider()
        self.client = NetworkHttpClient(
            endpoint=self.repository_endpoint,
            passport_provider=self.passport_provider,
            sign_requests=True,
        )

    def __repr__(self) -> str:
        return f"RepositoryResource:\n{dict_str({'repository_endpoint': self.repository_endpoint})}"

    @property
    def libraries(self) -> DataLibraryResource:
        return DataLibraryResource(
            repository_endpoint=self.repository_endpoint,
            repository_client=self.client,
            passport_provider=self.passport_provider,
        )

    @property
    def datasets(self) -> DataSetResource:
        return DataSetResource(
            repository_endpoint=self.repository_endpoint,
            repository_client=self.client,
            passport_provider=self.passport_provider,
        )

    def library(self, data_library_id: str) -> DataLibrary:
        return self.libraries.data_library(data_library_id)

    def dataset(self, data_set_id: str, data_library_id: Optional[str] = None):
        if data_library_id:
            return self.library(data_library_id).dataset(data_set_id)
        return self.datasets.data_set(data_set_id)

    def apply_for_dataset(
        self,
        data_library_id: str,
        data_set_id: str,
        applicant: Optional[str] = None,
        topic: Optional[str] = None,
        description: Optional[str] = None,
        organization: Optional[str] = None,
        expired_time: Optional[int] = None,
    ):
        if not data_library_id:
            raise ParameterError("data_library_id")
        if not data_set_id:
            raise ParameterError("data_set_id")
        body = {
            "dataLibraryID": data_library_id,
            "dataSetID": data_set_id,
        }
        optional = {
            "applicant": applicant,
            "topic": topic,
            "description": description,
            "organization": organization,
            "expiredTime": expired_time,
        }
        body.update({key: value for key, value in optional.items() if value is not None})
        return self.client.post("/api/repository/data_set/apply", body=body)
