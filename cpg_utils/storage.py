from abc import ABC, abstractmethod
from typing import Dict, Optional

import azure.core.exceptions
import azure.identity
import azure.storage.blob
import google.cloud.storage

from .creds import get_azure_credentials
from .deploy_config import get_deploy_config, get_server_config

data_manager: "DataManager" = None

class DataManager(ABC):
    """Multi-cloud abstraction for reading/writing cloud dataset blobs."""

    @staticmethod
    def get_data_manager(cloud_type: Optional[str] = None) -> "DataManager":
        """Instantiates a DataManager object of the appropriate cloud type."""
        if not cloud_type:
            cloud_type = get_deploy_config().cloud
        if cloud_type == "azure":
            return DataManagerAzure()
        assert cloud_type == "gcp"
        return DataManagerGCP()

    @abstractmethod
    def get_dataset_bucket_url(self, dataset: str, bucket_type: str) -> str:
        """Build dataset-specific bucket URL with Hail-style scheme ("gs:" or "hail-az:")."""

    @abstractmethod
    def get_blob(self, dataset: Optional[str], bucket_type: str, blob_path: str) -> Optional[bytes]:
        """Cloud-specific blob read. Returns None if blob doesn't exist."""

    @abstractmethod
    def set_blob(self, dataset: str, bucket_type: str, blob_path: str, contents: bytes) -> None:
        """Cloud-specific blob write."""


class DataManagerGCP(DataManager):
    """GCP Storage wrapper for reading/writing cloud dataset blobs."""

    _storage_client: google.cloud.storage.Client = None

    def __init__(self):
        """Loads GCP credentials and caches storage client."""
        self._storage_client = google.cloud.storage.Client()

    def get_dataset_bucket_url(self, dataset: str, bucket_type: str) -> str:
        """Build dataset-specific Hail-style bucket URL for GCP ("gs://...")."""
        return f"gs://cpg-{dataset}-{bucket_type}"

    def get_blob(self, dataset: Optional[str], bucket_type: str, blob_path: str) -> Optional[bytes]:
        """Reads a GCP storage bucket blob."""
        bucket_name = f"cpg-{dataset}-{bucket_type}" if dataset else f"cpg-{bucket_type}"
        bucket = self._storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_path)
    
        return None if blob is None else blob.download_as_bytes()

    def set_blob(self, dataset: Optional[str], bucket_type: str, blob_path: str, contents: bytes) -> None:
        """Writes a GCP storage bucket blob."""
        bucket_name = f"cpg-{dataset}-{bucket_type}" if dataset else f"cpg-{bucket_type}"
        bucket = self._storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        with blob.open(mode="wb") as f:
            f.write(contents)


class DataManagerAzure(DataManager):
    """Azure Storage wrapper for reading/writing cloud dataset blobs."""

    _credential = None

    def __init__(self):
        self._credential = get_azure_credentials()

    def get_storage_url(self, dataset: Optional[str]) -> str:
        """Gets storage host URL based on dataset name or AR base (without scheme)."""
        if dataset:
            # Need to map dataset name to storage account name.
            server_config = get_server_config()
            if dataset not in server_config:
                raise ValueError(f"No such dataset in server config: {dataset}")
            account = server_config[dataset]["projectId"]
        else: # Otherwise use the base analysis_runner storage account.
            account = get_deploy_config().analysis_runner_project
        return f"{account}sa.blob.core.windows.net"

    def get_dataset_bucket_url(self, dataset: str, bucket_type: str) -> str:
        """Build dataset-specific Hail-style bucket URL for Azure ("hail-az://...")."""
        return f"hail-az://{self.get_storage_url(dataset)}/cpg-{dataset}-{bucket_type}"

    def get_blob(self, dataset: Optional[str], bucket_type: str, blob_path: str) -> Optional[bytes]:
        """Reads an Azure storage blob."""
        storage_url = "https://" + self.get_storage_url(dataset)
        container_name = f"cpg-{dataset}-{bucket_type}" if dataset else f"cpg-{bucket_type}"
        blob_client = azure.storage.blob.BlobClient(
            storage_url, container_name, blob_path, credential=self._credential
        )
        try:
            download_stream = blob_client.download_blob()
        except azure.core.exceptions.ResourceNotFoundError:
            return None

        return download_stream.readall()

    def set_blob(self, dataset: Optional[str], bucket_type: str, blob_path: str, contents: bytes) -> None:
        """Writes an Azure storage blob."""
        storage_url = "https://" + self.get_storage_url(dataset)
        container_name = f"cpg-{dataset}-{bucket_type}" if dataset else f"cpg-{bucket_type}"
        blob_client = azure.storage.blob.BlobClient(
            storage_url, container_name, blob_path, credential=self._credential
        )
        blob_client.upload_blob(data=contents, overwrite=True)


def get_data_manager() -> DataManager:
    global data_manager
    if data_manager is None:
        data_manager = DataManager.get_data_manager()
    return data_manager


def clear_data_manager() -> None:
    global data_manager
    data_manager = None


def get_dataset_bucket_url(dataset: str, bucket_type: str) -> str:
    """Return dataset-specific bucket URL with Hail-style scheme ("gs:" or "hail-az:")."""
    return get_data_manager().get_dataset_bucket_url(dataset, bucket_type)
