import logging
from abc import ABC, abstractmethod
from os import getenv
from typing import Optional

import azure.identity
import google.cloud.storage
import azure.storage.blob

from .config import get_deploy_config, get_server_config

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
    def get_blob(self, dataset: str, bucket_type: str, blob_path: str) -> bytes:
        """Cloud-specific blob fetch."""


class DataManagerGCP(DataManager):
    """GCP Storage wrapper for reading/writing cloud dataset blobs."""

    _storage_client: google.cloud.storage.Client = None

    def __init__(self):
        """Loads GCP credentials and caches storage client."""
        self._storage_client = google.cloud.storage.Client()

    def get_blob(self, dataset: str, bucket_type: str, blob_path: str) -> bytes:
        """Reads a GCP storage bucket blob."""
        bucket_name = f"cpg-{dataset}-{bucket_type}"
        bucket = self._storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_path)
    
        return None if blob is None else blob.download_as_bytes()


class DataManagerAzure(DataManager):
    """Azure Storage wrapper for reading/writing cloud dataset blobs."""

    _credential = None

    def __init__(self):
        # EnvironmentCredential, ManagedIdentityCredential, AzureCliCredential
        self._credential = azure.identity.DefaultAzureCredential(
            exclude_powershell_credential = True,
            exclude_visual_studio_code_credential = True,
            exclude_shared_token_cache_credential = True,
            exclude_interactive_browser_credential = True
        )

    def get_blob(self, dataset: str, bucket_type: str, blob_path: str) -> bytes:
        """Reads an Azure storage blob."""
        # Need to map dataset name to storage account name.
        server_config = get_server_config()
        if dataset not in server_config:
            raise ValueError(f"No such dataset in server config: {dataset}")

        dataset_account = server_config[dataset]["projectId"]
        storage_url = f"https://{dataset_account}sa.blob.core.windows.net"
        container_name = f"cpg-{dataset}-{bucket_type}"
        blob_client = azure.storage.blob.BlobClient(
            storage_url, container_name, blob_path, credential=self._credential
        )
        download_stream = blob_client.download_blob()

        return download_stream.readall()


def get_data_manager() -> DataManager:
    global data_manager
    if data_manager is None:
        data_manager = DataManager.get_data_manager()
    return data_manager


