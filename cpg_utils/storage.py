import logging
import os
import uuid
from abc import ABC, abstractmethod
from typing import Optional

import azure.core.exceptions
import azure.identity
import azure.storage.blob
import google.cloud.storage
import toml
from frozendict import frozendict

from .config import update_dict
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
    def get_blob(self, dataset: str, bucket_type: str, blob_path: str) -> Optional[bytes]:
        """Cloud-specific blob read. Returns None if blob doesn't exist."""

    @abstractmethod
    def set_blob(self, dataset: str, bucket_type: str, blob_path: str, contents: bytes) -> None:
        """Cloud-specific blob write."""

    @classmethod
    def get_base_config(cls) -> dict:
        """Read in base config toml."""
        template_path = os.path.join(os.path.dirname(__file__), 'config-template.toml')
        with open(template_path, "r") as base_config:
            config = toml.loads(base_config.read())
        return config

    @abstractmethod
    def get_job_config(self, config_name: str) -> frozendict:
        """Reads a job configuration file."""

    @abstractmethod
    def set_job_config(self, config: dict) -> str:
        """Writes a job configuration file."""


class DataManagerGCP(DataManager):
    """GCP Storage wrapper for reading/writing cloud dataset blobs."""

    _storage_client: google.cloud.storage.Client = None

    def __init__(self):
        """Loads GCP credentials and caches storage client."""
        self._storage_client = google.cloud.storage.Client()

    def get_blob(self, dataset: str, bucket_type: str, blob_path: str) -> Optional[bytes]:
        """Reads a GCP storage bucket blob."""
        bucket_name = f"cpg-{dataset}-{bucket_type}"
        bucket = self._storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_path)
    
        return None if blob is None else blob.download_as_bytes()

    def set_blob(self, dataset: str, bucket_type: str, blob_path: str, contents: bytes) -> None:
        """Writes a GCP storage bucket blob."""
        bucket_name = f"cpg-{dataset}-{bucket_type}"
        bucket = self._storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        with blob.open(mode="wb") as f:
            f.write(contents)

    def get_job_config(self, config_name: str) -> frozendict:
        """Reads a GCP job configuration file."""
        # TODO GRS need deployment-specific location.
        bucket = self._storage_client.bucket("cpg-config")
        blob = bucket.get_blob(config_name)
        config_string = blob.download_as_bytes().decode('utf-8')

        # Override base config with job-specific config.
        config = DataManager.get_base_config()
        update_dict(config, toml.loads(config_string))
        return frozendict(config)

    def set_job_config(self, config: dict) -> str:
        """Writes a GCP job configuration file."""
        # TODO GRS need deployment-specific location.
        config_name = str(uuid.uuid4()) + ".toml"
        contents = toml.dumps(config).encode("utf-8")
        bucket = self._storage_client.bucket("cpg-config")
        blob = bucket.blob(config_name)
        with blob.open(mode="wb") as f:
            f.write(contents)
        return config_name


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

    def get_storage_url(self, dataset: str) -> str:
        """Gets storage host URL based on dataset name."""
        # Need to map dataset name to storage account name.
        server_config = get_server_config()
        if dataset not in server_config:
            raise ValueError(f"No such dataset in server config: {dataset}")
        dataset_account = server_config[dataset]["projectId"]
        return f"https://{dataset_account}sa.blob.core.windows.net"

    def get_blob(self, dataset: str, bucket_type: str, blob_path: str) -> Optional[bytes]:
        """Reads an Azure storage blob."""
        storage_url = self.get_storage_url(dataset)
        container_name = f"cpg-{dataset}-{bucket_type}"
        blob_client = azure.storage.blob.BlobClient(
            storage_url, container_name, blob_path, credential=self._credential
        )
        try:
            download_stream = blob_client.download_blob()
        except azure.core.exceptions.ResourceNotFoundError:
            return None

        return download_stream.readall()

    def set_blob(self, dataset: str, bucket_type: str, blob_path: str, contents: bytes) -> None:
        """Writes an Azure storage blob."""
        storage_url = self.get_storage_url(dataset)
        container_name = f"cpg-{dataset}-{bucket_type}"
        blob_client = azure.storage.blob.BlobClient(
            storage_url, container_name, blob_path, credential=self._credential
        )
        blob_client.upload_blob(data=contents, overwrite=True)

    def _get_config_client(self, config_name: str) -> azure.storage.blob.BlobClient:
        """Gets storage client for configuration account."""
        storage_account = get_deploy_config().analysis_runner_project + "tfsa"
        storage_url = f"https://{storage_account}.blob.core.windows.net"
        blob_client = azure.storage.blob.BlobClient(
            storage_url, "config", config_name, credential=self._credential
        )
        return blob_client

    def get_job_config(self, config_name: str) -> frozendict:
        """Reads an Azure job configuration file."""
        blob_client = self._get_config_client(config_name)
        download_stream = blob_client.download_blob()
        config_string = download_stream.readall().decode('utf-8')

        # Override base config with job-specific config.
        config = DataManager.get_base_config()
        update_dict(config, toml.loads(config_string))
        return frozendict(config)

    def set_job_config(self, config: dict) -> str:
        """Writes an Azure job configuration file."""
        contents = toml.dumps(config).encode("utf-8")
        config_name = str(uuid.uuid4()) + ".toml"
        blob_client = self._get_config_client(config_name)
        blob_client.upload_blob(data=contents, overwrite=False)
        return config_name


def get_data_manager() -> DataManager:
    global data_manager
    if data_manager is None:
        data_manager = DataManager.get_data_manager()
    return data_manager


def get_job_config(config_name: Optional[str]) -> frozendict:
    """Reads the given config name from storage to a dictionary."""
    return get_data_manager().get_job_config(config_name or os.getenv(CPG_CONFIG_PATH))


def set_job_config(config: dict) -> str:
    """Writes the given config dictionary to a blob and returns its unique name."""
    return get_data_manager().set_job_config(config)

