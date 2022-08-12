import logging
from abc import ABC, abstractmethod
from os import getenv
from typing import Optional

import azure.keyvault.secrets as secrets
from google.cloud import secretmanager

from .creds import get_azure_credentials


class SecretManager(ABC):
    """Multi-cloud abstraction for reading/writing cloud secrets."""

    @staticmethod
    def get_secret_manager(cloud_type: str) -> "SecretManager":
        if cloud_type == "azure":
            return SecretManagerAzure()
        assert cloud_type == "gcp"
        return SecretManagerGCP()

    @abstractmethod
    def read_secret(self, secret_host: str, secret_name: str):
        """Cloud-specific secret read."""


class SecretManagerGCP(SecretManager):
    """GCP Secret Manager wrapper for reading/writing cloud secrets."""

    _secret_client: secretmanager.SecretManagerServiceClient = None

    def __init__(self):
        """Loads GCP credentials and caches secrets client."""
        self._secret_client = secretmanager.SecretManagerServiceClient()

    def read_secret(self, secret_host: str, secret_name: str):
        """Reads the latest version of a GCP Secret Manager secret."""

        secret_path = self._secret_client.secret_path(secret_host, secret_name)
        response = self._secret_client.access_secret_version(
            request={"name": f"{secret_path}/versions/latest"}
        )

        return response.payload.data.decode("UTF-8")


class SecretManagerAzure(SecretManager):
    """Azure Key Vault wrapper for reading/writing cloud secrets."""

    _credential = None

    def __init__(self):
        self._credential = get_azure_credentials()

    def read_secret(self, secret_host: str, secret_name: str):
        """Reads an Azure Key Vault secret from the designated vault."""

        vault_url = f"https://{secret_host}.vault.azure.net"
        secret_client = secrets.SecretClient(vault_url=vault_url, credential=self._credential)
        response = secret_client.get_secret(secret_name)

        return response.value
