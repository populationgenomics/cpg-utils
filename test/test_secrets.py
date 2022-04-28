import json

import azure.identity
import azure.keyvault.secrets as secrets
import pytest
from cpg_utils.auth import check_dataset_access
from cpg_utils.config import set_deploy_config_from_env
from cpg_utils.secrets import (
    SecretManager,
    SecretManagerGCP,
    get_default_secret_manager,
    set_default_secret_manager
)
from google.cloud import secretmanager

TEST_SERVER_CONFIG = json.dumps({
	"dataset1": {
		"projectId": "dataset1_id",
		"allowedRepos": ["sample-metadata", "fewgenomes"],
		"testToken": "Hail test SA account",
		"standardToken": "Hail standard SA account",
		"fullToken": "Hail full SA account" 
	}
})


class MockSecretResponse:
    class Payload:
        def __init__(self, secret_value):
            self.data = bytes(secret_value, "UTF-8")
    def __init__(self, secret_value):
        self.payload = MockSecretResponse.Payload(secret_value)
        self.value = secret_value


class MockSecretClient:
    def secret_path(self, secret_host, secret_name):
        return secret_host + "/" + secret_name
    def access_secret_version(self, request):
        if "server-config" in request["name"]:
            return MockSecretResponse(TEST_SERVER_CONFIG)
        if "test_name" in request["name"]:
            return MockSecretResponse("supersecret in gcp")
        assert request["name"] == "dataset1_id/dataset1-read-members-cache/versions/latest"
        return MockSecretResponse("me@example.com,test1@test.com")
    def get_secret(self, secret_name):
        if secret_name == "server-config":
            return MockSecretResponse(TEST_SERVER_CONFIG)
        if secret_name == "test_name":
            return MockSecretResponse("supersecret in azure")
        assert secret_name == "dataset1-read-members-cache"
        return MockSecretResponse("me@example.com,test2@test.com")


def mock_get_client(*args, **kwargs):
    return MockSecretClient()


def test_cloud_secret(monkeypatch):
    monkeypatch.setattr(secretmanager, "SecretManagerServiceClient", mock_get_client)
    monkeypatch.setenv("CLOUD", "gcp")
    set_deploy_config_from_env()
    sm = get_default_secret_manager()
    assert isinstance(sm, SecretManagerGCP)


def test_gcp_secret(monkeypatch):
    monkeypatch.setattr(secretmanager, "SecretManagerServiceClient", mock_get_client)
    sm = SecretManager.get_secret_manager("gcp")
    assert sm.read_secret("test_host", "test_name") == "supersecret in gcp"

    set_default_secret_manager(sm)
    assert check_dataset_access("dataset1", "test1@test.com", "read") == True
    assert check_dataset_access("dataset1", "test2@test.com", "read") == False
    assert check_dataset_access("dataset2", "test2@test.com", "read") == False


def test_azure_secret(monkeypatch):
    monkeypatch.setattr(azure.identity, "DefaultAzureCredential", mock_get_client)
    monkeypatch.setattr(secrets, "SecretClient", mock_get_client)
    sm = SecretManager.get_secret_manager("azure")
    assert sm.read_secret("test_host", "test_name") == "supersecret in azure"

    set_default_secret_manager(sm)
    assert check_dataset_access("dataset1", "test1@test.com", "read") == False
    assert check_dataset_access("dataset1", "test2@test.com", "read") == True
    assert check_dataset_access("dataset2", "test2@test.com", "read") == False
