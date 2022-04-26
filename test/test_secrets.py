import json

import azure.identity
import azure.keyvault.secrets as secrets
import pytest
from cpg_utils.auth import check_dataset_access
from cpg_utils.config import DeployConfig, set_deploy_config
from cpg_utils.secrets import SecretManager
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
        return MockSecretResponse("supersecret,test1@test.com")
    def get_secret(self, secret_name):
        if secret_name == "server-config":
            return MockSecretResponse(TEST_SERVER_CONFIG)
        return MockSecretResponse("supersecret,test2@test.com")


def mock_get_client(*args, **kwargs):
    return MockSecretClient()


def test_gcp_secret(monkeypatch):
    monkeypatch.setattr(secretmanager, "SecretManagerServiceClient", mock_get_client)
    monkeypatch.setenv("CLOUD", "gcp")
    set_deploy_config(DeployConfig.from_environment())
    sm = SecretManager.get_secret_manager()
    assert sm.read_secret("test_host", "test_name") == "supersecret,test1@test.com"
    assert check_dataset_access("dataset1", "test1@test.com", "read") == True
    assert check_dataset_access("dataset1", "test2@test.com", "read") == False
    assert check_dataset_access("dataset2", "test2@test.com", "read") == False


def test_azure_secret(monkeypatch):
    monkeypatch.setattr(azure.identity, "DefaultAzureCredential", mock_get_client)
    monkeypatch.setattr(secrets, "SecretClient", mock_get_client)
    monkeypatch.setenv("CLOUD", "azure")
    set_deploy_config(DeployConfig.from_environment())
    sm = SecretManager.get_secret_manager()
    assert sm.read_secret("test_host", "test_name") == "supersecret,test2@test.com"
