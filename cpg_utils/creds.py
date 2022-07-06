
import json
from os import getenv

import azure.identity
import google.auth
import google.auth.exceptions
import google.auth.transport.requests


def get_azure_auth_token(scope: str) -> str:
    """Get Azure auth token for a particular scope"""
    return get_azure_credentials().get_token(scope).token


def get_azure_credentials() -> object:
    """
    Get Azure client credentials in one of two ways:
    - if AZURE_APPLICATION_CREDENTIALS is set, then grab from there
    - otherwise let the azure MSAL figure out the default 
    """
    if (credentials_filename := getenv("AZURE_APPLICATION_CREDENTIALS")):
        with open(credentials_filename, "r") as f:
            credentials = json.loads(f.read())
        credential = azure.identity.ClientSecretCredential(
            tenant_id=credentials["tenant"],
            client_id=credentials["appId"],
            client_secret=credentials["password"]
        )
    else:
        # EnvironmentCredential, ManagedIdentityCredential, AzureCliCredential
        credential = azure.identity.DefaultAzureCredential(
            exclude_powershell_credential = True,
            exclude_visual_studio_code_credential = True,
            exclude_shared_token_cache_credential = True,
            exclude_interactive_browser_credential = True
        )

    return credential


def get_google_auth_token(audience: str) -> str:
    """
    Get google auth token in one of two ways:
    - if GOOGLE_APPLICATION_CREDENTIALS is set, then grab from there
    - or run the equivalent of 'gcloud auth print-identity-token'
    ie: use service account identity token by default, then fallback otherwise
    https://stackoverflow.com/a/55804230
    """
    if (credentials_filename := getenv("GOOGLE_APPLICATION_CREDENTIALS")):
        with open(credentials_filename, "r") as f:
            from google.oauth2 import service_account

            info = json.load(f)
            credentials_content = (info.get("type") == "service_account") and info or None
            credentials = service_account.IDTokenCredentials.from_service_account_info(
                credentials_content, target_audience=audience
            )
            auth_req = google.auth.transport.requests.Request()
            credentials.refresh(auth_req)
            return credentials.token
    else:
        creds, _ = google.auth.default()
        creds.refresh(google.auth.transport.requests.Request())
        return creds.id_token


def get_analysis_runner_token() -> str:
    """Get analysis-runner Bearer auth token for Azure or GCP depending on deployment config."""
    from .deploy_config import get_deploy_config
    deploy_config = get_deploy_config()

    if deploy_config.cloud == "azure":
        scope = f"api://arapi-{deploy_config.analysis_runner_project}/.default"
        return get_azure_auth_token(scope)

    assert deploy_config.cloud == "gcp"
    audience = deploy_config.analysis_runner_host
    return get_google_auth_token(audience)


def get_sample_metadata_token() -> str:
    """Get sample-metadata Bearer auth token for Azure or GCP depending on deployment config."""
    from .deploy_config import get_deploy_config
    deploy_config = get_deploy_config()

    if deploy_config.cloud == "azure":
        scope = f"api://smapi-{deploy_config.sample_metadata_project}/.default"
        return get_azure_auth_token(scope)

    assert deploy_config.cloud == "gcp"
    audience = deploy_config.sample_metadata_host
    return get_google_auth_token(audience)
