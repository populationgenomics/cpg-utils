import json
from os import getenv
from typing import List, Mapping, Optional

import azure.identity
import google.auth
import google.auth.exceptions
import google.auth.transport.requests

from .config import get_deploy_config, get_server_config
from .secrets import read_secret


def get_dataset_access_list(dataset: str, access_type: str) -> List[str]:
    """Get the comma-separated list of members of a dataset's {access_type} group."""
    server_config = get_server_config()
    if dataset not in server_config:
        return []

    dataset_id = server_config[dataset]["projectId"]
    group_membership = read_secret(dataset_id, f"{dataset}-{access_type}-members-cache")
    return group_membership.split(",")
 

def check_dataset_access(dataset: str, user: str, access_type: str) -> bool:
    """Check that the user is a member of the dataset's {access_type} group."""
    group_members = get_dataset_access_list(dataset, access_type)
    return user in group_members


def get_user_from_headers(headers: Mapping[str, str]) -> Optional[str]:
    """Extract user email/SP from headers. Assumes caller has already been authenticated. """
    cloud_type = get_deploy_config().cloud

    # GCP fills in the 'x-goog-iap-jwt-assertion' header when running behind IAP.
    if cloud_type == "gcp" and (token := headers.get("x-goog-iap-jwt-assertion")):
        return google.auth.jwt.decode(token, verify=False).get("email")

    # Azure fills in the 'x-ms-client-principal-name' header when running behind AppService/AAD.
    if cloud_type == "azure" and (user := headers.get("x-ms-client-principal-name")):
        return user

    if (auth := headers.get("Authorization")) and auth.startswith("Bearer "):
        return google.auth.jwt.decode(auth[7:], verify=False).get("email")
   
    return None


def get_sample_metadata_token() -> str:
    """Get sample-metadata Bearer auth token for Azure or GCP depending on deployment config."""
    deploy_config = get_deploy_config()

    if deploy_config.cloud == "azure":
        scope = f"api://smapi-{deploy_config.sample_metadata_project}/.default"
        return get_azure_auth_token(scope)

    assert deploy_config.cloud == "gcp"
    audience = deploy_config.sample_metadata_host
    return get_google_auth_token(audience)


def get_azure_auth_token(scope: str) -> str:
    """
    Get Azure auth token in one of two ways:
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
        credential = azure.identity.DefaultAzureCredential()

    return credential.get_token(scope)


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
        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        return creds.id_token
