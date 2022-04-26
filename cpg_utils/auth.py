import logging
from os import getenv
from typing import Mapping, Optional

from google.auth import jwt

from .config import get_deploy_config, get_server_config
from .secrets import read_secret


def check_dataset_access(dataset: str, user: str, access_type: str) -> bool:
    """Check that the user is a member of the dataset's {access_type} group."""
    server_config = get_server_config()
    if dataset not in server_config:
        return False

    dataset_id = server_config[dataset]["projectId"]
    group_members = read_secret(dataset_id, f'{dataset}-{access_type}-members-cache').split(',')
    return user in group_members


def get_user_from_headers(headers: Mapping[str, str]) -> Optional[str]:
    """Extract user email/SP from headers. Assumes caller has already been authenticated. """
    cloud_type = get_deploy_config().cloud

    # GCP fills in the 'x-goog-iap-jwt-assertion' header when running behind IAP.
    if cloud_type == "gcp" and (token := headers.get("x-goog-iap-jwt-assertion")):
        return jwt.decode(token, verify=False).get("email")

    # Azure fills in the 'x-ms-client-principal-name' header when running behind AppService/AAD.
    if cloud_type == "azure" and (user := headers.get("x-ms-client-principal-name")):
        return user

    if (auth := headers.get("Authorization")) and auth.startswith("Bearer "):
        return jwt.decode(auth[7:], verify=False).get("email")
   
    return None
