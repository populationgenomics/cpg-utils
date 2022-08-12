from typing import List, Mapping, Optional

import google.auth

from .deploy_config import get_deploy_config


def get_dataset_access_list(dataset: str, access_type: str) -> List[str]:
    """Get the comma-separated list of members of a dataset's {access_type} group."""
    deploy_config = get_deploy_config()
    membership_key = f"{dataset}-{access_type}-members-cache"
    group_membership = deploy_config.read_dataset_config(dataset, membership_key)
    return group_membership.split(",")


def check_dataset_access(dataset: str, user: str, access_type: str) -> bool:
    """Check that the user is a member of the dataset's {access_type} group."""
    group_members = get_dataset_access_list(dataset, access_type)
    return user in group_members


def get_global_access_list(access_type: str) -> List[str]:
    """Get the comma-separated list of members of a global membership group."""
    group_membership = get_deploy_config().read_global_config("project-creator-users")
    return group_membership.split(",")


def check_global_access(user: str, access_type: str) -> bool:
    """Check that the user is a member of the global {access_type} group."""
    group_members = get_global_access_list(access_type)
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
