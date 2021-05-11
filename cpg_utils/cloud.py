"""Convenience functions related to cloud infrastructure."""

import logging
import googleapiclient.discovery

_CLOUD_IDENTITY_SERVICE_NAME = 'cloudidentity.googleapis.com'
_CLOUD_IDENTITY_API_VERSION = 'v1'
_DISCOVERY_URL = (
    f'https://{_CLOUD_IDENTITY_SERVICE_NAME}/$discovery/rest?'
    f'version={_CLOUD_IDENTITY_API_VERSION}'
)

_cloud_identity_service = googleapiclient.discovery.build(
    _CLOUD_IDENTITY_SERVICE_NAME,
    _CLOUD_IDENTITY_API_VERSION,
    discoveryServiceUrl=_DISCOVERY_URL,
)


def is_google_group_member(user: str, group: str) -> bool:
    """Returns whether the user is a member of the given Google group.

    Both user and group are specified as email addresses.

    Note:
    - This does *not* look up transitive memberships, i.e. nested groups.
    - The service account performing the lookup must be a member of the group itself,
      in order to have visiblity of all members.
    """

    try:
        # See https://bit.ly/37WcB1d for the API calls.
        # Pylint can't resolve the methods in Resource objects.
        # pylint: disable=E1101
        parent = (
            _cloud_identity_service.groups().lookup(groupKey_id=group).execute()['name']
        )

        _ = (
            _cloud_identity_service.groups()
            .memberships()
            .lookup(parent=parent, memberKey_id=user)
            .execute()['name']
        )

        return True
    except googleapiclient.errors.HttpError as e:  # Failed lookups result in a 403.
        logging.warning(e)
        return False
