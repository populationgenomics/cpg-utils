"""Convenience functions related to cloud infrastructure."""

import json
import os
import traceback

from google.auth import (
    credentials as google_auth_credentials,
    environment_vars,
    exceptions,
)
from google.auth import jwt
from google.auth._default import (
    _AUTHORIZED_USER_TYPE,
    _HELP_MESSAGE,
    _SERVICE_ACCOUNT_TYPE,
    _VALID_TYPES,
)
from google.cloud import secretmanager
from google.oauth2 import credentials as oauth2_credentials, service_account
import google.api_core.exceptions
import google.auth.transport
from google.auth.transport import requests
import google.oauth2


def email_from_id_token(id_token_jwt: str) -> str:
    """Decodes the ID token (JWT) to get the email address of the caller.

    See http://bit.ly/2YAIkzy for details.

    This function assumes that the token has been verified beforehand."""

    return jwt.decode(id_token_jwt, verify=False)['email']


def read_secret(
    project_id: str,
    secret_name: str,
    fail_gracefully: bool = True,
) -> str | None:
    """Reads the latest version of a GCP Secret Manager secret.

    Returns None if the secret doesn't exist or there was a problem retrieving it,
    unless `fail_gracefully` is set to False."""

    secret_manager = secretmanager.SecretManagerServiceClient()
    secret_path = secret_manager.secret_version_path(project_id, secret_name, 'latest')

    try:
        # noinspection PyTypeChecker
        response = secret_manager.access_secret_version(request={'name': secret_path})
        return response.payload.data.decode('UTF-8')
    except google.api_core.exceptions.ClientError:
        # Fail gracefully if there's no secret version yet.
        if fail_gracefully:
            traceback.print_exc()
            return None
        raise
    except AttributeError:
        # Sometimes the google API fails when no version is present, with:
        #   File "{site-packages}/google/api_core/exceptions.py",
        #   line 532, in from_grpc_error if isinstance(rpc_exc, grpc.Call) or _is_informative_grpc_error(rpc_exc):
        #   AttributeError: 'NoneType' object has no attribute 'Call'
        if fail_gracefully:
            traceback.print_exc()
            return None
        raise


def write_secret(project_id: str, secret_name: str, secret_value: str) -> None:
    """
    Adds a new version for a GCP Secret Manager secret and disables all previous versions

    Parameters
    ----------
    project_id
    secret_name
    secret_value

    Returns
    -------

    """

    secret_manager = secretmanager.SecretManagerServiceClient()
    secret_path = secret_manager.secret_path(project_id, secret_name)

    response = secret_manager.add_secret_version(
        request={
            'parent': secret_path,
            'payload': {'data': secret_value.encode('UTF-8')},
        }
    )

    # Disable all previous versions.
    for version in secret_manager.list_secret_versions(request={'parent': secret_path}):
        # Don't attempt to change the state of destroyed / already disabled secrets and
        # don't disable the latest version.
        if (
            version.state == secretmanager.SecretVersion.State.ENABLED
            and version.name != response.name
        ):
            secret_manager.disable_secret_version(request={'name': version.name})


def get_google_identity_token(
    target_audience: str | None, request: google.auth.transport.Request = None
) -> str:
    """Returns a Google identity token for the given audience."""
    if request is None:
        request = requests.Request()
    # Unfortunately this requires different handling for at least
    # three different cases and the standard libraries don't provide
    # a single helper function that captures all of them:
    # https://github.com/googleapis/google-auth-library-python/issues/590
    creds = _get_default_id_token_credentials(target_audience, request)
    creds.refresh(request)
    return creds.token


class IDTokenCredentialsAdapter(google_auth_credentials.Credentials):
    """Convert Credentials with ``openid`` scope to IDTokenCredentials."""

    def __init__(self, credentials: oauth2_credentials.Credentials):
        super().__init__()
        self.credentials = credentials
        self.token = credentials.id_token

    @property
    def expired(self):
        """Returns the expired property."""
        return self.credentials.expired

    def refresh(self, request):
        """Refreshes the token."""
        self.credentials.refresh(request)
        self.token = self.credentials.id_token


def _load_credentials_from_file(
    filename: str, target_audience: str | None
) -> google_auth_credentials.Credentials | None:
    """
    Loads credentials from a file.
    The credentials file must be a service account key or a stored authorized user credential.
    :param filename: The full path to the credentials file.
    :return: Loaded credentials
    :rtype: google.auth.credentials.Credentials
    :raise google.auth.exceptions.DefaultCredentialsError: if the file is in the wrong format or is missing.
    """
    if not os.path.exists(filename):
        raise exceptions.DefaultCredentialsError(f'File {filename} was not found.')

    with open(filename, encoding='utf-8') as file_obj:
        try:
            info = json.load(file_obj)
        except json.JSONDecodeError as exc:
            raise exceptions.DefaultCredentialsError(
                f'File {filename} is not a valid json file.'
            ) from exc

    # The type key should indicate that the file is either a service account
    # credentials file or an authorized user credentials file.
    credential_type = info.get('type')

    if credential_type == _AUTHORIZED_USER_TYPE:
        current_credentials = oauth2_credentials.Credentials.from_authorized_user_info(
            info, scopes=['openid', 'https://www.googleapis.com/auth/userinfo.email']
        )
        current_credentials = IDTokenCredentialsAdapter(credentials=current_credentials)

        return current_credentials

    if credential_type == _SERVICE_ACCOUNT_TYPE:
        try:
            return service_account.IDTokenCredentials.from_service_account_info(
                info, target_audience=target_audience
            )
        except ValueError as exc:
            raise exceptions.DefaultCredentialsError(
                f'Failed to load service account credentials from {filename}'
            ) from exc

    raise exceptions.DefaultCredentialsError(
        f'The file {filename} does not have a valid type. Type is {credential_type}, '
        f'expected one of {_VALID_TYPES}.'
    )


def _get_explicit_environ_credentials(
    target_audience: str | None,
) -> google_auth_credentials.Credentials | None:
    """Gets credentials from the GOOGLE_APPLICATION_CREDENTIALS environment variable."""
    explicit_file = os.environ.get(environment_vars.CREDENTIALS)

    if explicit_file is None:
        return None

    current_credentials = _load_credentials_from_file(
        os.environ[environment_vars.CREDENTIALS], target_audience=target_audience
    )

    return current_credentials


def _get_gcloud_sdk_credentials(
    target_audience: str | None,
) -> google_auth_credentials.Credentials | None:
    """Gets the credentials and project ID from the Cloud SDK."""
    from google.auth import _cloud_sdk  # pylint: disable=import-outside-toplevel

    # Check if application default credentials exist.
    credentials_filename = _cloud_sdk.get_application_default_credentials_path()

    if not os.path.isfile(credentials_filename):
        return None

    current_credentials = _load_credentials_from_file(
        credentials_filename, target_audience
    )

    return current_credentials


def _get_gce_credentials(
    target_audience: str | None, request: google.auth.transport.Request | None = None
) -> google_auth_credentials.Credentials | None:
    """Gets credentials and project ID from the GCE Metadata Service."""
    # Ping requires a transport, but we want application default credentials
    # to require no arguments. So, we'll use the _http_client transport which
    # uses http.client. This is only acceptable because the metadata server
    # doesn't do SSL and never requires proxies.

    # While this library is normally bundled with compute_engine, there are
    # some cases where it's not available, so we tolerate ImportError.

    # pylint: disable=import-outside-toplevel
    try:
        from google.auth import compute_engine
        from google.auth.compute_engine import _metadata
    except ImportError:
        return None

    from google.auth.transport import _http_client

    if request is None:
        request = _http_client.Request()

    if _metadata.ping(request=request):
        return compute_engine.IDTokenCredentials(
            request, target_audience, use_metadata_identity_endpoint=True
        )

    return None


def _get_default_id_token_credentials(
    target_audience: str | None, request: google.auth.transport.Request = None
) -> google_auth_credentials.Credentials:
    """Gets the default ID Token credentials for the current environment.
    `Application Default Credentials`_ provides an easy way to obtain credentials to call Google APIs for
    server-to-server or local applications.
    .. _Application Default Credentials: https://developers.google.com\
        /identity/protocols/application-default-credentials
    :param target_audience: The intended audience for these credentials.
    :param request: An object used to make HTTP requests. This is used to detect whether the application
            is running on Compute Engine. If not specified, then it will use the standard library http client
            to make requests.
    :return: the current environment's credentials.
    :rtype: google.auth.credentials.Credentials
    :raises ~google.auth.exceptions.DefaultCredentialsError:
        If no credentials were found, or if the credentials found were invalid.
    """
    checkers = (
        lambda: _get_explicit_environ_credentials(target_audience),
        lambda: _get_gcloud_sdk_credentials(target_audience),
        lambda: _get_gce_credentials(target_audience, request),
    )

    for checker in checkers:
        current_credentials = checker()
        if current_credentials is not None:
            return current_credentials

    raise exceptions.DefaultCredentialsError(_HELP_MESSAGE)
