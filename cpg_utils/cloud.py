"""Convenience functions related to cloud infrastructure."""

import contextlib
import json
import os
import re
import subprocess
import time
import traceback
import urllib.parse
from collections import defaultdict
from pathlib import Path
from typing import Any, NamedTuple

# pylint: disable=no-name-in-module
import google.api_core.exceptions
import google.auth.transport
from deprecated import deprecated
from google.auth import (
    credentials as google_auth_credentials,
)
from google.auth import (
    environment_vars,
    exceptions,
    jwt,
)
from google.auth._default import (
    _AUTHORIZED_USER_TYPE,
    _EXTERNAL_ACCOUNT_TYPE,
    _SERVICE_ACCOUNT_TYPE,
)
from google.auth.transport import requests
from google.cloud import artifactregistry, secretmanager
from google.oauth2 import credentials as oauth2_credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow

_CLOUD_SDK_MISSING_CREDENTIALS = """\
Your default credentials were not found. To set up Application Default Credentials, \
see https://cloud.google.com/docs/authentication/external/set-up-adc for more information.\
"""

IMPLEMENTED_CREDENTIALS_TYPES = (
    _AUTHORIZED_USER_TYPE,
    _SERVICE_ACCOUNT_TYPE,
    _EXTERNAL_ACCOUNT_TYPE,
)


def email_from_id_token(id_token_jwt: str) -> str:
    """Decodes the ID token (JWT) to get the email address of the caller.

    See for details
        https://developers.google.com/identity/sign-in/web/backend-auth?authuser=0#verify-the-integrity-of-the-id-token

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
        },
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


class DockerImage(NamedTuple):
    name: str
    uri: str
    tag_uri: str
    size: str
    build_time: str


_repo_image_tags: dict[str, defaultdict[str, dict[str, DockerImage]]] = {}


def _ensure_image_tags_loaded(project: str, location: str, repository: str) -> None:
    """Populate _repo_image_tags as a map-of-map-of-maps of 'repository' -> 'imagename' -> 'tag' -> image."""
    if repository in _repo_image_tags:
        return

    image_tags: defaultdict[str, dict[str, DockerImage]] = defaultdict(dict)

    request = artifactregistry.ListDockerImagesRequest(
        parent=f'projects/{project}/locations/{location}/repositories/{repository}',
        page_size=500,  # Increase efficiency by making fewer requests
    )
    for image in artifactregistry.ArtifactRegistryClient().list_docker_images(request):
        name_and_checksum = image.name.rpartition('/dockerImages/')[2]
        name = urllib.parse.unquote(name_and_checksum).rpartition('@')[0]
        base_uri = image.uri.rpartition('@')[0]
        for tag in image.tags:
            image_tags[name][tag] = DockerImage(
                image.name,
                image.uri,
                f'{base_uri}:{tag}',
                image.image_size_bytes,
                image.build_time,
            )

    image_tags.default_factory = None
    _repo_image_tags[repository] = image_tags


def find_image(repository: str | None, name: str, version: str) -> DockerImage:
    """Returns image details or raises ValueError if the image or tag does not exist."""
    repository = f'images-{repository}' if repository is not None else 'images'
    _ensure_image_tags_loaded('cpg-common', 'australia-southeast1', repository)
    try:
        return _repo_image_tags[repository][name][version]
    except KeyError as e:
        message = f'Image {name}:{version} not found in {repository} repository ({e} not found)'
        raise ValueError(message) from None


class IAPDesktopCredentialsAdapter(google_auth_credentials.Credentials):
    """Handle the OAuth flow for desktop applications to get an ID token with the IAP client as audience."""

    OAUTH_AUTH_URL = 'https://accounts.google.com/o/oauth2/v2/auth'
    OAUTH_TOKEN_URL = 'https://oauth2.googleapis.com/token'  # noqa: S105 - Public Google API endpoint, not a password
    TOKEN_CACHE_DIR = Path.home() / '.config' / 'cpg' / 'tokens'
    TOKEN_CACHE_FILE = TOKEN_CACHE_DIR / 'iap_tokens.json'

    def __init__(
        self,
        iap_client_id: str,
        desktop_client_id: str,
        desktop_client_secret: str,
    ):
        super().__init__()
        self.iap_client_id = iap_client_id
        self.desktop_client_id = desktop_client_id
        self.desktop_client_secret = desktop_client_secret
        self.token: str | None = None

    def _load_cache(self) -> dict:
        """Load the token cache file."""
        if not self.TOKEN_CACHE_FILE.exists():
            return {}
        try:
            with open(self.TOKEN_CACHE_FILE) as f:
                full_cache = json.load(f)

            # The cache is a nested dict: {iap_client_id: {desktop_client_id: { ...tokens... }}}
            iap_cache = full_cache.get(self.iap_client_id, {})
            return iap_cache.get(self.desktop_client_id, {})
        except (json.JSONDecodeError, KeyError):
            return {}

    def _save_cache(
        self,
        id_token: str | None = None,
        id_token_expires_at: float | None = None,
        refresh_token: str | None = None,
    ) -> None:
        """Save data to the token cache file."""
        self.TOKEN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        # Ensure only the user can access the tokens directory
        self.TOKEN_CACHE_DIR.chmod(0o700)

        full_cache = {}
        if self.TOKEN_CACHE_FILE.exists():
            try:
                with open(self.TOKEN_CACHE_FILE) as f:
                    full_cache = json.load(f)
            except (json.JSONDecodeError, KeyError):
                pass

        if self.iap_client_id not in full_cache:
            full_cache[self.iap_client_id] = {}

        if self.desktop_client_id not in full_cache[self.iap_client_id]:
            full_cache[self.iap_client_id][self.desktop_client_id] = {}

        # Update the specific nested dictionary
        cache_data = full_cache[self.iap_client_id][self.desktop_client_id]
        if id_token is not None:
            cache_data['id_token'] = id_token
        if id_token_expires_at is not None:
            cache_data['id_token_expires_at'] = id_token_expires_at
        if refresh_token is not None:
            cache_data['refresh_token'] = refresh_token

        # Write to file with restricted permissions (set via directory)
        with open(self.TOKEN_CACHE_FILE, 'w') as f:
            json.dump(full_cache, f)
        # Set file permissions explicitly
        self.TOKEN_CACHE_FILE.chmod(0o600)

    def _get_cached_id_token(self) -> str | None:
        """Get cached ID token if it exists and is not expired."""
        cache = self._load_cache()
        id_token = cache.get('id_token')
        expires_at = cache.get('id_token_expires_at', 0)

        # Check if we have an ID token and it expires in more than 5 mins (300 seconds)
        if id_token and expires_at and time.time() < (expires_at - 300):
            return id_token

        return None

    def _cache_id_token(self, id_token: str) -> None:
        """Cache the ID token with its expiration time."""
        try:
            claims = jwt.decode(id_token, verify=False)
            expires_at = claims.get('exp', 0)
            self._save_cache(id_token=id_token, id_token_expires_at=expires_at)
        except ValueError:
            # If the token is invalid or cannot be decoded, simply do not cache it
            pass

    def _do_oauth_flow(self) -> str:
        """Perform the OAuth 2.0 authorization flow for desktop apps."""
        flow = InstalledAppFlow.from_client_config(
            {
                'installed': {
                    'client_id': self.desktop_client_id,
                    'client_secret': self.desktop_client_secret,
                    'auth_uri': self.OAUTH_AUTH_URL,
                    'token_uri': self.OAUTH_TOKEN_URL,
                },
            },
            scopes=['openid', 'https://www.googleapis.com/auth/userinfo.email'],
        )

        # This will open the browser and start a local server
        creds = flow.run_local_server(port=0, prompt='consent')

        refresh_token = creds.refresh_token
        if not refresh_token:
            raise RuntimeError(
                'No refresh token in response. Try revoking access and re-authenticating.',
            )

        self._save_cache(refresh_token=refresh_token)
        return refresh_token

    def _get_id_token_from_refresh_token(
        self,
        request: google.auth.transport.Request,
        refresh_token: str,
    ) -> str:
        """Exchange refresh token for an ID token with the IAP client as aud."""
        token_data = {
            'client_id': self.desktop_client_id,
            'client_secret': self.desktop_client_secret,
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token',
            'audience': self.iap_client_id,
        }

        # Use the provided transport request instead of the requests library directly
        response = request(
            url=self.OAUTH_TOKEN_URL,
            method='POST',
            body=urllib.parse.urlencode(token_data).encode('utf-8'),
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
        )

        response_body = response.data.decode('utf-8')

        if response.status != 200:  # noqa: PLR2004 - 200 is fine
            error_info = response_body

            with contextlib.suppress(json.JSONDecodeError):
                error_info = json.loads(response_body)

            bad_request = 400
            if response.status == bad_request and 'invalid_grant' in str(error_info):
                raise RuntimeError(
                    'Refresh token expired or revoked. Re-authentication required.',
                )
            raise RuntimeError(f'Token refresh failed: {error_info}')

        tokens = json.loads(response_body)
        id_token = tokens.get('id_token')

        if not id_token:
            raise RuntimeError('No ID token in response')

        return id_token

    def refresh(self, request: google.auth.transport.Request) -> None:
        """Get an ID token from cache if possible, otherwise attempt to refresh, otherwise perform the OAuth flow."""
        cached_id_token = self._get_cached_id_token()
        if cached_id_token:
            self.token = cached_id_token
            return

        cache = self._load_cache()
        refresh_token = cache.get('refresh_token')

        if refresh_token:
            try:
                id_token = self._get_id_token_from_refresh_token(request, refresh_token)
                self._cache_id_token(id_token)
                self.token = id_token
                return
            except RuntimeError as e:
                # If the refresh token is expired or revoked, clear it and fall through to do the flow again.
                if 'expired' in str(e).lower() or 'revoked' in str(e).lower():
                    refresh_token = None
                    self._save_cache(refresh_token=None)
                else:
                    raise

        if not refresh_token:
            refresh_token = self._do_oauth_flow()

        id_token = self._get_id_token_from_refresh_token(request, refresh_token)
        self._cache_id_token(id_token)
        self.token = id_token


def get_google_identity_token(
    target_audience: str | None,
    request: google.auth.transport.Request | None = None,
    enable_desktop_auth: bool = False,
    desktop_client_id: str | None = None,
    desktop_client_secret: str | None = None,
) -> str:
    """Returns a Google identity token for the given audience."""
    if request is None:
        request = requests.Request()
    # Unfortunately this requires different handling for at least
    # three different cases and the standard libraries don't provide
    # a single helper function that captures all of them:
    # https://github.com/googleapis/google-auth-library-python/issues/590
    creds = _get_default_id_token_credentials(target_audience, request)

    # Application default credentials with _AUTHORIZED_USER_TYPE creds can't be used
    # to access IAP-secured resources. You have to use a desktop OAuth flow.
    # This will prompt the user to authenticate via their browser, and cache tokens locally
    # See https://docs.cloud.google.com/iap/docs/authentication-howto#authenticate_from_a_desktop_app
    # This is implemented conditionally to ensure backwards compatibility with code that expects
    # User ADC creds without any interactive prompts.
    if enable_desktop_auth and isinstance(creds, IDTokenCredentialsAdapter):
        if not (desktop_client_id and desktop_client_secret and target_audience):
            raise ValueError(
                'target_audience, desktop_client_id and desktop_client_secret '
                'are required when enable_desktop_auth is True and using user credentials.',
            )
        creds = IAPDesktopCredentialsAdapter(
            iap_client_id=target_audience,
            desktop_client_id=desktop_client_id,
            desktop_client_secret=desktop_client_secret,
        )

    creds.refresh(request)
    token = creds.token
    if not token:
        raise ValueError('Could not generate google identity token')
    return token


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

    def refresh(self, request: google.auth.transport.Request):
        """Refreshes the token."""
        self.credentials.refresh(request)
        self.token = self.credentials.id_token


class ExternalCredentialsAdapter(google_auth_credentials.Credentials):
    """
    Wrapper around ExternalCredentials because I (mfranklin) cannot work out how to
    make the python version work, and have defaulted to using the gcloud command line.
    """

    def __init__(
        self,
        audience: str | None,
        impersonate_id: str | None = None,
    ):
        super().__init__()
        self.token: str | None = None
        self.audience = audience
        impersonate_id = impersonate_id or os.environ.get('GOOGLE_IMPERSONATE_IDENTITY')
        if not impersonate_id:
            raise exceptions.DefaultCredentialsError(
                'GOOGLE_IMPERSONATE_IDENTITY environment variable is not set. '
                'Cannot impersonate service account.',
            )

        self.impersonate_id = impersonate_id

    def refresh(self, *args: Any, **kwargs: Any):  # noqa: ARG002
        """Call gcloud to get a new token."""
        command = [
            'gcloud',
            'auth',
            'print-identity-token',
            f'--impersonate-service-account={self.impersonate_id}',
            '--include-email',
        ]
        if self.audience:
            command.append(f'--audiences={self.audience}')
        self.token = (
            subprocess.check_output(command).decode('utf-8').strip()  # noqa: S603
        )


def _load_credentials_from_file(
    filename: str,
    target_audience: str | None,
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
                f'File {filename} is not a valid json file.',
            ) from exc

    # The type key should indicate that the file is either a service account
    # credentials file or an authorized user credentials file.
    credential_type = info.get('type')

    if credential_type == _AUTHORIZED_USER_TYPE:
        current_credentials = oauth2_credentials.Credentials.from_authorized_user_info(
            info,
            scopes=['openid', 'https://www.googleapis.com/auth/userinfo.email'],
        )
        return IDTokenCredentialsAdapter(credentials=current_credentials)

    if credential_type == _SERVICE_ACCOUNT_TYPE:
        try:
            return service_account.IDTokenCredentials.from_service_account_info(
                info,
                target_audience=target_audience,
            )
        except ValueError as exc:
            raise exceptions.DefaultCredentialsError(
                f'Failed to load service account credentials from {filename}',
            ) from exc

    if credential_type == _EXTERNAL_ACCOUNT_TYPE:
        return ExternalCredentialsAdapter(audience=target_audience)

    raise exceptions.DefaultCredentialsError(
        f'The file {filename} does not have a valid type of google-cloud credentials. '
        f'Type is {credential_type}, but cpg-utils only implements '
        f'{IMPLEMENTED_CREDENTIALS_TYPES}.',
    )


def _get_explicit_environ_credentials(
    target_audience: str | None,
) -> google_auth_credentials.Credentials | None:
    """Gets credentials from the GOOGLE_APPLICATION_CREDENTIALS environment variable."""
    explicit_file = os.environ.get(environment_vars.CREDENTIALS)

    if explicit_file is None:
        return None

    return _load_credentials_from_file(
        os.environ[environment_vars.CREDENTIALS],
        target_audience=target_audience,
    )


def _get_gcloud_sdk_credentials(
    target_audience: str | None,
) -> google_auth_credentials.Credentials | None:
    """Gets the credentials and project ID from the Cloud SDK."""
    from google.auth import _cloud_sdk  # pylint: disable=import-outside-toplevel

    # Check if application default credentials exist.
    credentials_filename = _cloud_sdk.get_application_default_credentials_path()

    if not os.path.isfile(credentials_filename):
        return None

    return _load_credentials_from_file(
        credentials_filename,
        target_audience,
    )


def _get_gce_credentials(
    target_audience: str | None,
    request: google.auth.transport.Request | None = None,
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
            request,
            target_audience,
            use_metadata_identity_endpoint=True,
        )

    return None


def _get_default_id_token_credentials(
    target_audience: str | None,
    request: google.auth.transport.Request | None = None,
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

    raise exceptions.DefaultCredentialsError(_CLOUD_SDK_MISSING_CREDENTIALS)


def get_path_components_from_gcp_path(path: str) -> dict[str, str]:
    """
    Return the {bucket_name}, {dataset}, {bucket_type}, {subdir}, and {file} for GS only paths
    Uses regex to match the full bucket name, dataset name, bucket type (e.g. 'test', 'main-upload', 'release'),
    subdirectory, and the file name.
    """

    bucket_types = ['archive', 'hail', 'main', 'test', 'release']

    # compile pattern matching all CPG bucket formats
    gspath_pattern = re.compile(
        r'gs://(?P<bucket>cpg-(?P<dataset>[\w-]+)-(?P<bucket_type>['
        + '|'.join(s for s in bucket_types)
        + r']+[-\w]*))/(?P<suffix>.+/)?(?P<file>.*)$',
    )

    # if a match succeeds, return the key: value dictionary
    if path_match := gspath_pattern.match(path):
        return path_match.groupdict()

    # raise an error if the input String was not a valid CPG bucket path
    raise ValueError('The input String did not match a valid GCP path')


def get_project_id_from_service_account_email(service_account_email: str) -> str:
    """
    Get GCP project id from service_account_email

    >>> get_project_id_from_service_account_email('cromwell-test@tob-wgs.iam.gserviceaccount.com')
    'tob-wgs'
    """
    # quick and dirty
    return service_account_email.split('@')[-1].split('.')[0]


@deprecated(reason='Use cpg_utils.membership.is_member_in_cached_group instead')
def is_member_in_cached_group(*args: Any, **kwargs: Any):
    from cpg_utils.membership import (
        is_member_in_cached_group as _is_member_in_cached_group,
    )

    return _is_member_in_cached_group(*args, **kwargs)
