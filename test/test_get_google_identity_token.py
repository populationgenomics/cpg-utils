# ruff: noqa: ARG005 # as there is lots of mocking, there are lots of unused args, so ignore this rule
import io
import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from google.auth import environment_vars

from cpg_utils import cloud


@pytest.fixture
def mock_request() -> MagicMock:
    return MagicMock()


# These tests are pretty heavily mocked, so of slightly questionable usefulness
# but at least show the different types of tokens that you can get and verify
# that all code paths work


def test_explicit_environ_authorized_user(
    monkeypatch: pytest.MonkeyPatch,
    mock_request: MagicMock,
) -> None:
    mock_info = {"type": "authorized_user"}
    mock_creds = MagicMock()
    mock_creds.id_token = "mock_auth_user_token"  # noqa: S105

    monkeypatch.setenv(environment_vars.CREDENTIALS, "/mock/creds.json")
    monkeypatch.setattr("os.path.exists", lambda p: True)

    monkeypatch.setattr(
        "builtins.open",
        lambda *args, **kwargs: io.StringIO(json.dumps(mock_info)),
    )
    monkeypatch.setattr(
        "google.oauth2.credentials.Credentials.from_authorized_user_info",
        lambda info, scopes: mock_creds,
    )

    token = cloud.get_google_identity_token("mock_audience", mock_request)
    assert token == "mock_auth_user_token"  # noqa: S105
    mock_creds.refresh.assert_called_once_with(mock_request)


def test_explicit_environ_service_account(
    monkeypatch: pytest.MonkeyPatch,
    mock_request: MagicMock,
) -> None:
    mock_info = {"type": "service_account"}
    mock_creds = MagicMock()
    # For service account, IDTokenCredentials.token returns the token directly
    mock_creds.token = "mock_service_account_token"  # noqa: S105

    monkeypatch.setenv(environment_vars.CREDENTIALS, "/mock/creds.json")
    monkeypatch.setattr("os.path.exists", lambda p: True)

    monkeypatch.setattr(
        "builtins.open",
        lambda *args, **kwargs: io.StringIO(json.dumps(mock_info)),
    )
    monkeypatch.setattr(
        "google.oauth2.service_account.IDTokenCredentials.from_service_account_info",
        lambda info, target_audience: mock_creds,
    )

    token = cloud.get_google_identity_token("mock_audience", mock_request)
    assert token == "mock_service_account_token"  # noqa: S105
    mock_creds.refresh.assert_called_once_with(mock_request)


def test_explicit_environ_external_account(
    monkeypatch: pytest.MonkeyPatch,
    mock_request: MagicMock,
) -> None:
    mock_info = {"type": "external_account"}

    monkeypatch.setenv(environment_vars.CREDENTIALS, "/mock/creds.json")
    monkeypatch.setenv("GOOGLE_IMPERSONATE_IDENTITY", "mock_impersonate_id")
    monkeypatch.setattr("os.path.exists", lambda p: True)

    monkeypatch.setattr(
        "builtins.open",
        lambda *args, **kwargs: io.StringIO(json.dumps(mock_info)),
    )
    monkeypatch.setattr("subprocess.check_output", lambda cmd: b"mock_external_token\n")

    token = cloud.get_google_identity_token("mock_audience", mock_request)
    assert token == "mock_external_token"  # noqa: S105


def test_gcloud_sdk_credentials(
    monkeypatch: pytest.MonkeyPatch,
    mock_request: MagicMock,
) -> None:
    mock_info = {"type": "authorized_user"}
    mock_creds = MagicMock()
    mock_creds.id_token = "mock_gcloud_token"  # noqa: S105

    monkeypatch.delenv(environment_vars.CREDENTIALS, raising=False)

    monkeypatch.setattr(
        "google.auth._cloud_sdk.get_application_default_credentials_path",
        lambda: "/mock/gcloud/creds.json",
    )
    monkeypatch.setattr("os.path.isfile", lambda p: p == "/mock/gcloud/creds.json")
    monkeypatch.setattr("os.path.exists", lambda p: p == "/mock/gcloud/creds.json")

    monkeypatch.setattr(
        "builtins.open",
        lambda *args, **kwargs: io.StringIO(json.dumps(mock_info)),
    )
    monkeypatch.setattr(
        "google.oauth2.credentials.Credentials.from_authorized_user_info",
        lambda info, scopes: mock_creds,
    )

    token = cloud.get_google_identity_token("mock_audience", mock_request)
    assert token == "mock_gcloud_token"  # noqa: S105
    mock_creds.refresh.assert_called_once_with(mock_request)


def test_gce_credentials(
    monkeypatch: pytest.MonkeyPatch,
    mock_request: MagicMock,
) -> None:
    mock_creds = MagicMock()
    mock_creds.token = "mock_gce_token"  # noqa: S105

    monkeypatch.delenv(environment_vars.CREDENTIALS, raising=False)

    monkeypatch.setattr(
        "google.auth._cloud_sdk.get_application_default_credentials_path",
        lambda: "/mock/gcloud/creds.json",
    )
    monkeypatch.setattr("os.path.isfile", lambda p: False)

    monkeypatch.setattr(
        "google.auth.compute_engine._metadata.ping",
        lambda request: True,
    )
    monkeypatch.setattr(
        "google.auth.compute_engine.IDTokenCredentials",
        lambda request, target_audience, use_metadata_identity_endpoint: mock_creds,
    )

    token = cloud.get_google_identity_token("mock_audience", mock_request)
    assert token == "mock_gce_token"  # noqa: S105
    mock_creds.refresh.assert_called_once_with(mock_request)


def test_enable_desktop_auth(
    monkeypatch: pytest.MonkeyPatch,
    mock_request: MagicMock,
) -> None:
    mock_info = {"type": "authorized_user"}
    mock_creds = MagicMock()
    mock_creds.id_token = "mock_auth_user_token"  # noqa: S105

    monkeypatch.setenv(environment_vars.CREDENTIALS, "/mock/creds.json")
    monkeypatch.setattr("os.path.exists", lambda p: True)

    monkeypatch.setattr(
        "builtins.open",
        lambda *args, **kwargs: io.StringIO(json.dumps(mock_info)),
    )
    monkeypatch.setattr(
        "google.oauth2.credentials.Credentials.from_authorized_user_info",
        lambda info, scopes: mock_creds,
    )

    def mock_desktop_refresh(self: Any, _req: Any) -> None:
        self.token = "mock_desktop_token"  # noqa: S105

    monkeypatch.setattr(
        "cpg_utils.cloud.IAPDesktopCredentialsAdapter.refresh",
        mock_desktop_refresh,
    )

    token = cloud.get_google_identity_token(
        target_audience="mock_audience",
        request=mock_request,
        enable_desktop_auth=True,
        desktop_client_id="mock_client_id",
        desktop_client_secret="mock_client_secret",  # noqa: S106
    )
    assert token == "mock_desktop_token"  # noqa: S105


def test_get_google_identity_token_no_credentials(
    monkeypatch: pytest.MonkeyPatch,
    mock_request: MagicMock,
) -> None:
    monkeypatch.delenv(environment_vars.CREDENTIALS, raising=False)

    monkeypatch.setattr(
        "google.auth._cloud_sdk.get_application_default_credentials_path",
        lambda: "/mock/gcloud/creds.json",
    )
    monkeypatch.setattr("os.path.isfile", lambda p: False)

    try:
        from google.auth.compute_engine import _metadata

        monkeypatch.setattr(
            _metadata,
            "ping",
            lambda request: False,
        )
    except ImportError:
        pass

    with pytest.raises(cloud.exceptions.DefaultCredentialsError):
        cloud.get_google_identity_token("mock_audience", mock_request)


def test_explicit_environ_invalid_type(
    monkeypatch: pytest.MonkeyPatch,
    mock_request: MagicMock,
) -> None:
    mock_info = {"type": "invalid_type"}

    monkeypatch.setenv(environment_vars.CREDENTIALS, "/mock/creds.json")
    monkeypatch.setattr("os.path.exists", lambda p: True)

    monkeypatch.setattr(
        "builtins.open",
        lambda *args, **kwargs: io.StringIO(json.dumps(mock_info)),
    )

    with pytest.raises(cloud.exceptions.DefaultCredentialsError) as exc_info:
        cloud.get_google_identity_token("mock_audience", mock_request)

    assert "does not have a valid type" in str(exc_info.value)
