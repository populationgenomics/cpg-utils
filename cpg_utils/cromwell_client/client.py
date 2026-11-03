"""
Authentication and a thin REST client for the Cromwell workflows API.

The client makes exactly one HTTP request per method call and never retries; retrying
is left to the caller (see :func:`cpg_utils.cromwell_client.watch.watch_workflow`).
"""

import base64
import binascii
import json
import logging
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import requests

from cpg_utils.constants import CROMWELL_AUDIENCE, CROMWELL_URL
from cpg_utils.cromwell_client.common import check_input_document_count
from cpg_utils.cromwell_client.errors import (
    CromwellNotFound,
    CromwellRequestError,
    CromwellUnavailable,
)

logger = logging.getLogger(__name__)

FALLBACK_TOKEN_LIFETIME_SECONDS = 50 * 60
_BODY_SNIPPET_CHARS = 500
_UNAVAILABLE_STATUS_CODES = frozenset({401, 408, 429})


def _jwt_expiry(token: str) -> float | None:
    """Return the ``exp`` claim of an (unverified) JWT, or None if it can't be read."""
    try:
        payload_b64 = token.split('.')[1]
        payload_b64 += '=' * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        exp = payload['exp']
    except (IndexError, KeyError, TypeError, ValueError, binascii.Error):
        return None
    if isinstance(exp, bool) or not isinstance(exp, int | float):
        return None
    return float(exp)


class CachedTokenProvider:
    """
    A thread-safe, caching source of Google ID tokens for the Cromwell server.

    The token is reused until ``refresh_margin_seconds`` before the ``exp`` claim in its
    (unverified) JWT payload. If ``exp`` can't be read, the token is kept for 50
    minutes.

    >>> provider = CachedTokenProvider(mint=lambda: 'not-a-jwt')
    >>> provider()
    'not-a-jwt'
    """

    def __init__(
        self,
        audience: str = CROMWELL_AUDIENCE,
        *,
        mint: Callable[[], str] | None = None,
        refresh_margin_seconds: float = 300,
    ) -> None:
        """
        Args:
            audience: the OAuth client ID the ID token is minted for.
            mint: returns a fresh token. Defaults to
                ``cpg_utils.cloud.get_google_identity_token(audience)``.
            refresh_margin_seconds: re-mint this long before the token expires.
        """
        self.audience = audience
        self._mint = mint if mint is not None else self._default_mint
        self.refresh_margin_seconds = refresh_margin_seconds
        self._lock = threading.Lock()
        self._token: str | None = None
        self._valid_until = 0.0

    def _default_mint(self) -> str:
        from cpg_utils.cloud import get_google_identity_token

        return get_google_identity_token(self.audience)

    def __call__(self) -> str:
        """Return a cached token, minting a new one if it is missing or near expiry."""
        with self._lock:
            now = time.time()
            if self._token is not None and now < self._valid_until:
                return self._token
            try:
                token = self._mint()
            except Exception as e:  # noqa: BLE001 - any minting failure is fatal here
                raise CromwellUnavailable(
                    f'Could not obtain an ID token for Cromwell: {e}',
                ) from e
            if not token:
                raise CromwellUnavailable(
                    'Could not obtain an ID token for Cromwell: empty token',
                )
            exp = _jwt_expiry(token)
            if exp is None:
                self._valid_until = now + FALLBACK_TOKEN_LIFETIME_SECONDS
            else:
                self._valid_until = exp - self.refresh_margin_seconds
            self._token = token
            return token

    def invalidate(self) -> None:
        """Drop the cached token so the next call mints a new one."""
        with self._lock:
            self._token = None
            self._valid_until = 0.0


def _snippet(response: requests.Response) -> str:
    try:
        text = response.text
    except Exception:  # noqa: BLE001
        return '<unreadable body>'
    return text[:_BODY_SNIPPET_CHARS]


class CromwellClient:
    """
    A thin client for Cromwell's ``/api/workflows/v1`` REST API.

    Each method makes exactly one request, authenticated with
    ``Authorization: Bearer <token_provider()>``, and maps failures onto the
    exceptions in :mod:`cpg_utils.cromwell_client.errors`. It never retries.

    >>> client = CromwellClient('https://cromwell.example.org/', token_provider=lambda: 't')
    >>> client.api_url
    'https://cromwell.example.org/api/workflows/v1'
    """

    def __init__(
        self,
        url: str = CROMWELL_URL,
        *,
        token_provider: Callable[[], str] | None = None,
        timeout_seconds: float = 60,
        submit_timeout_seconds: float = 300,
    ) -> None:
        """
        Args:
            url: base URL of the Cromwell server.
            token_provider: returns a bearer token. Defaults to a
                :class:`CachedTokenProvider` for ``CROMWELL_AUDIENCE``. If it has an
                ``invalidate()`` method, that is called after an HTTP 401.
            timeout_seconds: per-request timeout for everything except submission.
            submit_timeout_seconds: timeout for the (potentially large) submission.
        """
        self.url = url.rstrip('/')
        self.api_url = f'{self.url}/api/workflows/v1'
        if token_provider is None:
            token_provider = CachedTokenProvider()
        self.token_provider: Callable[[], str] = token_provider
        self.timeout_seconds = timeout_seconds
        self.submit_timeout_seconds = submit_timeout_seconds
        self._local = threading.local()

    @property
    def _session(self) -> requests.Session:
        session = getattr(self._local, 'session', None)
        if session is None:
            session = requests.Session()
            self._local.session = session
        return session

    def _request(
        self,
        method: str,
        path: str,
        *,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        url = f'{self.api_url}{path}'
        headers = {'Authorization': f'Bearer {self.token_provider()}'}
        try:
            response = self._session.request(
                method,
                url,
                headers=headers,
                timeout=timeout if timeout is not None else self.timeout_seconds,
                **kwargs,
            )
        except requests.RequestException as e:
            raise CromwellUnavailable(f'{method} {url} failed: {e}') from e

        code = response.status_code
        if code == 401:  # noqa: PLR2004
            invalidate = getattr(self.token_provider, 'invalidate', None)
            if callable(invalidate):
                invalidate()
        if code >= 500 or code in _UNAVAILABLE_STATUS_CODES:  # noqa: PLR2004
            raise CromwellUnavailable(
                f'{method} {url} returned HTTP {code}: {_snippet(response)}',
            )
        if code == 404:  # noqa: PLR2004
            raise CromwellNotFound(
                f'{method} {url} returned HTTP 404: {_snippet(response)}',
            )
        if code >= 400:  # noqa: PLR2004
            raise CromwellRequestError(
                f'{method} {url} returned HTTP {code}: {_snippet(response)}',
                status_code=code,
            )
        try:
            body = response.json()
        except ValueError as e:
            raise CromwellUnavailable(
                f'{method} {url} returned a non-JSON body: {_snippet(response)}',
            ) from e
        # Every endpoint used here returns a JSON object; anything else is a broken
        # response (e.g. from a proxy), and is treated like a non-JSON body.
        if not isinstance(body, dict):
            raise CromwellUnavailable(
                f'{method} {url} returned JSON that is not an object: '
                f'{_snippet(response)}',
            )
        return body

    def submit(
        self,
        workflow_source: bytes,
        *,
        workflow_filename: str = 'workflow.wdl',
        input_documents: Sequence[bytes] = (),
        options: Mapping[str, Any],
        dependencies: bytes | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> str:
        """
        Submit a Workflow and return its ID.

        Args:
            workflow_source: the WDL file's bytes.
            workflow_filename: the file name sent with the WDL.
            input_documents: up to five JSON inputs documents. Cromwell applies later
                documents over earlier ones.
            options: the Workflow options, sent as JSON in the caller's key order.
            dependencies: a zip of imported WDL/libs, if any.
            labels: Cromwell workflow labels (not ``google_labels``).
        """
        check_input_document_count(len(input_documents))
        files: list[tuple[str, tuple[str | None, Any] | tuple[str | None, Any, str]]]
        files = [('workflowSource', (workflow_filename, workflow_source))]
        for index, document in enumerate(input_documents, start=1):
            field = 'workflowInputs' if index == 1 else f'workflowInputs_{index}'
            files.append((field, (f'{field}.json', document, 'application/json')))
        files.append(('workflowOptions', (None, json.dumps(options))))
        if dependencies is not None:
            files.append(
                ('workflowDependencies', ('deps.zip', dependencies, 'application/zip')),
            )
        if labels is not None:
            files.append(('labels', (None, json.dumps(labels))))

        body = self._request(
            'POST',
            '',
            files=files,
            timeout=self.submit_timeout_seconds,
        )
        workflow_id = body.get('id')
        if not workflow_id:
            raise CromwellUnavailable(
                f'Cromwell accepted the submission but returned no id: {body!r:.500}',
            )
        return workflow_id

    def status(self, workflow_id: str) -> str:
        """Return the Workflow's current status, e.g. ``Running`` or ``Succeeded``."""
        body = self._request('GET', f'/{workflow_id}/status')
        status = body.get('status')
        if not status:
            raise CromwellUnavailable(
                f'Cromwell returned no status for {workflow_id}: {body!r:.500}',
            )
        return status

    def metadata(
        self,
        workflow_id: str,
        *,
        include_keys: Sequence[str] = (),
    ) -> dict[str, Any]:
        """
        Return the Workflow's metadata, optionally trimmed with ``includeKey``.
        """
        params = [('includeKey', key) for key in include_keys]
        return self._request(
            'GET',
            f'/{workflow_id}/metadata',
            params=params or None,
        )

    def outputs(self, workflow_id: str) -> dict[str, Any]:
        """Return the Workflow outputs (the ``outputs`` object of the response)."""
        body = self._request('GET', f'/{workflow_id}/outputs')
        outputs = body.get('outputs') or {}
        if not isinstance(outputs, dict):
            raise CromwellUnavailable(
                f'Cromwell returned non-object outputs for {workflow_id}: '
                f'{outputs!r:.500}',
            )
        return outputs

    def abort(self, workflow_id: str) -> str:
        """Request an abort and return the status Cromwell reports, e.g. ``Aborting``."""
        body = self._request('POST', f'/{workflow_id}/abort')
        return body.get('status', '')

    def query_by_label(self, key: str, value: str) -> list[dict[str, Any]]:
        """
        Return the Workflows carrying the Cromwell label ``key:value``.

        Each result has ``id``, ``status`` and ``submission``, plus ``end`` once the
        Workflow has finished.
        """
        body = self._request(
            'GET',
            '/query',
            params=[('label', f'{key}:{value}'), ('includeSubworkflows', 'false')],
        )
        results = body.get('results') or []
        if not isinstance(results, list):
            raise CromwellUnavailable(
                f'Cromwell returned non-list query results: {results!r:.500}',
            )
        return results
