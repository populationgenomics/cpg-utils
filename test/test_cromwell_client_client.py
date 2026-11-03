"""Tests for cpg_utils.cromwell_client.client (spec Phase 1)."""

import base64
import json
from collections.abc import Callable
from typing import Any

import pytest
import requests

from cpg_utils.cromwell_client import client as client_module
from cpg_utils.cromwell_client.client import CachedTokenProvider, CromwellClient
from cpg_utils.cromwell_client.errors import (
    CromwellNotFound,
    CromwellRequestError,
    CromwellUnavailable,
)

URL = 'https://cromwell.test'
API = f'{URL}/api/workflows/v1'


def _jwt(payload: dict[str, Any]) -> str:
    def enc(d: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b'=').decode()

    return f'{enc({"alg": "none"})}.{enc(payload)}.sig'


class _Clock:
    def __init__(self, now: float = 1_000_000.0) -> None:
        self.now = now

    def time(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    c = _Clock()
    monkeypatch.setattr(client_module.time, 'time', c.time)
    return c


# --- §1.1 CachedTokenProvider -------------------------------------------------------


def test_token_reused_before_expiry_and_reminted_inside_margin(clock: _Clock):
    minted: list[str] = []

    def mint() -> str:
        token = _jwt({'exp': clock.now + 3600, 'n': len(minted)})
        minted.append(token)
        return token

    provider = CachedTokenProvider(mint=mint, refresh_margin_seconds=300)
    first = provider()
    clock.now += 3600 - 301  # just outside the margin
    assert provider() == first
    assert len(minted) == 1
    clock.now += 2  # now inside the margin
    second = provider()
    assert second != first
    assert len(minted) == 2  # noqa: PLR2004


def test_malformed_jwt_falls_back_to_50_minutes(clock: _Clock):
    calls = []

    def mint() -> str:
        calls.append(1)
        return 'not.a-valid.jwt'

    provider = CachedTokenProvider(mint=mint)
    provider()
    clock.now += 50 * 60 - 1
    provider()
    assert len(calls) == 1
    clock.now += 2
    provider()
    assert len(calls) == 2  # noqa: PLR2004


def test_invalidate_forces_remint(clock: _Clock):  # noqa: ARG001
    calls = []

    def mint() -> str:
        calls.append(1)
        return _jwt({'exp': 9e12})

    provider = CachedTokenProvider(mint=mint)
    provider()
    provider()
    provider.invalidate()
    provider()
    assert len(calls) == 2  # noqa: PLR2004


def test_mint_failure_becomes_unavailable():
    def mint() -> str:
        raise RuntimeError('no credentials')

    with pytest.raises(CromwellUnavailable) as exc:
        CachedTokenProvider(mint=mint)()
    assert isinstance(exc.value.__cause__, RuntimeError)


def test_empty_token_becomes_unavailable():
    with pytest.raises(CromwellUnavailable):
        CachedTokenProvider(mint=lambda: '')()


def test_default_mint_uses_cloud_identity_token(monkeypatch: pytest.MonkeyPatch):
    import cpg_utils.cloud

    audiences = []

    def fake_token(audience: str) -> str:
        audiences.append(audience)
        return 'tok'

    monkeypatch.setattr(cpg_utils.cloud, 'get_google_identity_token', fake_token)
    assert CachedTokenProvider('my-audience')() == 'tok'
    assert audiences == ['my-audience']


# --- §1.2 CromwellClient ------------------------------------------------------------


class _Response:
    def __init__(
        self,
        status_code: int = 200,
        body: Any = None,
        text: str | None = None,
    ):
        self.status_code = status_code
        self._body = body
        self.text = text if text is not None else json.dumps(body)

    def json(self) -> Any:
        if self._body is None:
            raise ValueError('not json')
        return self._body


class _Recorder:
    """Stands in for requests.Session.request and records each call."""

    def __init__(self, *responses: _Response | Exception) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def __call__(
        self,
        _session: requests.Session,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> _Response:
        self.calls.append({'method': method, 'url': url, **kwargs})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


Stub = Callable[..., _Recorder]


@pytest.fixture
def stub(monkeypatch: pytest.MonkeyPatch):
    def install(*responses: _Response | Exception) -> _Recorder:
        recorder = _Recorder(*responses)

        def request(
            session: requests.Session,
            method: str,
            url: str,
            **kw: Any,
        ) -> _Response:
            return recorder(session, method, url, **kw)

        monkeypatch.setattr(requests.Session, 'request', request)
        return recorder

    return install


def _client(**kwargs: Any) -> CromwellClient:
    return CromwellClient(URL, token_provider=lambda: 'TOKEN', **kwargs)


def test_submit_multipart_fields_in_order(stub: Stub):
    rec = stub(_Response(body={'id': 'wf-1', 'status': 'Submitted'}))
    docs = [f'{{"n": {i}}}'.encode() for i in range(5)]
    options = {'z': 1, 'a': 2}
    workflow_id = _client(submit_timeout_seconds=123).submit(
        b'version 1.0',
        workflow_filename='main.wdl',
        input_documents=docs,
        options=options,
        dependencies=b'PK-zip',
        labels={'cpg-submission-key': 'abc'},
    )
    assert workflow_id == 'wf-1'
    (call,) = rec.calls
    assert call['method'] == 'POST'
    assert call['url'] == API
    assert call['headers'] == {'Authorization': 'Bearer TOKEN'}
    assert call['timeout'] == 123  # noqa: PLR2004
    files = call['files']
    assert [name for name, _ in files] == [
        'workflowSource',
        'workflowInputs',
        'workflowInputs_2',
        'workflowInputs_3',
        'workflowInputs_4',
        'workflowInputs_5',
        'workflowOptions',
        'workflowDependencies',
        'labels',
    ]
    fields = dict(files)
    assert fields['workflowSource'] == ('main.wdl', b'version 1.0')
    assert [fields[n][1] for n, _ in files[1:6]] == docs
    assert fields['workflowOptions'][1] == json.dumps(options)
    assert fields['workflowOptions'][1] == '{"z": 1, "a": 2}'
    assert fields['workflowDependencies'][:2] == ('deps.zip', b'PK-zip')
    assert json.loads(fields['labels'][1]) == {'cpg-submission-key': 'abc'}


def test_submit_omits_labels_and_dependencies_when_none(stub: Stub):
    rec = stub(_Response(body={'id': 'wf-1'}))
    _client().submit(b'wdl', options={})
    names = [name for name, _ in rec.calls[0]['files']]
    assert names == ['workflowSource', 'workflowOptions']


def test_submit_more_than_five_documents_raises_before_request(stub: Stub):
    rec = stub()
    with pytest.raises(ValueError, match='at most 5'):
        _client().submit(b'wdl', input_documents=[b'{}'] * 6, options={})
    assert rec.calls == []


def test_submit_without_id_is_unavailable(stub: Stub):
    stub(_Response(body={'status': 'Submitted'}))
    with pytest.raises(CromwellUnavailable):
        _client().submit(b'wdl', options={})


@pytest.mark.parametrize(
    ('response', 'expected'),
    [
        (requests.ConnectionError('boom'), CromwellUnavailable),
        (requests.Timeout('slow'), CromwellUnavailable),
        (_Response(500, {'e': 1}), CromwellUnavailable),
        (_Response(503, {'e': 1}), CromwellUnavailable),
        (_Response(408, {'e': 1}), CromwellUnavailable),
        (_Response(429, {'e': 1}), CromwellUnavailable),
        (_Response(401, {'e': 1}), CromwellUnavailable),
        (_Response(404, {'e': 1}), CromwellNotFound),
        (_Response(400, {'e': 1}), CromwellRequestError),
        (_Response(403, {'e': 1}), CromwellRequestError),
        (_Response(200, None, text='<html>'), CromwellUnavailable),
    ],
)
def test_error_mapping(
    stub: Stub,
    response: _Response | Exception,
    expected: type[Exception],
):
    stub(response)
    with pytest.raises(expected) as exc:
        _client().status('wf-1')
    assert type(exc.value) is expected
    if isinstance(response, _Response) and isinstance(exc.value, CromwellRequestError):
        assert exc.value.status_code == response.status_code


def test_not_found_is_a_request_error(stub: Stub):
    stub(_Response(404, {'status': 'fail'}))
    with pytest.raises(CromwellRequestError) as exc:
        _client().status('wf-1')
    assert exc.value.status_code == 404  # noqa: PLR2004


def test_error_body_is_truncated(stub: Stub):
    stub(_Response(500, None, text='x' * 2000))
    with pytest.raises(CromwellUnavailable) as exc:
        _client().status('wf-1')
    assert 'x' * 500 in str(exc.value)
    assert 'x' * 501 not in str(exc.value)


def test_401_invalidates_token(stub: Stub):
    stub(_Response(401, {'e': 1}))
    provider = CachedTokenProvider(mint=lambda: 'tok')
    invalidated = []
    provider.invalidate = lambda: invalidated.append(1)  # type: ignore[method-assign]
    with pytest.raises(CromwellUnavailable):
        CromwellClient(URL, token_provider=provider).status('wf-1')
    assert invalidated == [1]


def test_401_with_plain_callable_provider(stub: Stub):
    stub(_Response(401, {'e': 1}))
    with pytest.raises(CromwellUnavailable):
        _client().status('wf-1')


def test_status_url_and_result(stub: Stub):
    rec = stub(_Response(body={'id': 'wf-1', 'status': 'Running'}))
    assert _client(timeout_seconds=7).status('wf-1') == 'Running'
    assert rec.calls[0]['method'] == 'GET'
    assert rec.calls[0]['url'] == f'{API}/wf-1/status'
    assert rec.calls[0]['timeout'] == 7  # noqa: PLR2004


def test_metadata_include_keys(stub: Stub):
    rec = stub(_Response(body={'id': 'wf-1'}), _Response(body={'id': 'wf-1'}))
    client = _client()
    client.metadata('wf-1', include_keys=('status', 'submission'))
    client.metadata('wf-1')
    assert rec.calls[0]['url'] == f'{API}/wf-1/metadata'
    assert rec.calls[0]['params'] == [
        ('includeKey', 'status'),
        ('includeKey', 'submission'),
    ]
    assert rec.calls[1]['params'] is None


def test_outputs_and_abort(stub: Stub):
    rec = stub(
        _Response(body={'id': 'wf-1', 'outputs': {'w.out': 'gs://b/o'}}),
        _Response(body={'id': 'wf-1', 'status': 'Aborting'}),
    )
    client = _client()
    assert client.outputs('wf-1') == {'w.out': 'gs://b/o'}
    assert client.abort('wf-1') == 'Aborting'
    assert rec.calls[0]['url'] == f'{API}/wf-1/outputs'
    assert (rec.calls[1]['method'], rec.calls[1]['url']) == (
        'POST',
        f'{API}/wf-1/abort',
    )


def test_query_by_label(stub: Stub):
    results = [{'id': 'wf-1', 'status': 'Running', 'submission': '2026-01-01T00:00Z'}]
    rec = stub(
        _Response(body={'results': results, 'totalResultsCount': 1}),
        _Response(body={'totalResultsCount': 0}),
    )
    client = _client()
    assert client.query_by_label('cpg-submission-key', 'abc') == results
    assert client.query_by_label('cpg-submission-key', 'def') == []
    assert rec.calls[0]['url'] == f'{API}/query'
    assert rec.calls[0]['params'] == [
        ('label', 'cpg-submission-key:abc'),
        ('includeSubworkflows', 'false'),
    ]


CLIENT_CALLS: list[tuple[str, Callable[[CromwellClient], Any]]] = [
    ('status', lambda c: c.status('wf-1')),
    ('metadata', lambda c: c.metadata('wf-1')),
    ('outputs', lambda c: c.outputs('wf-1')),
    ('abort', lambda c: c.abort('wf-1')),
    ('query_by_label', lambda c: c.query_by_label('k', 'v')),
    ('submit', lambda c: c.submit(b'wdl', options={})),
]


@pytest.mark.parametrize('body', [['a', 'list'], 'a string', 42, True])
@pytest.mark.parametrize(
    ('name', 'call'),
    CLIENT_CALLS,
    ids=[n for n, _ in CLIENT_CALLS],
)
def test_json_body_that_is_not_an_object_is_unavailable(
    stub: Stub,
    body: Any,
    name: str,  # noqa: ARG001
    call: Callable[[CromwellClient], Any],
):
    stub(_Response(body=body))
    with pytest.raises(CromwellUnavailable, match='not an object'):
        call(_client())


def test_outputs_that_are_not_an_object_are_unavailable(stub: Stub):
    stub(_Response(body={'id': 'wf-1', 'outputs': ['gs://b/o']}))
    with pytest.raises(CromwellUnavailable, match='non-object outputs'):
        _client().outputs('wf-1')


def test_query_results_that_are_not_a_list_are_unavailable(stub: Stub):
    stub(_Response(body={'results': {'id': 'wf-1'}}))
    with pytest.raises(CromwellUnavailable, match='non-list query results'):
        _client().query_by_label('k', 'v')
