"""Tests for cpg_utils.cromwell_client.collect (spec Phase 5)."""

import logging
import pathlib
from typing import Any

import pytest

from cpg_utils.cromwell_client import collect as collect_module
from cpg_utils.cromwell_client.collect import collect_outputs
from cpg_utils.cromwell_client.errors import CollectError

REAL_IS_FILE_VALUE = collect_module._is_file_value


@pytest.fixture(autouse=True)
def local_files_are_files(monkeypatch: pytest.MonkeyPatch):
    """Treat absolute local paths as file outputs, standing in for gs:// paths."""
    monkeypatch.setattr(
        collect_module,
        '_is_file_value',
        lambda value: isinstance(value, str) and value.startswith('/'),
    )


@pytest.fixture
def src(tmp_path: pathlib.Path) -> pathlib.Path:
    d = tmp_path / 'cromwell-execution'
    d.mkdir()
    for name in ('a.txt', 'b.txt', 'c.txt', 'x.bam', 'x.bam.bai'):
        (d / name).write_text(f'content of {name}\n')
    return d


@pytest.fixture
def dst(tmp_path: pathlib.Path) -> pathlib.Path:
    return tmp_path / 'dest'


def test_single_file(src: pathlib.Path, dst: pathlib.Path):
    target = dst / 'nested' / 'out.txt'
    result = collect_outputs({'w.a': str(src / 'a.txt')}, {'w.a': target})
    assert result == {'w.a': str(target)}
    assert target.read_text() == 'content of a.txt\n'


def test_array(src: pathlib.Path, dst: pathlib.Path):
    outputs = {'w.arr': [str(src / 'a.txt'), str(src / 'b.txt')]}
    targets = [str(dst / '1.txt'), dst / '2.txt']
    result = collect_outputs(outputs, {'w.arr': targets})
    assert result == {'w.arr': [str(dst / '1.txt'), str(dst / '2.txt')]}
    assert (dst / '2.txt').read_text() == 'content of b.txt\n'


def test_resource_group_as_two_keys(src: pathlib.Path, dst: pathlib.Path):
    outputs = {
        'w.bam': str(src / 'x.bam'),
        'w.bai': str(src / 'x.bam.bai'),
        'w.ignored': str(src / 'c.txt'),
    }
    result = collect_outputs(
        outputs,
        {'w.bam': dst / 's.bam', 'w.bai': dst / 's.bam.bai'},
    )
    assert set(result) == {'w.bam', 'w.bai'}
    assert (dst / 's.bam.bai').read_text() == 'content of x.bam.bai\n'
    assert not (dst / 'c.txt').exists()


def test_planning_problems_reported_together(src: pathlib.Path, dst: pathlib.Path):
    outputs: dict[str, Any] = {
        'w.null': None,
        'w.arr': [str(src / 'a.txt')],
        'w.single': str(src / 'a.txt'),
        'w.list_for_single': [str(src / 'a.txt')],
        'w.nested': [[str(src / 'a.txt')], None],
        'w.good': str(src / 'b.txt'),
    }
    with pytest.raises(CollectError) as exc:
        collect_outputs(
            outputs,
            {
                'w.missing': dst / 'm',
                'w.null': dst / 'n',
                'w.arr': [dst / '1', dst / '2'],
                'w.single': [dst / 's'],
                'w.list_for_single': dst / 'l',
                'w.nested': [dst / 'x', dst / 'y'],
                'w.good': dst / 'good',
            },
        )
    problems = exc.value.problems
    assert len(problems) == 7  # noqa: PLR2004
    for fragment in (
        'w.missing: not among',
        'w.null: output is null',
        'w.arr: output has 1 element(s) but 2',
        'w.single: destination is a list',
        'w.list_for_single: output is a list',
        'w.nested[0]: element is a list',
        'w.nested[1]: element is null',
    ):
        assert any(fragment in p for p in problems), fragment
    # planning failed, so nothing was copied
    assert not dst.exists()


def test_scalar_passes_through_with_warning(
    dst: pathlib.Path,
    caplog: pytest.LogCaptureFixture,
):
    outputs = {'w.n': 3, 'w.s': 'hello', 'w.flag': False, 'w.f': 1.5}
    with caplog.at_level(logging.WARNING):
        result = collect_outputs(outputs, {k: dst / k for k in outputs})
    assert result == outputs
    warnings = [r for r in caplog.records if 'is not a file' in r.message]
    assert len(warnings) == len(outputs)
    assert not dst.exists()


def test_source_equal_to_destination_is_skipped(
    src: pathlib.Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    def fail(*_args: Any) -> None:
        raise AssertionError('should not copy')

    monkeypatch.setattr(collect_module, '_copy_file', fail)
    path = str(src / 'a.txt')
    with caplog.at_level(logging.INFO):
        assert collect_outputs({'w.a': path}, {'w.a': path}) == {'w.a': path}
    assert any('already at' in r.message for r in caplog.records)


@pytest.mark.parametrize('threads', [1, 8, 0])
def test_one_failing_copy_does_not_stop_the_others(
    src: pathlib.Path,
    dst: pathlib.Path,
    threads: int,
):
    outputs = {
        'w.a': str(src / 'a.txt'),
        'w.gone': str(src / 'deleted.txt'),
        'w.b': str(src / 'b.txt'),
        'w.also_gone': str(src / 'also-deleted.txt'),
    }
    with pytest.raises(CollectError) as exc:
        collect_outputs(outputs, {k: dst / k for k in outputs}, threads=threads)
    assert len(exc.value.problems) == 2  # noqa: PLR2004
    assert any(p.startswith('w.gone:') for p in exc.value.problems)
    assert any(p.startswith('w.also_gone:') for p in exc.value.problems)
    assert (dst / 'w.a').read_text() == 'content of a.txt\n'
    assert (dst / 'w.b').read_text() == 'content of b.txt\n'


def test_rerun_overwrites(src: pathlib.Path, dst: pathlib.Path):
    target = dst / 'out.txt'
    collect_outputs({'w.a': str(src / 'a.txt')}, {'w.a': target})
    collect_outputs({'w.a': str(src / 'b.txt')}, {'w.a': target})
    assert target.read_text() == 'content of b.txt\n'


def test_gcs_to_gcs_uses_rewrite(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(collect_module, '_is_file_value', REAL_IS_FILE_VALUE)
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        collect_module,
        '_rewrite_gcs',
        lambda s, d: calls.append((str(s), str(d))),
    )
    result = collect_outputs(
        {'w.big': 'gs://cromwell-bucket/exec/big.cram', 'w.n': 1},
        {'w.big': 'gs://dataset-bucket/out/big.cram', 'w.n': 'gs://unused'},
    )
    assert result == {'w.big': 'gs://dataset-bucket/out/big.cram', 'w.n': 1}
    assert calls == [
        ('gs://cromwell-bucket/exec/big.cram', 'gs://dataset-bucket/out/big.cram'),
    ]


class _FakeBlob:
    def __init__(self, name: str, rewrites: list[tuple[Any, ...]]) -> None:
        self.name = name
        self.rewrites = rewrites
        self.tokens: list[Any] = []

    def rewrite(self, source: '_FakeBlob', token: Any = None) -> tuple[Any, int, int]:
        assert source.name == 'src-bucket/big'
        self.tokens.append(token)
        return self.rewrites.pop(0)


def test_rewrite_loops_on_token(monkeypatch: pytest.MonkeyPatch):
    from cloudpathlib.gs import GSPath

    dst_blob = _FakeBlob('dst-bucket/out', [('t1', 1, 3), ('t2', 2, 3), (None, 3, 3)])

    class _Bucket:
        def __init__(self, name: str) -> None:
            self.name = name

        def blob(self, name: str) -> _FakeBlob:
            path = f'{self.name}/{name}'
            return dst_blob if path == 'dst-bucket/out' else _FakeBlob(path, [])

    class _Client:
        def bucket(self, name: str) -> _Bucket:
            return _Bucket(name)

    monkeypatch.setattr(collect_module, '_storage_client', _Client)
    collect_module._rewrite_gcs(
        GSPath('gs://src-bucket/big'),
        GSPath('gs://dst-bucket/out'),
    )
    assert dst_blob.tokens == [None, 't1', 't2']
