"""Tests for cpg_utils.cromwell_client.deps (spec §3.1)."""

import io
import os
import pathlib
import sys
import zipfile

import pytest

from cpg_utils.cromwell_client.deps import zip_dependencies


@pytest.fixture
def tree(tmp_path: pathlib.Path) -> pathlib.Path:
    root = tmp_path / 'repo'
    (root / 'lib' / 'pkg').mkdir(parents=True)
    (root / 'lib' / 'pkg' / '__init__.py').write_text('VALUE = 42\n')
    (root / 'lib' / 'pkg' / 'helper.py').write_text('def f():\n    return 1\n')
    (root / 'lib' / 'empty_dir').mkdir()
    (root / 'tasks').mkdir()
    (root / 'tasks' / 'b.wdl').write_text('version 1.0\n')
    (root / 'tasks' / 'a.wdl').write_text('version 1.0\n')
    (root / 'Structs.wdl').write_text('version 1.0\n')
    (root / 'unrelated.txt').write_text('not zipped\n')
    return root


def _names(data: bytes) -> list[str]:
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        return archive.namelist()


def test_entry_names_are_relative_sorted_and_files_only(tree: pathlib.Path):
    data = zip_dependencies(tree, ['tasks', 'lib/', 'Structs.wdl', 'tasks/a.wdl'])
    assert _names(data) == [
        'Structs.wdl',
        'lib/pkg/__init__.py',
        'lib/pkg/helper.py',
        'tasks/a.wdl',
        'tasks/b.wdl',
    ]


def test_entries_have_fixed_metadata(tree: pathlib.Path):
    data = zip_dependencies(tree, ['lib'])
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        for info in archive.infolist():
            assert info.date_time == (1980, 1, 1, 0, 0, 0)
            assert info.external_attr == 0o644 << 16
            assert info.compress_type == zipfile.ZIP_DEFLATED
            assert info.extra == b''


def test_output_is_identical_after_mtime_changes(tree: pathlib.Path):
    first = zip_dependencies(tree, ['lib', 'tasks'])
    for path in tree.rglob('*'):
        os.utime(path, (1_234_567_890, 1_234_567_890))
    second = zip_dependencies(str(tree), ['tasks', 'lib'])
    assert first == second


def test_absolute_lib_inside_cwd_is_accepted(tree: pathlib.Path):
    data = zip_dependencies(tree, [tree / 'tasks'])
    assert _names(data) == ['tasks/a.wdl', 'tasks/b.wdl']


def test_lib_outside_cwd_raises(tree: pathlib.Path, tmp_path: pathlib.Path):
    outside = tmp_path / 'outside'
    outside.mkdir()
    (outside / 'x.wdl').write_text('version 1.0\n')
    with pytest.raises(ValueError, match='outside'):
        zip_dependencies(tree, [outside])
    with pytest.raises(ValueError, match='outside'):
        zip_dependencies(tree, ['../outside'])


def test_missing_lib_raises(tree: pathlib.Path):
    with pytest.raises(ValueError, match='does not exist'):
        zip_dependencies(tree, ['nope'])


def test_symlinked_directories_are_not_followed(tree: pathlib.Path):
    (tree / 'lib' / 'linked').symlink_to(tree / 'tasks', target_is_directory=True)
    assert not any(
        n.startswith('lib/linked') for n in _names(zip_dependencies(tree, ['lib']))
    )


def test_python_imports_resolve_from_zip_root(
    tree: pathlib.Path,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
):
    zip_path = tmp_path / 'deps.zip'
    zip_path.write_bytes(zip_dependencies(tree / 'lib', ['pkg']))
    monkeypatch.syspath_prepend(str(zip_path))
    monkeypatch.delitem(sys.modules, 'pkg', raising=False)
    import pkg  # type: ignore[import-not-found]

    assert pkg.VALUE == 42  # noqa: PLR2004
    assert str(zip_path) in pkg.__file__
    monkeypatch.delitem(sys.modules, 'pkg', raising=False)


def test_symlinked_lib_keeps_the_link_name(tree: pathlib.Path):
    (tree / 'Linked.wdl').symlink_to(tree / 'Structs.wdl')
    (tree / 'linked_tasks').symlink_to(tree / 'tasks', target_is_directory=True)
    assert _names(zip_dependencies(tree, ['Linked.wdl', 'linked_tasks'])) == [
        'Linked.wdl',
        'linked_tasks/a.wdl',
        'linked_tasks/b.wdl',
    ]


def test_cwd_reached_through_a_symlink(tree: pathlib.Path, tmp_path: pathlib.Path):
    alias = tmp_path / 'alias'
    alias.symlink_to(tree, target_is_directory=True)
    assert _names(zip_dependencies(alias, [alias / 'tasks', 'Structs.wdl'])) == [
        'Structs.wdl',
        'tasks/a.wdl',
        'tasks/b.wdl',
    ]


def test_lib_named_by_a_trailing_parent_reference(tree: pathlib.Path):
    assert _names(zip_dependencies(tree, ['tasks/..'])) == _names(
        zip_dependencies(tree, ['.']),
    )


def test_symlink_inside_cwd_pointing_outside_raises(
    tree: pathlib.Path,
    tmp_path: pathlib.Path,
):
    outside = tmp_path / 'outside.wdl'
    outside.write_text('version 1.0\n')
    (tree / 'escape.wdl').symlink_to(outside)
    with pytest.raises(ValueError, match='outside'):
        zip_dependencies(tree, ['escape.wdl'])
