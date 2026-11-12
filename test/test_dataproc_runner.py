import importlib
import io
import os
import pathlib
import sys
import unittest
import zipfile
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from cpg_utils.dataproc_runner import (
    DEFAULT_HAIL_VERSION,
    HailDataprocCluster,
    parse_label_kvs,
    resolve_autoscaling_policy_uri,
    sanitise_labels,
    validate_python_file_uris,
    zip_python_package,
)


def _make_cluster(**kwargs) -> HailDataprocCluster:  # noqa: ANN003
    return HailDataprocCluster(
        project='test-project',
        region='australia-southeast1',
        cluster_name_prefix='test-cluster',
        staging_bucket='gs://staging',
        temp_bucket='gs://temp',
        cluster_client=MagicMock(),
        job_client=MagicMock(),
        storage_client=MagicMock(),
        **kwargs,
    )


class TestDataprocRunnerPackages(unittest.TestCase):
    def test_default_packages_populate_pkgs_metadata(self):
        cluster = _make_cluster()
        config = cluster._build_cluster_config()
        pkgs = config['config']['gce_cluster_config']['metadata']['PKGS']
        self.assertEqual(pkgs, f'cpg-utils|hail=={DEFAULT_HAIL_VERSION}')

    def test_custom_packages_populate_pkgs_metadata(self):
        cluster = _make_cluster(packages=['cpg-utils', 'hail==0.2.999', 'gnomad'])
        config = cluster._build_cluster_config()
        pkgs = config['config']['gce_cluster_config']['metadata']['PKGS']
        self.assertEqual(pkgs, 'cpg-utils|hail==0.2.999|gnomad')

    def test_empty_pkgs(self):
        cluster = _make_cluster(packages=[])
        config = cluster._build_cluster_config()
        self.assertEqual(
            config['config']['gce_cluster_config']['metadata'].get('PKGS'),
            f'cpg-utils|hail=={DEFAULT_HAIL_VERSION}',
        )

    def test_null_pkgs(self):
        cluster = _make_cluster(packages=None)
        config = cluster._build_cluster_config()
        self.assertEqual(
            config['config']['gce_cluster_config']['metadata']['PKGS'],
            f'cpg-utils|hail=={DEFAULT_HAIL_VERSION}',
        )


class TestDataprocRunnerServiceAccount(unittest.TestCase):
    def test_service_account_omitted_by_default(self):
        cluster = _make_cluster()
        config = cluster._build_cluster_config()
        self.assertIsNone(cluster.service_account)
        self.assertNotIn(
            'service_account',
            config['config']['gce_cluster_config'],
        )

    def test_explicit_service_account(self):
        sa = 'dataproc-test@test-project.iam.gserviceaccount.com'
        cluster = _make_cluster(service_account=sa)
        config = cluster._build_cluster_config()
        self.assertEqual(cluster.service_account, sa)
        self.assertEqual(
            config['config']['gce_cluster_config']['service_account'],
            sa,
        )


_DISK_ROLES = ('master_config', 'worker_config', 'secondary_worker_config')


class TestDataprocRunnerDiskConfig(unittest.TestCase):
    def test_default_disk_config(self):
        config = _make_cluster()._build_cluster_config()['config']
        for role in _DISK_ROLES:
            disk_config = config[role]['disk_config']
            self.assertEqual(disk_config['boot_disk_type'], 'pd-standard')
            # num_local_ssds is omitted by default rather than set to 0.
            self.assertNotIn('num_local_ssds', disk_config)

    def test_custom_boot_disk_type(self):
        config = _make_cluster(boot_disk_type='pd-ssd')._build_cluster_config()[
            'config'
        ]
        for role in _DISK_ROLES:
            self.assertEqual(config[role]['disk_config']['boot_disk_type'], 'pd-ssd')

    def test_local_ssds_added_to_every_role(self):
        config = _make_cluster(num_local_ssds=2)._build_cluster_config()['config']
        for role in _DISK_ROLES:
            self.assertEqual(config[role]['disk_config']['num_local_ssds'], 2)


def test_good_keys():
    kv_pairs = ['good=key', 'also=good']
    assert parse_label_kvs(kv_pairs) == {'good': 'key', 'also': 'good'}


def test_missing_equals_keys():
    kv_pairs = ['good=key', 'bad']
    with pytest.raises(ValueError) as ve:
        parse_label_kvs(kv_pairs)
    assert (
        'One or more key=value parameters did not contain an equals character, so they could'
        in str(ve.value)
    )


def test_key_sanitation():
    kv_pairs = ['good=key', 'also=good', r'what\'s_this!=#ARGH#']
    assert parse_label_kvs(kv_pairs) == {
        'good': 'key',
        'also': 'good',
        'what-s_this': 'argh',
    }


def test_sanitise_labels_good():
    assert sanitise_labels({'AR-GUID': 'Abc123', 'Bad Key!': 'x'}) == {
        'ar-guid': 'abc123',
        'bad-key': 'x',
    }


def test_sanitise_labels_bad():
    with pytest.raises(ValueError) as ve:
        sanitise_labels({'1AR-GUID': 'Abc123'})
    assert "Failures: {'1ar-guid': 'abc123'}" in str(ve.value)


def test_autoscaling_policy_resolver():
    assert resolve_autoscaling_policy_uri(policy_ref='projects/stub') == 'projects/stub'


def test_autoscaling_policy_resolver_from_parts():
    assert (
        resolve_autoscaling_policy_uri(
            policy_ref='1234',
            project='project',
            region='aus',
        )
        == 'projects/project/regions/aus/autoscalingPolicies/1234'
    )


def test_autoscaling_policy_resolver_fails():
    with pytest.raises(ValueError) as ve:
        _ = resolve_autoscaling_policy_uri(policy_ref='not_projects/stub')
    assert 'Invalid autoscaling policy' in str(ve.value)


def _submitted_pyspark_job(cluster: HailDataprocCluster) -> dict:
    request = cluster._job_client.submit_job.call_args.kwargs['request']
    return request['job']['pyspark_job']


def test_submit_job_without_python_file_uris_is_unchanged():
    cluster = _make_cluster()
    cluster.submit_job('gs://staging/main.py', args=['--a'], file_uris=['gs://b/f'])
    assert _submitted_pyspark_job(cluster) == {
        'main_python_file_uri': 'gs://staging/main.py',
        'properties': {},
        'args': ['--a'],
        'file_uris': ['gs://b/f'],
    }


def test_submit_job_passes_python_file_uris():
    cluster = _make_cluster()
    uris = ['gs://staging/h/helpers.zip', 'gs://staging/u/util.py']
    cluster.submit_job('gs://staging/main.py', python_file_uris=uris)
    assert _submitted_pyspark_job(cluster)['python_file_uris'] == uris


def test_run_job_forwards_python_file_uris():
    cluster = _make_cluster()
    with patch.object(HailDataprocCluster, 'wait_for_job') as wait:
        cluster.run_job(
            'gs://staging/main.py',
            python_file_uris=['gs://staging/helpers.zip'],
            stream_logs=False,
        )
    assert _submitted_pyspark_job(cluster)['python_file_uris'] == [
        'gs://staging/helpers.zip',
    ]
    wait.assert_called_once()


@pytest.mark.parametrize(
    'uris',
    [
        ['gs://staging/helpers.tar.gz'],
        ['gs://staging/a/util.py', 'gs://staging/b/util.py'],
    ],
)
def test_invalid_python_file_uris_rejected_before_cluster_start(uris: list[str]):
    cluster = _make_cluster()
    with pytest.raises(ValueError, match='python_file_uris'):
        cluster.submit_job('gs://staging/main.py', python_file_uris=uris)
    cluster._cluster_client.create_cluster.assert_not_called()
    assert not cluster.is_started


def test_validate_python_file_uris_accepts_supported_types():
    validate_python_file_uris(['gs://b/a.py', 'gs://b/b.zip', 'hdfs:///c.egg'])


_PACKAGE_VALUE = 42


def _write_package(root: pathlib.Path, name: str = 'my_helpers') -> pathlib.Path:
    pkg = root / name
    (pkg / 'sub').mkdir(parents=True)
    (pkg / '__pycache__').mkdir()
    (pkg / '__init__.py').write_text('')
    (pkg / 'annotations.py').write_text(f'VALUE = {_PACKAGE_VALUE}\n')
    (pkg / 'sub' / '__init__.py').write_text('')
    (pkg / 'sub' / 'mod.py').write_text('from my_helpers.annotations import VALUE\n')
    (pkg / 'sub' / 'stale.pyc').write_bytes(b'bytecode')
    (pkg / '__pycache__' / 'annotations.cpython-313.pyc').write_bytes(b'bytecode')
    return pkg


def test_zip_python_package_layout(tmp_path: pathlib.Path):
    pkg = _write_package(tmp_path)
    with zipfile.ZipFile(io.BytesIO(zip_python_package(pkg))) as archive:
        names = archive.namelist()
    assert names == [
        'my_helpers/__init__.py',
        'my_helpers/annotations.py',
        'my_helpers/sub/__init__.py',
        'my_helpers/sub/mod.py',
    ]


def test_zip_python_package_is_deterministic(tmp_path: pathlib.Path):
    pkg = _write_package(tmp_path)
    first = zip_python_package(pkg)
    os.utime(pkg / 'annotations.py', (0, 0))
    (pkg / '__pycache__' / 'new.cpython-313.pyc').write_bytes(b'changed')
    assert zip_python_package(pkg) == first


def test_zip_python_package_is_importable(tmp_path: pathlib.Path):
    pkg = _write_package(tmp_path / 'src')
    zip_path = tmp_path / 'my_helpers.zip'
    zip_path.write_bytes(zip_python_package(pkg))
    sys.path.insert(0, str(zip_path))
    try:
        mod = importlib.import_module('my_helpers.sub.mod')
        assert mod.VALUE == _PACKAGE_VALUE
        assert str(zip_path) in str(mod.__file__)
    finally:
        sys.path.remove(str(zip_path))
        for name in [m for m in sys.modules if m.split('.')[0] == 'my_helpers']:
            del sys.modules[name]


def test_zip_python_package_uses_given_dir_name(tmp_path: pathlib.Path):
    pkg = _write_package(tmp_path)
    cwd = os.getcwd()
    os.chdir(pkg)
    try:
        data = zip_python_package('.')
    finally:
        os.chdir(cwd)
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        assert archive.namelist()[0] == 'my_helpers/__init__.py'


def test_zip_python_package_skips_dot_prefixed_and_non_py(tmp_path: pathlib.Path):
    pkg = _write_package(tmp_path)
    (pkg / '.env').write_text('TOKEN=secret')
    (pkg / '.DS_Store').write_bytes(b'')
    (pkg / '.hidden.py').write_text('')
    (pkg / 'notes.txt').write_text('')
    (pkg / '.git').mkdir()
    (pkg / '.git' / 'HEAD').write_text('ref')
    # Dot-prefixed directories are not walked, so a venv's .py and .so files are
    # neither zipped nor rejected as extension modules.
    venv = pkg / '.venv' / 'lib'
    venv.mkdir(parents=True)
    (venv / 'site.py').write_text('')
    (venv / 'fast.cpython-311-darwin.so').write_bytes(b'')
    with zipfile.ZipFile(io.BytesIO(zip_python_package(pkg))) as archive:
        names = archive.namelist()
    assert names == [
        'my_helpers/__init__.py',
        'my_helpers/annotations.py',
        'my_helpers/sub/__init__.py',
        'my_helpers/sub/mod.py',
    ]


def _package_with(root: pathlib.Path, name: str) -> pathlib.Path:
    """A valid package plus one extra file, or a dangling symlink for 'dangling.py'."""
    pkg = _write_package(root)
    if name == 'dangling.py':
        (pkg / 'sub' / name).symlink_to('nowhere.py')
    else:
        (pkg / 'sub' / name).write_bytes(b'')
    return pkg


@pytest.mark.parametrize(
    ('make', 'message'),
    [
        (lambda root: root / 'missing', 'is not a directory'),
        (lambda root: _write_package(root, 'hail-scripts'), 'not a valid Python'),
        (lambda root: _write_package(root, 'import'), 'not a valid Python'),
        (
            lambda root: (root / 'empty').mkdir() or root / 'empty',
            'contains no files',
        ),
        (
            lambda root: _package_with(root, 'fast.cpython-311-darwin.so'),
            r'compiled extension modules.*fast\.cpython-311-darwin\.so',
        ),
        (
            lambda root: _package_with(root, 'fast.pyd'),
            r'compiled extension modules.*fast\.pyd',
        ),
        (
            lambda root: _package_with(root, 'dangling.py'),
            r'dangling\.py.*does not have a real target',
        ),
    ],
)
def test_zip_python_package_rejects(
    tmp_path: pathlib.Path,
    make: Callable[[pathlib.Path], pathlib.Path],
    message: str,
):
    with pytest.raises(ValueError, match=message):
        zip_python_package(make(tmp_path))


class _FakeUpload:
    """Records what upload_to_gcs would have uploaded."""

    def __init__(self) -> None:
        self.uploads: dict[str, bytes] = {}

    def __call__(
        self,
        local_path: str,
        bucket: str,
        prefix: str,
        client: Any,  # noqa: ARG002
    ) -> str:
        uri = f'{bucket}/{prefix}/{os.path.basename(local_path)}'
        self.uploads[uri] = pathlib.Path(local_path).read_bytes()
        return uri


def test_upload_pyfiles(tmp_path: pathlib.Path):
    pkg = _write_package(tmp_path)
    module = tmp_path / 'extra.py'
    module.write_text('X = 1\n')
    cluster = _make_cluster()
    fake = _FakeUpload()
    with patch('cpg_utils.dataproc_runner.upload_to_gcs', fake):
        uris = cluster.upload_pyfiles([pkg, str(module)])

    prefix = f'gs://staging/scripts/{cluster.name}/pyfiles/'
    assert all(uri.startswith(prefix) for uri in uris)
    assert uris[0].endswith('/my_helpers.zip')
    assert uris[1].endswith('/extra.py')
    assert fake.uploads[uris[0]] == zip_python_package(pkg)
    assert fake.uploads[uris[1]] == b'X = 1\n'
    # Pyfiles never start the cluster.
    assert not cluster.is_started


def test_upload_pyfiles_is_content_addressed(tmp_path: pathlib.Path):
    pkg = _write_package(tmp_path)
    cluster = _make_cluster()
    with patch('cpg_utils.dataproc_runner.upload_to_gcs', _FakeUpload()):
        first = cluster.upload_pyfiles([pkg])
        again = cluster.upload_pyfiles([pkg])
        (pkg / 'annotations.py').write_text('VALUE = 43\n')
        changed = cluster.upload_pyfiles([pkg])
    assert first == again
    assert changed != first
    assert posix_basename(changed[0]) == posix_basename(first[0]) == 'my_helpers.zip'


def test_upload_pyfiles_prefix_override(tmp_path: pathlib.Path):
    module = tmp_path / 'extra.py'
    module.write_text('X = 1\n')
    cluster = _make_cluster()
    with patch('cpg_utils.dataproc_runner.upload_to_gcs', _FakeUpload()):
        (uri,) = cluster.upload_pyfiles([module], prefix='shared/pyfiles/')
        (root_uri,) = cluster.upload_pyfiles([module], prefix='')
    digest = uri.split('/')[-2]
    assert uri == f'gs://staging/shared/pyfiles/{digest}/extra.py'
    assert root_uri == f'gs://staging/{digest}/extra.py'


@pytest.mark.parametrize(
    ('names', 'message'),
    [
        (['notes.txt'], 'must be a package directory'),
        (['missing.py'], 'does not exist'),
        (['a/util.py', 'b/util.py'], 'share a basename'),
        (['helpers.zip'], 'not a valid zip archive'),
        (['helpers.egg'], 'not a valid zip archive'),
    ],
)
def test_upload_pyfiles_rejects(tmp_path: pathlib.Path, names: list[str], message: str):
    for name in names:
        if name != 'missing.py':
            path = tmp_path / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('')
    cluster = _make_cluster()
    fake = _FakeUpload()
    with (
        patch('cpg_utils.dataproc_runner.upload_to_gcs', fake),
        pytest.raises(ValueError, match=message),
    ):
        cluster.upload_pyfiles([tmp_path / name for name in names])
    assert not fake.uploads


def test_upload_pyfiles_accepts_real_zip(tmp_path: pathlib.Path):
    archive = tmp_path / 'helpers.zip'
    archive.write_bytes(zip_python_package(_write_package(tmp_path)))
    cluster = _make_cluster()
    fake = _FakeUpload()
    with patch('cpg_utils.dataproc_runner.upload_to_gcs', fake):
        (uri,) = cluster.upload_pyfiles([archive])
    assert fake.uploads[uri] == archive.read_bytes()


@pytest.mark.parametrize('make', [str, pathlib.Path])
def test_upload_pyfiles_rejects_single_path(
    tmp_path: pathlib.Path,
    make: Callable[[pathlib.Path], Any],
):
    pkg = _write_package(tmp_path)
    cluster = _make_cluster()
    fake = _FakeUpload()
    with (
        patch('cpg_utils.dataproc_runner.upload_to_gcs', fake),
        pytest.raises(TypeError, match='not a single path'),
    ):
        cluster.upload_pyfiles(make(pkg))
    assert not fake.uploads


def test_submit_job_rejects_single_python_file_uri():
    cluster = _make_cluster()
    with pytest.raises(TypeError, match='not a single URI'):
        cluster.submit_job(
            'gs://staging/main.py',
            python_file_uris='gs://staging/helpers.zip',  # type: ignore[arg-type]
        )
    cluster._cluster_client.create_cluster.assert_not_called()
    assert not cluster.is_started


def posix_basename(uri: str) -> str:
    return uri.rsplit('/', 1)[-1]
