import pytest
import unittest
from unittest.mock import MagicMock

from cpg_utils.dataproc_runner import HailDataprocCluster, parse_label_kvs


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
        self.assertEqual(pkgs, 'cpg-utils')

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
            None,
        )

    def test_null_pkgs(self):
        cluster = _make_cluster(packages=None)
        config = cluster._build_cluster_config()
        self.assertEqual(
            config['config']['gce_cluster_config']['metadata']['PKGS'],
            'cpg-utils',
        )

def test_good_keys():
    kv_pairs = ['good=key', 'also=good']
    assert parse_label_kvs(kv_pairs) == {'good': 'key', 'also': 'good'}

def test_missing_equals_keys():
    kv_pairs = ['good=key', 'bad']
    with pytest.raises(ValueError) as ve:
        parse_label_kvs(kv_pairs)
    assert 'One or more key=value parameters did not contain an equals character, so they could' in str(ve.value)

def test_key_sanitation():
    kv_pairs = ['good=key', 'also=good', r'what\'s_this!=#ARGH#']
    assert parse_label_kvs(kv_pairs) == {'good': 'key', 'also': 'good', 'what-s_this': 'argh'}
