import unittest
from unittest.mock import MagicMock

from cpg_utils.dataproc_runner import HailDataprocCluster


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
