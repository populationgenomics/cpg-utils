import unittest
from unittest.mock import MagicMock

import pytest

from cpg_utils.dataproc_runner import (
    DEFAULT_HAIL_VERSION,
    HailDataprocCluster,
    parse_label_kvs,
    resolve_autoscaling_policy_uri,
    sanitise_labels,
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
