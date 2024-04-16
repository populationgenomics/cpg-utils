"""
test cases for cpg-utils.config
"""

import os
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

import toml

from cpg_utils.config import (
    ConfigError,
    config_retrieve,
    dataset_path,
    get_config,
    image_path,
    output_path,
    set_config_paths,
)

test_config_path = Path(__file__).parent / 'input' / 'test_conf.toml'
prod_config_path = Path(__file__).parent / 'input' / 'prod_conf.toml'

with open(test_config_path, encoding='utf-8') as handle:
    test_config = toml.load(handle)

with open(prod_config_path, encoding='utf-8') as handle:
    prod_config = toml.load(handle)


class TestConfig(TestCase):
    def setUp(self) -> None:

        super().setUp()

        # surprisingly there's nothing in the set which disallows NO Path,
        # so it safely acts as a reset
        set_config_paths([])

    def test_retrieve(self):
        self.assertEqual(
            'test',
            config_retrieve(
                ['workflow', 'access_level'],
                config={'workflow': {'access_level': 'test'}},
            ),
        )

        self.assertEqual(
            'default',
            config_retrieve(['workflow', 'access_level'], config={}, default='default'),
        )

        with self.assertRaises(ConfigError):
            config_retrieve(['workflow', 'access_level'], config={})

        with self.assertRaises(ConfigError):
            config_retrieve(['key1', 'key2', 'key3'], config={'key1': {'key2': {}}})

        # check that providing default=None isn't triggering some falsey check
        self.assertIsNone(
            config_retrieve(['key1', 'key2', 'key3'], config={}, default=None),
        )

    def test_retrieve_no_config(self):
        """
        Test the config_retrieve behaviour when no config is available
        """
        # we've got not config here
        with self.assertRaises(ConfigError):
            config_retrieve(['some-key'])

        self.assertIsNone(config_retrieve(['some-key'], default=None))

    @patch.dict(os.environ, {'CPG_CONFIG_PATH': test_config_path.as_posix()})
    def test_read_from_env(self):
        """
        test_conf : test TOML configuration
        """
        conf = get_config()

        self.assertDictEqual(dict(conf), test_config)

    def test_set_path(self):
        set_config_paths([str(test_config_path)])
        conf = get_config()
        self.assertDictEqual(dict(conf), test_config)

    @patch('cpg_utils.config.get_config')
    def test_dataset_path(self, mock_get_config: MagicMock):
        """
        Test dataset_path, minimal config
        """
        mock_get_config.return_value = {
            'storage': {
                'default': {
                    'default': 'fake://cpg-bucket',
                },
            },
        }
        self.assertEqual('fake://cpg-bucket/final', dataset_path('final'))

    @patch('cpg_utils.config.get_config')
    def test_dataset_path_different_dataset_different_category(
        self,
        mock_get_config: MagicMock,
    ):
        """
        Test dataset_path, minimal config
        """
        mock_get_config.return_value = {
            'storage': {
                'another-dataset': {
                    'another-category': 'fake://cpg-another-bucket',
                },
            },
        }
        self.assertEqual(
            'fake://cpg-another-bucket/other',
            dataset_path(
                'other',
                dataset='another-dataset',
                category='another-category',
            ),
        )

    @patch('cpg_utils.config.get_config')
    def test_dataset_path_test(
        self,
        mock_get_config: MagicMock,
    ):
        """
        Test dataset_path, minimal config
        """
        # no-access-level
        mock_get_config.return_value = {
            'storage': {
                'another-dataset': {
                    'default': 'fake://cpg-another-test-bucket',
                },
            },
        }

        with self.assertRaises(ConfigError):
            dataset_path('final', test=True)

        # standard access-level
        # this one requires a specific "test" section in the storage.[dataset]
        mock_get_config.return_value = {
            'workflow': {
                'access_level': 'standard',
            },
            'storage': {
                'default': {  # dataset
                    'test': {
                        'default': 'fake://cpg-test-bucket',
                    },
                },
            },
        }
        self.assertEqual(
            'fake://cpg-test-bucket/final',
            dataset_path('final', test=True),
        )

        # test access-level
        # can just read the storage[dataset].default bucket directly,
        # as it's already test
        mock_get_config.return_value = {
            'workflow': {
                'access_level': 'test',
            },
            'storage': {
                'default': {  # dataset
                    'default': 'fake://cpg-test-bucket',
                },
            },
        }
        self.assertEqual(
            'fake://cpg-test-bucket/final',
            dataset_path('final', test=True),
        )

    @patch('cpg_utils.config.get_config')
    def test_output_path(self, mock_get_config: MagicMock):
        """
        Test output_path
        """
        mock_get_config.return_value = {
            'workflow': {'output_prefix': 'output/prefix'},
            'storage': {
                'default': {
                    'default': 'fake://cpg-bucket',
                },
            },
        }
        self.assertEqual('fake://cpg-bucket/output/prefix/final', output_path('final'))

    @patch('cpg_utils.config.get_config')
    def test_image_path(self, mock_get_config: MagicMock):
        """
        Test image_path
        """
        mock_get_config.return_value = {'images': {'image-name': 'image/path:version'}}
        self.assertEqual('image/path:version', image_path('image-name'))
