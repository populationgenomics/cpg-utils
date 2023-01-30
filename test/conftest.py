"""
conftest to create pytest fixtures
"""

import os

import pytest

from cpg_utils.config import set_config_paths


PWD = os.path.dirname(__file__)
INPUT = os.path.join(PWD, 'input')

TEST_CONF = os.path.join(INPUT, 'test_conf.toml')
PROD_CONF = os.path.join(INPUT, 'prod_conf.toml')


@pytest.fixture(scope='function')
def test_conf():
    """
    loads the dummy test config
    """
    set_config_paths([TEST_CONF])


@pytest.fixture(scope='function')
def prod_conf():
    """
    loads the dummy prod config
    """
    set_config_paths([PROD_CONF])
