"""
test case for cpg-utils.config
"""


import toml

from cpg_utils.config import get_config, set_config_paths


# pylint: disable=unused-argument


def test_read_config(test_conf):
    """
    test_conf : test TOML configuration
    """
    conf = get_config()
    assert conf.keys() == {'hail', 'workflow', 'storage'}
    assert conf['workflow']['dataset'] == 'mito-disease'


def test_new_conf(tmp_path):
    """
    tmp_path : pytest fixture
    """

    temp_conf = {'test': {'value': 1}}
    tmp_toml_path = tmp_path / 'conf.toml'
    with open(tmp_toml_path, 'w', encoding='utf-8') as handle:
        toml.dump(temp_conf, handle)

    set_config_paths([str(tmp_toml_path)])
    conf = get_config()
    assert conf.keys() == {'test'}
    assert conf['test']['value'] == 1
