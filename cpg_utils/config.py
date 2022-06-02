"""Provides access to config variables."""

import os
from typing import Optional

import yaml
from cloudpathlib import AnyPath
import toml

from cpg_utils import to_path


# We use these globals for lazy initialization, but pylint doesn't like that.
# pylint: disable=global-statement, invalid-name
_config_path = os.getenv('CPG_CONFIG_PATH')  # See set_config_path.
_config: Optional[dict] = None  # Cached config, initialized lazily.


def set_config_path(config_path: str) -> None:
    """Sets the config path that's used by subsequent calls to get_config.

    If this isn't called, the value of the CPG_CONFIG_PATH environment variable is used
    instead.

    Parameters
    ----------
    config_path: str
        A cloudpathlib-compatible path to a TOML file containing the configuration.
    """

    global _config_path, _config
    if _config_path != config_path:
        _config_path = config_path
        _config = None  # Make sure the config gets reloaded.


def get_config() -> dict:
    """Returns the configuration dictionary.

    Call set_config_path beforehand to override the default path.

    Examples
    --------
    Here's a typical configuration file in TOML format:

    [hail]
    billing_project = "tob-wgs"
    bucket = "cpg-tob-wgs-hail"

    [workflow]
    access_level = "test"
    dataset = "tob-wgs"
    dataset_gcp_project = "tob-wgs"
    driver_image = "australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:36c6d4548ef347f14fd34a5b58908057effcde82-hail-ad1fc0e2a30f67855aee84ae9adabc3f3135bd47"
    image_registry_prefix = "australia-southeast1-docker.pkg.dev/cpg-common/images"
    reference_prefix = "gs://cpg-reference"
    output_prefix = "plasma/chr22/v6"

    >>> from cpg_utils.config import get_config
    >>> get_config()['workflow']['dataset']
    'tob-wgs'

    Notes
    -----
    Caches the result based on the config path alone.

    Returns
    -------
    dict
    """

    global _config
    if _config is None:  # Lazily initialize the config.
        assert (
            _config_path
        ), 'Either set the CPG_CONFIG_PATH environment variable or call set_config_path'

        with AnyPath(_config_path).open() as f:
            config_str = f.read()

        # Print the config content, which is helpful for debugging.
        print(f'Configuration at {_config_path}:\n{config_str}')
        _config = toml.loads(config_str)

        # Add images and references YML configs as extra sections:
        _load_images_and_references(_config)

    return _config


def update_dict(d1: dict, d2: dict) -> None:
    """Updates the d1 dict with the values from the d2 dict recursively in-place."""
    for k, v2 in d2.items():
        v1 = d1.get(k)
        if isinstance(v1, dict) and isinstance(v2, dict):
            update_dict(v1, v2)
        else:
            d1[k] = v2


def _load_images_and_references(_config):
    for section in ['images', 'references']:
        with (to_path(__file__).parent / f'{section}.yml').open() as f:
            defaults = yaml.safe_load(f)
            if section in _config:
                update_dict(defaults, _config[section])
            _config[section] = defaults

        # If user specified a custom config, overriding defaults:
        if user_yml := _config['workflow'].get(f'{section}_config_path'):
            with to_path(user_yml).open() as f:
                update_dict(_config[section], yaml.safe_load(f))
