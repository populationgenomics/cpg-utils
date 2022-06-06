"""Provides access to config variables."""

import os
from typing import Optional
from cloudpathlib import AnyPath
import toml

# We use these globals for lazy initialization, but pylint doesn't like that.
# pylint: disable=global-statement, invalid-name
_config_paths = (os.getenv('CPG_CONFIG_PATH') or '').split(',')  # See set_config_paths.
_config: Optional[dict] = None  # Cached config, initialized lazily.


def set_config_paths(config_paths: list[str]) -> None:
    """Sets the config paths that are used by subsequent calls to get_config.

    If this isn't called, the value of the CPG_CONFIG_PATH environment variable is used
    instead.

    Parameters
    ----------
    config_paths: list[str]
        A list of cloudpathlib-compatible paths to TOML files containing configurations.
    """

    global _config_paths, _config
    if _config_paths != config_paths:
        _config_paths = config_paths
        _config = None  # Make sure the config gets reloaded.


def get_config() -> dict:
    """Returns the configuration dictionary.

    Call `set_config_paths` beforehand to override the default path.
    See `read_configs` for the path value semantics.

    Notes
    -----
    Caches the result based on the config paths alone.

    Returns
    -------
    dict
    """

    global _config
    if _config is None:  # Lazily initialize the config.
        assert (
            _config_paths
        ), 'Either set the CPG_CONFIG_PATH environment variable or call set_config_paths'

        _config = read_configs(_config_paths)

        # Print the config content, which is helpful for debugging.
        print(f'Configuration at {",".join(_config_paths)}:\n{toml.dumps(_config)}')

    return _config


def read_configs(config_paths: list[str]) -> dict:
    """Creates a merged configuration from the given config paths.

    For a list of configurations (e.g. ['base.toml', 'override.toml']), the
    configurations get applied from left to right. I.e. the first config gets updated by
    values of the second config, etc.

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

    Returns
    -------
    dict
    """

    config: dict = {}
    for path in config_paths:
        with AnyPath(path).open() as f:
            config_str = f.read()
            update_dict(config, toml.loads(config_str))
    return config


def update_dict(d1: dict, d2: dict) -> None:
    """Updates the d1 dict with the values from the d2 dict recursively in-place."""
    for k, v2 in d2.items():
        v1 = d1.get(k)
        if isinstance(v1, dict) and isinstance(v2, dict):
            update_dict(v1, v2)
        else:
            d1[k] = v2
