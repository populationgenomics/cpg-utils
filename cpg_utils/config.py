"""Provides access to config variables."""

import os
from typing import Optional, List, Dict, Any

import toml
from frozendict import frozendict

from cpg_utils import to_path

# We use these globals for lazy initialization, but pylint doesn't like that.
# pylint: disable=global-statement, invalid-name
_config_paths = _val.split(',') if (_val := os.getenv('CPG_CONFIG_PATH')) else []
_config: Optional[frozendict] = None  # Cached config, initialized lazily.


def _validate_configs(config_paths: list[str]) -> None:
    if [p for p in config_paths if not p.endswith('.toml')]:
        raise ValueError(
            f'All config files must have ".toml" extensions, got: {config_paths}'
        )

    paths = [to_path(p) for p in config_paths]
    if bad_paths := [p for p in paths if not p.exists()]:
        raise ValueError(f'Some config files do not exist: {bad_paths}')

    # Reading each file to validate syntax:
    exception_by_path = {}
    for p in paths:
        with p.open() as f:
            try:
                toml.loads(f.read())
            except toml.decoder.TomlDecodeError as e:
                exception_by_path[p] = e
    if exception_by_path:
        msg = f'Failed parsing some config files:'
        for path, exception in exception_by_path.items():
            msg += f'\n\t{path}: {exception}'
        raise ValueError(msg)


_validate_configs(_config_paths)


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
        _validate_configs(config_paths)
        _config_paths = config_paths
        os.environ['CPG_CONFIG_PATH'] = ','.join(_config_paths)
        _config = None  # Make sure the config gets reloaded.


def prepend_config_paths(config_paths: list[str]) -> None:
    """
    Prepend to the list of config paths. Equivalent to `dict.set_defaults`: any
    values in current CPG_CONFIG_PATH will have the precedence over the provided
    `config_paths` when merging the configs.
    """
    if _env_var := os.environ.get('CPG_CONFIG_PATH'):
        config_paths.extend(_env_var.split(','))

    set_config_paths(config_paths)


def append_config_paths(config_paths: list[str]) -> None:
    """
    Append to the list of config paths. Any values in new configs will have the
    precedence over the existing CPG_CONFIG_PATH when merging the configs.
    """
    if _env_var := os.environ.get('CPG_CONFIG_PATH'):
        config_paths = _env_var.split(',') + config_paths

    set_config_paths(config_paths)


def get_config(print_config=True) -> frozendict:
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
        if print_config:
            print(
                f'Configuration at {",".join(_config_paths)}:\n{toml.dumps(dict(_config))}'
            )

    return _config


def read_configs(config_paths: List[str]) -> frozendict:
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
    reference_prefix = "gs://cpg-common-main/references"
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
        with to_path(path).open() as f:
            config_str = f.read()
            update_dict(config, toml.loads(config_str))
    return frozendict(config)


def update_dict(d1: Dict, d2: Dict) -> None:
    """Updates the d1 dict with the values from the d2 dict recursively in-place."""
    for k, v2 in d2.items():
        v1 = d1.get(k)
        if isinstance(v1, dict) and isinstance(v2, dict):
            update_dict(v1, v2)
        else:
            d1[k] = v2


class ConfigError(Exception):
    """
    Error retrieving keys from config.
    """


def retrieve(key: list[str] | str, default: Any | None = None) -> str:
    """
    Retrieve key from config, assuming nested key specified as a list of strings.
    """
    if isinstance(key, str):
        key = [key]

    d = get_config()
    for k in key[:-1]:
        if k not in d:
            raise ConfigError(f'Key "{k}" not found in {d}')
        d = d[k]
    k = key[-1]
    if k not in d and not default:
        raise ConfigError(f'Key "{k}" not found in {d}')
    return d.get(k, default)
