"""Provides access to config variables."""

import os
from typing import Any, Dict, Optional, MutableMapping
from cloudpathlib import AnyPath
import toml

_config_cache: Dict[str, MutableMapping[str, Any]] = dict()


def get_config(config_path: Optional[str] = None) -> MutableMapping[str, Any]:
    """Returns a configuration dictionary.

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

    Notes
    -----
    Caches the result based on the config path alone.

    Parameters
    ----------
    config_path: str, optional
        A cloudpathlib-compatible path to a TOML file containing the configuration. If
        this parameter not provided, the CPG_CONFIG_PATH environment variable must be
        set instead.

    Returns
    -------
    dict
    """

    if config_path is None:
        config_path = os.getenv('CPG_CONFIG_PATH')
    assert config_path

    cached = _config_cache.get(config_path)
    if cached is not None:
        return cached

    with AnyPath(config_path).open() as f:
        config_str = f.read()

    # Print the config content, which is helpful for debugging.
    print(f'Configuration at {config_path}:\n{config_str}')
    config = _config_cache[config_path] = toml.loads(config_str)

    return config
