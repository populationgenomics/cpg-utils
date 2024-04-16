"""Provides access to config variables."""

import copy
import os
from shlex import quote
from typing import Any

import toml
from frozendict import frozendict

from cpg_utils import Path, to_path

AR_GUID_NAME = 'ar-guid'

# We use these globals for lazy initialization, but pylint doesn't like that.
# pylint: disable=global-statement, invalid-name
CONFIG_TYPE = frozendict[str, Any]
_config_paths = _val.split(',') if (_val := os.getenv('CPG_CONFIG_PATH')) else []
_config: CONFIG_TYPE | None = None  # Cached config, initialized lazily.

# region GET_SET_CONFIG


def _validate_configs(config_paths: list[str]) -> None:
    if [p for p in config_paths if not p.endswith('.toml')]:
        raise ValueError(
            f'All config files must have ".toml" extensions, got: {config_paths}',
        )

    paths = [to_path(p) for p in config_paths]
    if bad_paths := [p for p in paths if not p.exists()]:
        raise ValueError(f'Some config files do not exist: {bad_paths}')

    # Reading each file to validate syntax:
    exception_by_path: dict[Path, toml.decoder.TomlDecodeError] = {}
    for p in paths:
        with p.open() as f:
            try:
                toml.loads(f.read())
            except toml.decoder.TomlDecodeError as e:
                exception_by_path[p] = e
    if exception_by_path:
        msg = 'Failed parsing some config files:'
        for path, exception in exception_by_path.items():
            msg += f'\n\t{path}: {exception}'
        raise ValueError(msg)


def get_config_paths() -> list[str]:
    """
    Returns the config paths that are used by subsequent calls to get_config.

    If this isn't called, the value of the CPG_CONFIG_PATH environment variable is used
    instead.

    Returns
    -------
    list[str]
    """
    global _config_paths
    if not _config_paths:
        env_val = os.getenv('CPG_CONFIG_PATH')
        _config_paths = env_val.split(',') if env_val else []

        if not _config_paths:
            raise ConfigError(
                'Either set the CPG_CONFIG_PATH environment variable or call set_config_paths',
            )

    return _config_paths


def set_config_paths(config_paths: list[str]) -> None:
    """
    Sets the config paths that are used by subsequent calls to get_config.

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
    _new_config_paths = copy.copy(config_paths)
    if _config_paths:
        _new_config_paths.extend(_config_paths)

    set_config_paths(_new_config_paths)


def append_config_paths(config_paths: list[str]) -> None:
    """
    Append to the list of config paths. Any values in new configs will have the
    precedence over the existing CPG_CONFIG_PATH when merging the configs.
    """
    _new_config_paths = copy.copy(config_paths)
    if _config_paths:
        _new_config_paths = _config_paths + _new_config_paths

    set_config_paths(_new_config_paths)


def get_config(print_config: bool = False) -> CONFIG_TYPE:
    """
    Returns the configuration dictionary.
    Consider using `config_retrieve(keys)` instead.

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
        _config = read_configs(get_config_paths())

        # Print the config content, which is helpful for debugging.
        if print_config:
            print(
                f'Configuration at {",".join(_config_paths)}:\n{toml.dumps(dict(_config))}',
            )

    if not _config:
        raise ConfigError('No config found')

    return _config


def read_configs(config_paths: list[str]) -> CONFIG_TYPE:
    """
    Creates a merged configuration from the given config paths.
    This does NOT affect any state, re get_config.

    For a list of configurations (e.g. ['base.toml', 'override.toml']), the
    configurations get applied from left to right. I.e. the first config gets updated by
    values of the second config, etc.

    Returns
    -------
    dict
    """
    if not config_paths:
        raise ValueError('No config paths provided')

    config: dict = {}
    for path in config_paths:
        with to_path(path).open() as f:
            config_str = f.read()
            update_dict(config, toml.loads(config_str))
    return frozendict(config)


def update_dict(d1: dict, d2: dict) -> dict:
    """
    Updates the d1 dict with the values from the d2 dict recursively in-place.
    Returns the pointer to d1 (the same as )

    >>> update_dict({'a': 1, 'b': {'c': 1}}, {'b': {'c': 2, 'd': 2}})
    {'a': 1, 'b': {'c': 2, 'd': 2}}
    """
    for k, v2 in d2.items():
        v1 = d1.get(k)
        if isinstance(v1, dict) and isinstance(v2, dict):
            update_dict(v1, v2)
        else:
            d1[k] = v2

    return d1


# endregion GET_SET_CONFIG


class ConfigError(Exception):
    """
    Error retrieving keys from config.
    """


class UnsuppliedDefault:
    pass


def config_retrieve(
    key: list[str] | str,
    default: Any | None = UnsuppliedDefault,
    config: CONFIG_TYPE | dict[str, Any] | None = None,
) -> Any:
    """
    Retrieve key from config, assuming nested key specified as a list of strings.

    >> config_retrieve(['workflow', 'access_level'], config={'workflow': {'access_level': 'test'}})
    'test'

    >> config_retrieve(['workflow', 'access_level'], config={}, default='default')
    'default'

    >> config_retrieve('workflow', config={})
    ConfigError("Key 'workflow' not found in {}")

    >> config_retrieve(['key1', 'key2', 'key3'], config={'key1': {'key2': {}}})
    ConfigError('Key "key3" not found in {} (path: key1 -> key2)')

    Allow None as default value
    >> config_retrieve(['key1', 'key2', 'key3'], config={}, default=None) is None
    True
    """
    if default is UnsuppliedDefault:
        d = config if config is not None else get_config()
    else:
        try:
            d = config if config is not None else get_config()
        except ConfigError:
            return default

    if isinstance(key, str):
        key = [key]

    if not key:
        raise ValueError('Key cannot be empty')

    for idx, k in enumerate(key):
        if k not in d:
            if default is UnsuppliedDefault:
                message = f'Key "{k}" not found in {d}'
                if idx > 0:
                    key_bits = ' -> '.join(key[: idx + 1])
                    message += f' (path: {key_bits})'

                raise ConfigError(message)
            return default

        d = d[k]

    return d


def get_driver_image() -> str:
    """
    Get the driver image from the config.
    """
    return config_retrieve(['workflow', 'driver_image'])


def get_access_level() -> str:
    """
    Get access level from the config.
    """
    return config_retrieve(['workflow', 'access_level'])


def get_gcp_project() -> str:
    return config_retrieve(['workflow', 'dataset_gcp_project'])


def get_cpg_namespace(access_level: str | None = None) -> str:
    """
    Get storage namespace from the access level.
    """
    access_level = access_level or get_access_level()
    return 'test' if access_level == 'test' else 'main'


def try_get_ar_guid():
    """Attempts to get the AR GUID from the environment.

    This is a fallback for when the AR GUID is not available in the config.
    """
    return config_retrieve(['workflow', AR_GUID_NAME], default=None)


# region PATHS


def dataset_path(
    suffix: str,
    category: str | None = None,
    dataset: str | None = None,
    test: bool = False,
) -> str:
    """
    Returns a full path for the current dataset, given a category and a path suffix.

    This is useful for specifying input files, as in contrast to the `output_path`
    function, `dataset_path` does _not_ take the `workflow/output_prefix` config
    variable into account.

    Assumes the config structure like below, which is auto-generated by
    the analysis-runner:

    ```toml
    [workflow]
    access_level = "standard"

    [storage.default]
    default = "gs://thousand-genomes-main"
    web = "gs://cpg-thousand-genomes-main-web"
    analysis = "gs://cpg-thousand-genomes-main-analysis"
    tmp = "gs://cpg-thousand-genomes-main-tmp"
    web_url = "https://main-web.populationgenomics.org.au/thousand-genomes"

    [storage.thousand-genomes]
    default = "gs://cpg-thousand-genomes-main"
    web = "gs://cpg-thousand-genomes-main-web"
    analysis = "gs://cpg-thousand-genomes-main-analysis"
    tmp = "gs://cpg-thousand-genomes-main-tmp"
    web_url = "https://main-web.populationgenomics.org.au/thousand-genomes"
    ```

    Examples
    --------
    Assuming that the analysis-runner has been invoked with
    `--dataset fewgenomes --access-level test`:

    > from cpg_utils.hail_batch import dataset_path
    > dataset_path('1kg_densified/combined.mt')
    'gs://cpg-fewgenomes-test/1kg_densified/combined.mt'
    > dataset_path('1kg_densified/report.html', 'web')
    'gs://cpg-fewgenomes-test-web/1kg_densified/report.html'
    > dataset_path('1kg_densified/report.html', 'web', test=True)
    'gs://cpg-fewgenomes-test-web/1kg_densified/report.html'
    > dataset_path('1kg_densified/report.html', 'web_url')
    'https://main-web.populationgenomics.org.au/fewgenomes/1kg_densified/report.html'

    Notes
    -----
    * If you specify test=True, the `workflow/access_level` config variable is required

    Parameters
    ----------
    suffix : str
        A path suffix to append to the bucket.
    category : str, optional
        A category like "tmp", "web", etc., defaults to "default" if omited.
    dataset : str, optional
        Dataset name, takes precedence over the `workflow/dataset` config variable
    test : bool
        Return "test" namespace version of the path

    Returns
    -------
    str
    """

    config = get_config()
    if 'storage' not in config:
        raise ConfigError('Storage section not found in config')
    if dataset and dataset not in config['storage']:
        raise ConfigError(
            f'Storage section for dataset "{dataset}" not found in config. '
            f'Please check that you have permissions to the dataset. '
            f'Expected section: [storage.{dataset}]',
        )
    dataset = dataset or 'default'

    # manual redirect to test paths
    if test:
        if 'workflow' not in config:
            raise ConfigError('Workflow section not found in config')
        if 'access_level' not in config['workflow']:
            raise ConfigError('Access level not found in workflow section of config')
        if config['workflow']['access_level'] != 'test':
            section = get_config()['storage'][dataset]['test']
        else:
            section = get_config()['storage'][dataset]
    else:
        section = get_config()['storage'][dataset]

    category = category or 'default'
    prefix = section.get(category)
    if not prefix:
        raise ConfigError(
            f'Category "{category}" not found in storage section '
            f'for dataset "{dataset}": {section}',
        )

    return os.path.join(prefix, suffix)


def cpg_test_dataset_path(
    suffix: str,
    category: str | None = None,
    dataset: str | None = None,
) -> str:
    """
    CPG-specific method to get corresponding test paths when running
    from the main namespace.
    """
    return dataset_path(suffix, category, dataset, test=True)


def web_url(suffix: str = '', dataset: str | None = None, test: bool = False) -> str:
    """
    Web URL to match the dataset_path of category 'web_url'.
    """
    return dataset_path(suffix=suffix, dataset=dataset, category='web_url', test=test)


def output_path(
    suffix: str,
    category: str | None = None,
    dataset: str | None = None,
    test: bool = False,
) -> str:
    """
    Returns a full path for the given category and path suffix.

    In contrast to the `dataset_path` function, `output_path` takes the
    `workflow/output_prefix` config variable into account.

    Examples
    --------
    If using the analysis-runner, the `workflow/output_prefix` would be set to the
    value provided using the --output argument, e.g.:
    ```
    analysis-runner --dataset fewgenomes --access-level test --output 1kg_pca/v42` ...
    ```
    will use '1kg_pca/v42' as the base path to build upon in this method:

    > from cpg_utils.hail_batch import output_path
    > output_path('loadings.ht')
    'gs://cpg-fewgenomes-test/1kg_pca/v42/loadings.ht'
    > output_path('report.html', 'web')
    'gs://cpg-fewgenomes-test-web/1kg_pca/v42/report.html'

    Notes
    -----
    Requires the `workflow/output_prefix` config variable to be set, in addition to the
    requirements for `dataset_path`.

    Parameters
    ----------
    suffix : str
        A path suffix to append to the bucket + output directory.
    category : str, optional
        A category like "tmp", "web", etc., defaults to "default" if ommited.
    dataset : str, optional
        Dataset name, takes precedence over the `workflow/dataset` config variable
    test : bool, optional
        Boolean - if True, generate a test bucket path. Default to False.

    Returns
    -------
    str
    """
    output_prefix = config_retrieve(['workflow', 'output_prefix'])
    return dataset_path(
        os.path.join(output_prefix, suffix),
        category=category,
        dataset=dataset,
        test=test,
    )


def image_path(key: str) -> str:
    """
    Returns a path to a container image using key in config's "images" section.

    Examples
    --------
    >> image_path('bcftools')
    'australia-southeast1-docker.pkg.dev/cpg-common/images/bcftools:1.10.2'

    Assuming config structure as follows:

    ```toml
    [images]
    bcftools = 'australia-southeast1-docker.pkg.dev/cpg-common/images/bcftools:1.10.2'
    ```

    Parameters
    ----------
    key : str
        Describes the key within the `images` config section. Can list sections
        separated with '/'.

    Returns
    -------
    str
    """
    return config_retrieve(['images', *key.strip('/').split('/')])


def reference_path(key: str) -> str:
    """
    Returns a path to a reference resource using key in config's "references" section.

    Examples
    --------
    >> reference_path('vep_mount')
    'gs://cpg-common-main/references/vep/105.0/mount'
    >> reference_path('broad/genome_calling_interval_lists')
    'gs://cpg-common-main/references/hg38/v0/wgs_calling_regions.hg38.interval_list'

    Assuming config structure as follows:

    ```toml
    [references]
    vep_mount = 'gs://cpg-common-main/references/vep/105.0/mount'
    [references.broad]
    genome_calling_interval_lists = 'gs://cpg-common-main/references/hg38/v0/wgs_calling_regions.hg38.interval_list'
    ```

    Parameters
    ----------
    key : str
        Describes the key within the `references` config section. Can list sections
        separated with '/'.

    Returns
    -------
    str
    """
    return config_retrieve(['references', *key.strip('/').split('/')])


def genome_build() -> str:
    """
    Return the default genome build name
    """
    return config_retrieve(['references', 'genome_build'], default='GRCh38')


def get_gcloud_set_project(gcp_project: str | None = None) -> str:
    """
    Get the gcloud command to set the project.
    """
    gcp_project = gcp_project or get_gcp_project()
    command = ['gcloud', 'config', 'set', 'project', gcp_project]
    return ' '.join([quote(c) for c in command])
