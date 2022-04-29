"""Convenience functions related to permissions """
from typing import List
import os
import re

from cloudpathlib import AnyPath

group_name_santise_matcher = re.compile(r'[^\w_-]+')

DEFAULT_GROUP_CACHE_LOCATION = os.getenv(
    'CPG_GROUP_CACHE_LOCATION', 'gs://cpg-group-cache'
)


def group_name_to_filename(
    group_name: str,
    version: str,
    group_cache_location: str = DEFAULT_GROUP_CACHE_LOCATION,
) -> str:
    """
    Convert google group name to GCS safe directory name.
    Replace all non [alphanumeric characters and _-] with '_'.

    >>> _group_name_to_filename('my-group_with+chars@populationgenomics.org.au')
    'my-group_with_chars_populationgenomics_org_au'
    """
    base = group_name_santise_matcher.sub('_', group_name)
    return os.path.join(group_cache_location, base, version)


def get_group_members(group: str):
    """Returns the members of the given group.

    :param group: str The group to query.
    :returns: List[str] Members of the group
    """
    # TODO: change this to call serverless function
    return get_group_members_direct(group)


def get_group_members_direct(
    group: str, group_cache_location=DEFAULT_GROUP_CACHE_LOCATION
) -> List[str]:
    """Returns the members of the given group.

    :param group: str The group to query.
    :returns: List[str] Members of the group
    """
    filename = group_name_to_filename(group, 'latest', group_cache_location)
    return AnyPath(filename).read_text().split(',')
