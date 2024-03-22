import os

from cloudpathlib import AnyPath

from cpg_utils.config import config_retrieve


def get_cached_group_members(
    group: str,
    members_cache_location: str | None = None,
) -> set[str]:
    """
    Get cached members of a group, based on the members_cache_location
    """
    group_name = group.split('@')[0]

    members_cache_location = members_cache_location or config_retrieve(
        ['infrastructure', 'members_cache_location'],
    )

    if not members_cache_location:
        raise ValueError('members_cache_location is not set')

    pathname = os.path.join(members_cache_location, group_name + '-members.txt')

    with AnyPath(pathname).open() as f:
        return {line.strip() for line in f.readlines() if line.strip()}


def is_member_in_cached_group(
    group: str,
    member: str,
    members_cache_location: str | None = None,
) -> bool:
    """
    Check if a member is in a group, based on the infrastructure config
    """
    return member.lower() in get_cached_group_members(
        group,
        members_cache_location=members_cache_location,
    )
