"""
cached existence checking logic, ported from cpg-flow
https://github.com/populationgenomics/cpg-flow/blob/main/src/cpg_flow/utils.py

this collection of methods can be used to:
- detect if a single file or directory exists, without caching
- detect if a single file or directory exists, with caching
- cache existence checks across a whole directory for rapid checking of multiple adjacent files/directories
"""

import traceback  # noqa: I001
from os.path import basename, dirname

import logging
from functools import lru_cache

from cpg_utils import to_path, Path


@lru_cache
def exists(path: Path | str, verbose: bool = True) -> bool:
    """
    `exists_not_cached` that caches the result.

    The python code runtime happens entirely during the workflow construction,
    without waiting for it to finish, so there is no expectation that the object
    existence status would change during the runtime. This, this function uses
    `@lru_cache` to make sure that object existence is checked only once.
    """
    return exists_not_cached(path, verbose)


def exists_not_cached(path_or_string: Path | str, verbose: bool = True) -> bool:
    """
    Check if the object by path exists, where the object can be:
        * local file,
        * local directory,
        * cloud object,
        * cloud or local *.mt, *.ht, or *.vds Hail data, in which case it will check
          for the existence of a corresponding _SUCCESS object instead.
    @param path: path to the file/directory/object/mt/ht
    @param verbose: print on each check
    @return: True if the object exists
    """

    path = to_path(path_or_string)

    if path.suffix in {'.mt', '.ht'}:
        path /= '_SUCCESS'
    if path.suffix == '.vds':
        path /= 'variant_data/_SUCCESS'

    if verbose:
        try:
            res = check_exists_path(path)

        # a failure to detect the parent folder causes a crash
        # instead stick to a core responsibility - existence = False
        except FileNotFoundError as fnfe:
            logging.error(f'Failed checking {path}')
            logging.error(f'{fnfe}')
            return False
        except BaseException as be:
            traceback.print_exc()
            logging.error(f'Failed checking {path}')
            raise be
        exist_debug_statement = 'exists' if res else 'missing'
        logging.debug(f'Checked {path} [{exist_debug_statement}]')
        return res

    return check_exists_path(path)


def check_exists_path(test_path: Path) -> bool:
    """
    Check whether a path exists using a cached per-directory listing.
    NB. reversion to Strings prevents a get call, which is typically
    forbidden to local users
    """
    return basename(str(test_path)) in get_contents_of_path(dirname(str(test_path)))


@lru_cache
def get_contents_of_path(test_path: str) -> set[str]:
    """
    Get the contents of a GCS path, returning non-complete paths, eg:

    > get_contents_of_path('gs://my-bucket/my-dir/')
    {'my-file.txt'}

    """
    return {f.name for f in to_path(test_path.rstrip('/')).iterdir()}
