"""
CPG utils
"""
import coloredlogs
import importlib
import pathlib
from typing import Union

from cloudpathlib import CloudPath
from cloudpathlib.anypath import to_anypath


log_fmt = '%(asctime)s %(levelname)s (%(name)s %(lineno)s): %(message)s'
coloredlogs.install(level='DEBUG', fmt=log_fmt)

# The AnyPath class https://cloudpathlib.drivendata.org/stable/anypath-polymorphism/
# is very handy to parse a string that can be either a cloud URL or a local posix path.
# However, AnyPath can't be used for type hinting, because neither CloudPath nor
# pathlib.Path derive from it. The AnyPath's constructor method doesn't actually return
# an instance of AnyPath class, but rather Union[CloudPath, pathlib.Path], and it's
# designed to dynamically pick a specific CloudPath or pathlib.Path subclass.
# Here we create an alias for such union to allow using simple "Path" in type hints:
Path = Union[CloudPath, pathlib.Path]

# We would still need to call AnyPath() to parse a string, which might be confusing.
# Something like to_path() would look better, so we are aliasing a handy method
# to_anypath to to_path, which returns exactly the Union type we are looking for:
to_path = to_anypath


def get_package_name() -> str:
    """
    Get name of the package.
    """
    return __name__.split('.', 1)[0]


def get_version() -> str:
    """
    Get package version.
    """
    return importlib.metadata.version(get_package_name())
