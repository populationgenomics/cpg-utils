"""
Build a reproducible ``workflowDependencies`` zip from local files.
"""

import io
import logging
import os
import pathlib
import zipfile
from collections.abc import Sequence

logger = logging.getLogger(__name__)

# The earliest timestamp a zip entry can hold; fixed so the bytes never depend on mtime.
_FIXED_DATE_TIME = (1980, 1, 1, 0, 0, 0)
_FILE_MODE = 0o644
_UNIX_CREATE_SYSTEM = 3


def _files_under(top: pathlib.Path) -> list[pathlib.Path]:
    """Regular files at or below ``top``, without following symlinked directories."""
    if top.is_file():
        return [top]
    found = []
    for dirpath, _dirnames, filenames in os.walk(top, followlinks=False):
        for filename in filenames:
            path = pathlib.Path(dirpath, filename)
            if path.is_file():
                found.append(path)
    return found


def zip_dependencies(
    cwd: str | os.PathLike[str],
    libs: Sequence[str | os.PathLike[str]],
) -> bytes:
    """
    Zip ``libs`` into reproducible bytes for Cromwell's ``workflowDependencies``.

    Entries are named by their path relative to ``cwd``, which matches
    ``cd cwd && zip -r deps.zip lib/``. The zip holds regular files only, sorted by
    name, each with a fixed timestamp and permissions, so zipping the same tree twice
    gives identical bytes (and so the same Submission key).

    Args:
        cwd: the directory entry names are relative to, usually the directory that
            holds the main WDL.
        libs: files or directories to include, relative to ``cwd`` or absolute. Each
            must exist and lie inside ``cwd``.

    Raises:
        ValueError: if a lib is missing or lies outside ``cwd``.

    Example::

        deps = zip_dependencies('gatk-sv/wdl', ['tasks', 'Structs.wdl'])
    """
    root = pathlib.Path(cwd).resolve()
    names: dict[str, pathlib.Path] = {}
    for lib in libs:
        given = pathlib.Path(lib)
        candidate = given if given.is_absolute() else root / given
        if not candidate.exists():
            raise ValueError(f'Dependency {str(lib)!r} does not exist under {root}')
        top = candidate.resolve()
        if not top.is_relative_to(root):
            raise ValueError(f'Dependency {str(lib)!r} resolves outside {root}: {top}')
        for path in _files_under(top):
            names[path.relative_to(root).as_posix()] = path

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for name in sorted(names):
            info = zipfile.ZipInfo(name, date_time=_FIXED_DATE_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = _FILE_MODE << 16
            info.create_system = _UNIX_CREATE_SYSTEM
            archive.writestr(info, names[name].read_bytes())
    logger.debug(f'Zipped {len(names)} dependency file(s) from {root}')
    return buffer.getvalue()
