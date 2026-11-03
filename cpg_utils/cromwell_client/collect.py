"""
Collect: copy Workflow outputs to caller-chosen Destinations.
"""

import logging
import pathlib
import shutil
import threading
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, TypeAlias

from cloudpathlib import CloudPath
from cloudpathlib.gs import GSPath

from cpg_utils import Path, to_path
from cpg_utils.cromwell_client.errors import CollectError

logger = logging.getLogger(__name__)

# a local or cloud path; equivalent to `str | cpg_utils.Path`
Destination: TypeAlias = str | CloudPath | pathlib.Path
# copies one file: (source, destination), both as strings; raises on failure
CopyFile: TypeAlias = Callable[[str, str], None]

_local = threading.local()


@dataclass(frozen=True)
class _Copy:
    key: str
    src: str
    dst: str


def _is_file_value(value: Any) -> bool:
    """Whether an output value is a file Cromwell wrote (as opposed to a scalar)."""
    return isinstance(value, str) and value.startswith('gs://')


def _is_single_destination(destination: Any) -> bool:
    return isinstance(destination, str | pathlib.PurePath | CloudPath)


def _storage_client() -> Any:
    """A google.cloud.storage client per thread."""
    client = getattr(_local, 'storage_client', None)
    if client is None:
        from google.cloud import storage

        client = storage.Client()
        _local.storage_client = client
    return client


def _rewrite_gcs(src: GSPath, dst: GSPath) -> None:
    """
    Copy one GCS object to another with ``Blob.rewrite``.

    Unlike a single ``copyTo`` call (what cloudpathlib uses), rewrite works for
    multi-GB objects across buckets, locations and storage classes, by looping on the
    rewrite token until the copy is complete.
    """
    client = _storage_client()
    src_blob = client.bucket(src.bucket).blob(src.blob)
    dst_blob = client.bucket(dst.bucket).blob(dst.blob)
    token, written, total = dst_blob.rewrite(src_blob)
    while token is not None:
        logger.debug(f'Rewriting {src} -> {dst}: {written}/{total} bytes')
        token, written, total = dst_blob.rewrite(src_blob, token=token)


def _copy_file(src: Path, dst: Path) -> None:
    if isinstance(src, GSPath) and isinstance(dst, GSPath):
        _rewrite_gcs(src, dst)
        return
    if isinstance(dst, pathlib.Path):
        dst.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(src, CloudPath):
        src.copy(dst)
    elif isinstance(dst, CloudPath):
        dst.upload_from(src, force_overwrite_to_cloud=True)
    else:
        shutil.copyfile(src, dst)


def _run_copy(copy: _Copy, copy_file: CopyFile | None) -> str | None:
    """Do one copy, returning a problem description instead of raising."""
    src, dst = to_path(copy.src), to_path(copy.dst)
    if str(src) == str(dst):
        logger.info(f'Output {copy.key} is already at {dst}; not copying')
        return None
    try:
        logger.info(f'Copying output {copy.key}: {src} -> {dst}')
        if copy_file is None:
            _copy_file(src, dst)
        else:
            copy_file(str(src), str(dst))
    except Exception as e:  # noqa: BLE001 - every failure is reported together
        return f'{copy.key}: copying {src} to {dst} failed: {type(e).__name__}: {e}'
    return None


def _plan_value(
    key: str,
    value: Any,
    destination: Destination,
    copies: list[_Copy],
    problems: list[str],
) -> str:
    dst = str(to_path(destination))
    if _is_file_value(value):
        copies.append(_Copy(key, value, dst))
    else:
        problems.append(
            f'{key}: output is not a file, so it has no destination: {value!r:.200}',
        )
    return dst


def _plan(
    outputs: Mapping[str, Any],
    destinations: Mapping[str, Destination | Sequence[Destination]],
) -> tuple[dict[str, Any], list[_Copy]]:
    problems: list[str] = []
    copies: list[_Copy] = []
    result: dict[str, Any] = {}

    for key, destination in destinations.items():
        if key not in outputs:
            problems.append(f'{key}: not among the Workflow outputs')
            continue
        value = outputs[key]
        if value is None:
            problems.append(f'{key}: output is null')
            continue

        if _is_single_destination(destination):
            if isinstance(value, list | dict):
                problems.append(
                    f'{key}: output is a {type(value).__name__} but the destination '
                    'is a single path',
                )
                continue
            result[key] = _plan_value(key, value, destination, copies, problems)  # type: ignore[arg-type]
            continue

        targets = list(destination)  # type: ignore[arg-type]
        if not isinstance(value, list):
            problems.append(
                f'{key}: destination is a list but the output is a '
                f'{type(value).__name__}',
            )
            continue
        if len(value) != len(targets):
            problems.append(
                f'{key}: output has {len(value)} element(s) but {len(targets)} '
                'destination(s) were given',
            )
            continue
        element_problems = [
            f'{key}[{index}]: element is '
            + ('null' if element is None else f'a {type(element).__name__}')
            for index, element in enumerate(value)
            if element is None or isinstance(element, list | dict)
        ]
        if element_problems:
            problems.extend(element_problems)
            continue
        result[key] = [
            _plan_value(f'{key}[{index}]', element, target, copies, problems)
            for index, (element, target) in enumerate(zip(value, targets, strict=True))
        ]

    if problems:
        raise CollectError(problems)
    return result, copies


def collect_outputs(
    outputs: Mapping[str, Any],
    destinations: Mapping[str, Destination | Sequence[Destination]],
    *,
    threads: int = 8,
    copy_file: CopyFile | None = None,
) -> dict[str, Any]:
    """
    Copy Workflow outputs to their Destinations, and return every output where it
    now lives.

    ``destinations`` maps a full output name (e.g. ``'MyWorkflow.bam'``) to one path,
    or to a list of paths for an ``Array[File]`` output, matched element by element.
    A resource group such as a BAM and its index is simply two keys. ``destinations``
    is only for file outputs; outputs not named in it are returned unchanged.

    Everything is checked before anything is copied,
    and every problem is reported in one :class:`CollectError`. A copy is skipped when
    the source already is the destination. Copies run in parallel; every copy is
    attempted even if some fail. Rerunning overwrites the destinations.

    Args:
        outputs: the Workflow outputs, e.g. from ``watch_workflow``.
        destinations: ``{output name: path | [paths]}``.
        threads: number of parallel copies.
        copy_file: copies one file, given its source and destination as strings,
            and raises on failure. It is called from several threads at once.
            Defaults to copying with ``Blob.rewrite`` (gs→gs) or cloudpathlib; pass
            one to copy some other way, or to keep a test off GCS.

    Returns:
        A copy of ``outputs`` in which each key in ``destinations`` is replaced by its
        destination path (or list of paths), as a string. Every other output keeps
        its raw value.

    Raises:
        CollectError: if any output is missing, null, not a file or the wrong shape,
            or any copy fails.

    Example::

        located = collect_outputs(
            outputs,
            {
                'Align.bam': f'{prefix}/sample.bam',
                'Align.bai': f'{prefix}/sample.bam.bai',
                'Align.metrics': [f'{prefix}/a.txt', f'{prefix}/b.txt'],
            },
        )
    """
    result, copies = _plan(outputs, destinations)
    if copies:
        with ThreadPoolExecutor(max_workers=max(1, threads)) as pool:
            problems = [
                p
                for p in pool.map(lambda c: _run_copy(c, copy_file), copies)
                if p is not None
            ]
        if problems:
            raise CollectError(problems)
    return {**outputs, **result}
