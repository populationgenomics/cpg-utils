"""
Collect: copy Workflow outputs to caller-chosen Destinations.
"""

import logging
import pathlib
import shutil
import threading
from collections.abc import Mapping, Sequence
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


def _run_copy(copy: _Copy) -> str | None:
    """Do one copy, returning a problem description instead of raising."""
    src, dst = to_path(copy.src), to_path(copy.dst)
    if str(src) == str(dst):
        logger.info(f'Output {copy.key} is already at {dst}; not copying')
        return None
    try:
        logger.info(f'Copying output {copy.key}: {src} -> {dst}')
        _copy_file(src, dst)
    except Exception as e:  # noqa: BLE001 - every failure is reported together
        return f'{copy.key}: copying {src} to {dst} failed: {type(e).__name__}: {e}'
    return None


def _plan_value(
    key: str,
    value: Any,
    destination: Destination,
    copies: list[_Copy],
) -> Any:
    if _is_file_value(value):
        dst = str(to_path(destination))
        copies.append(_Copy(key, value, dst))
        return dst
    logger.warning(f'Output {key} is not a file; destination ignored: {value!r}')
    return value


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
            result[key] = _plan_value(key, value, destination, copies)  # type: ignore[arg-type]
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
            _plan_value(f'{key}[{index}]', element, target, copies)
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
) -> dict[str, Any]:
    """
    Copy Workflow outputs to their Destinations.

    ``destinations`` maps a full output name (e.g. ``'MyWorkflow.bam'``) to one path,
    or to a list of paths for an ``Array[File]`` output, matched element by element.
    A resource group such as a BAM and its index is simply two keys. Outputs not named
    in ``destinations`` are ignored.

    Everything is checked before anything is copied, and every problem is reported
    in one :class:`CollectError`. A non-file output (a number, or a string that isn't a
    ``gs://`` path) is passed through unchanged with a warning. A copy is skipped when
    the source already is the destination. Copies run in parallel; every copy is
    attempted even if some fail. Rerunning overwrites the destinations.

    Args:
        outputs: the Workflow outputs, e.g. from ``watch_workflow``.
        destinations: ``{output name: path | [paths]}``.
        threads: number of parallel copies.

    Returns:
        ``{output name: destination | [destinations] | raw value}`` for each key in
        ``destinations``.

    Raises:
        CollectError: if any output is missing, null or the wrong shape, or any copy
            fails.

    Example::

        collected = collect_outputs(
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
            problems = [p for p in pool.map(_run_copy, copies) if p is not None]
        if problems:
            raise CollectError(problems)
    return result
