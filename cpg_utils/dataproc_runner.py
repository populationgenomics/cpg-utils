"""
Dataproc client wrapper for running Hail jobs on ephemeral clusters.

This is a non-Hail alternative to`cpg_utils.dataproc`.
Instead of wrapping the cluster lifecycle inside a Hail Batch driver job and using hailctl, to manate, it uses the
Dataproc REST API directly via the google-cloud-dataproc library.
It is intended for orchestrators (e.g. Nextflow) that need to submit one or more Hail PySpark scripts to an ephemeral
cluster and tear it down when done.

Typical usage::

    from cpg_utils.dataproc_runner import HailDataprocCluster

    with HailDataprocCluster(
        project='my-project',
        region='australia-southeast1',
        cluster_name_prefix='my-pipeline',
        staging_bucket='gs://my-staging',
        temp_bucket='gs://my-temp',
        labels={'ar-guid': 'abc123', 'stage': 'align'},
    ) as cluster:
        script_uri = cluster.upload('local_script.py')
        input_uri = cluster.upload('samplesheet.tsv')
        cluster.run_job(
            script_uri,
            args=['--samplesheet', input_uri, '--out', 'gs://out/vds'],
        )
        # Multiple jobs can be submitted to the same cluster before shutdown:
        cluster.run_job(script_uri, args=['--samplesheet', input_uri, '--stage', 'qc'])
"""

import os
import re
import signal
import sys
import threading
import time
import uuid
from enum import unique
from types import FrameType, TracebackType
from typing import Any

from google.api_core import exceptions as gax_exceptions
from google.cloud import dataproc_v1, storage

DEFAULT_HAIL_VERSION = '0.2.138'
DEFAULT_HAIL_IMAGE = '2.2-debian12'
DEFAULT_MASTER_TYPE = 'n1-highmem-4'
DEFAULT_WORKER_TYPE = 'n1-highmem-4'
DEFAULT_NUM_WORKERS = 2
DEFAULT_NUM_SECONDARY_WORKERS = 0
DEFAULT_MAX_IDLE_SECONDS = 900
DEFAULT_MAX_AGE_SECONDS = 86400
DEFAULT_BOOT_DISK_SIZE_GB = 100
DEFAULT_INIT_TIMEOUT_SECONDS = 600
DEFAULT_LOG_POLL_SECONDS = 5.0
DEFAULT_JOB_POLL_INITIAL_SECONDS = 10.0
DEFAULT_JOB_POLL_MAX_SECONDS = 60.0
DEFAULT_JOB_POLL_BACKOFF = 1.5

# GCP label keys/values: lowercase letters, digits, underscore, hyphen; <=63 chars.
# Keys must start with a lowercase letter; values may be empty.
_LABEL_INVALID = re.compile(r'[^a-z0-9_-]')
# Cluster names are stricter: no underscores allowed.
_CLUSTER_INVALID = re.compile(r'[^a-z0-9-]')

_TERMINAL_ERROR_STATES = frozenset({'ERROR', 'CANCELLED'})


class DataprocJobError(RuntimeError):
    """Raised when a Dataproc job reaches a non-``DONE`` terminal state.

    The failing :class:`~google.cloud.dataproc_v1.Job` is exposed on ``.job``
    and its terminal state name (``'ERROR'`` / ``'CANCELLED'``) on ``.state``,
    so callers that need to inspect the failure can do so without re-fetching.
    """

    def __init__(self, job: dataproc_v1.Job, state: str) -> None:
        details = job.status.details or 'No details available'
        super().__init__(
            f'Dataproc job {job.reference.job_id} failed with state '
            f'{state}: {details}',
        )
        self.job = job
        self.state = state


def sanitise_gcp_string(
    s: str,
    *,
    pattern: re.Pattern[str] = _LABEL_INVALID,
    max_len: int = 63
) -> str:
    """
    Coerce ``s`` into a valid GCP label key/value.

    Lowercases, substitutes disallowed characters with ``-``, and truncates to max_len characters.
    The default pattern allows underscores (for label keys/values)
    Use the alternative pattern `_CLUSTER_INVALID` for the stricter dataproc cluster-name rules.

    >>> sanitise_gcp_string('My Cool Label')
    'my-cool-label'
    >>> sanitise_gcp_string('foo_bar-123')
    'foo_bar-123'
    >>> len(sanitise_gcp_string('X' * 200))
    63
    >>> len(sanitise_gcp_string('X' * 200, max_len=2))
    2
    """
    return pattern.sub('-', s.lower())[:max_len]


def unique_cluster_name(name: str) -> str:
    """sanitise ``name`` for use as a Dataproc cluster name.

    Cluster names are stricter than labels: they cannot contain underscores.

    >>> unique_cluster_name('My_Cluster_1')
    'my-cluster-1'
    """
    # sanitised name, truncated to 42 chars - the hyphen and uuid add 9 chars, for a 51-char dataproc max length name
    sanitised_name = sanitise_gcp_string(name, pattern=_CLUSTER_INVALID, max_len=42)
    return f'{sanitised_name}-{uuid.uuid4().hex[:8]}'


def sanitise_labels(labels: dict[str, str]) -> dict[str, str]:
    """Return a copy of ``labels`` with keys and values sanitised for GCP.

    Entries whose key does not start with a lowercase letter after sanitisation
    are dropped with a warning to stderr, matching the GCP label constraints.

    >>> sanitise_labels({'AR-GUID': 'Abc123', 'Bad Key!': 'x'})
    {'ar-guid': 'abc123', 'bad-key-': 'x'}
    >>> sanitise_labels({'1nvalid': 'x'})
    {}
    """
    result: dict[str, str] = {}
    for raw_key, raw_value in labels.items():
        key = sanitise_gcp_string(raw_key)
        value = sanitise_gcp_string(str(raw_value))
        if not key or not key[0].isalpha():
            print(
                f'Skipping label with invalid key {raw_key!r} '
                f'(sanitised to {key!r})',
                file=sys.stderr,
            )
            continue
        result[key] = value
    return result


def parse_label_kvs(items: list[str]) -> dict[str, str]:
    """Parse ``key=value`` strings (as from CLI args) into a sanitised label dict.

    Malformed entries (missing ``=``) are skipped with a warning.

    >>> parse_label_kvs(['ar-guid=abc', 'Stage=Align', 'no-equals'])
    {'ar-guid': 'abc', 'stage': 'align'}
    """
    parsed: dict[str, str] = {}
    for item in items:
        if '=' not in item:
            print(
                f'Skipping invalid label {item!r} (expected key=value)',
                file=sys.stderr,
            )
            continue
        key, value = item.split('=', 1)
        parsed[key] = value
    return sanitise_labels(parsed)


def resolve_autoscaling_policy_uri(
    project: str,
    region: str,
    policy_ref: str,
) -> str:
    """Return the full resource URI for an autoscaling policy.

    Accepts either a bare policy ID or an already-qualified
    ``projects/.../autoscalingPolicies/...`` URI.

    >>> resolve_autoscaling_policy_uri('proj', 'australia-southeast1', 'my-policy')
    'projects/proj/regions/australia-southeast1/autoscalingPolicies/my-policy'
    >>> resolve_autoscaling_policy_uri(
    ...     'proj', 'australia-southeast1',
    ...     'projects/other/regions/eu/autoscalingPolicies/foo',
    ... )
    'projects/other/regions/eu/autoscalingPolicies/foo'
    """
    if policy_ref.startswith('projects/'):
        return policy_ref
    return f'projects/{project}/regions/{region}/autoscalingPolicies/{policy_ref}'


def upload_to_gcs(
    local_path: str,
    bucket: str,
    prefix: str = '',
    storage_client: storage.Client | None = None,
) -> str:
    """
    Upload a file `local_path` to `<bucket>/<prefix>/<basename>` - return the ``gs://`` URI.

    `bucket` may be either `gs://bucket-name` or `bucket-name`.
    Passing an empty `prefix` uploads to the bucket root.
    """
    client = storage_client or storage.Client()
    bucket_name = bucket.removeprefix('gs://').rstrip('/')
    basename = os.path.basename(local_path)
    blob_name = f'{prefix.strip("/")}/{basename}' if prefix else basename
    client.bucket(bucket_name).blob(blob_name).upload_from_filename(local_path)
    gcs_uri = f'gs://{bucket_name}/{blob_name}'
    print(f'Uploaded {local_path} to {gcs_uri}')
    return gcs_uri


class HailDataprocCluster:
    """Ephemeral Dataproc cluster preconfigured for Hail PySpark jobs.

    Lifecycle:

    - Constructing the object does *not* create the cluster; call :meth:`start`
      (or enter as a context manager) to do that.
    - :meth:`submit_job` will lazily :meth:`start` the cluster if it hasn't been
      started yet, then submit the job and return the Dataproc job id.
    - :meth:`wait_for_job` polls a submitted job until it reaches a terminal
      state and (by default) streams driver stdout to this process's stdout.
    - :meth:`run_job` is a convenience wrapper over submit + wait.
    - Multiple jobs may be submitted to the same cluster before :meth:`shutdown`;
      each :meth:`submit_job` call re-uses the running cluster.
    - :meth:`shutdown` deletes the cluster and is idempotent; the context
      manager runs it on exit, and :func:`install_sigterm_cleanup` wires it to
      SIGTERM.
    """

    def __init__(
        self,
        project: str,
        region: str,
        cluster_name_prefix: str,
        staging_bucket: str,
        temp_bucket: str,
        *,
        master_type: str = DEFAULT_MASTER_TYPE,
        worker_type: str = DEFAULT_WORKER_TYPE,
        num_workers: int = DEFAULT_NUM_WORKERS,
        num_secondary_workers: int = DEFAULT_NUM_SECONDARY_WORKERS,
        max_idle_seconds: int = DEFAULT_MAX_IDLE_SECONDS,
        max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
        autoscaling_policy: str | None = None,
        labels: dict[str, str] | None = None,
        hail_version: str = DEFAULT_HAIL_VERSION,
        hail_image: str = DEFAULT_HAIL_IMAGE,
        boot_disk_size_gb: int = DEFAULT_BOOT_DISK_SIZE_GB,
        init_timeout_seconds: int = DEFAULT_INIT_TIMEOUT_SECONDS,
        cluster_client: dataproc_v1.ClusterControllerClient | None = None,
        job_client: dataproc_v1.JobControllerClient | None = None,
        storage_client: storage.Client | None = None,
    ) -> None:
        self._project = project
        self._region = region
        self._staging_bucket = staging_bucket.rstrip('/')
        self._temp_bucket = temp_bucket.rstrip('/')
        self._master_type = master_type
        self._worker_type = worker_type
        self._num_workers = num_workers
        self._num_secondary_workers = num_secondary_workers
        self._max_idle_seconds = max_idle_seconds
        self._max_age_seconds = max_age_seconds
        self._hail_version = hail_version
        self._hail_image = hail_image
        self._boot_disk_size_gb = boot_disk_size_gb
        self._init_timeout_seconds = init_timeout_seconds
        self._labels = sanitise_labels(labels or {})
        self._policy_uri: str | None = (
            resolve_autoscaling_policy_uri(project, region, autoscaling_policy)
            if autoscaling_policy
            else None
        )

        self._name = unique_cluster_name(cluster_name_prefix)
        # 8 hex chars of uniqueness; keeps the total name well under the 51-char limit.

        endpoint = f'{region}-dataproc.googleapis.com:443'
        self._cluster_client = cluster_client or dataproc_v1.ClusterControllerClient(
            client_options={'api_endpoint': endpoint},
        )
        self._job_client = job_client or dataproc_v1.JobControllerClient(
            client_options={'api_endpoint': endpoint},
        )
        self._storage_client = storage_client or storage.Client()

        self._started = False
        self._shutdown_called = False
        self._cluster: dataproc_v1.Cluster | None = None
        self._job_ids: list[str] = []

    @property
    def name(self) -> str:
        """The generated cluster name (prefix + 8 random hex chars)."""
        return self._name

    @property
    def project(self) -> str:
        return self._project

    @property
    def region(self) -> str:
        return self._region

    @property
    def labels(self) -> dict[str, str]:
        """A copy of the sanitised labels applied to the cluster."""
        return dict(self._labels)

    @property
    def autoscaling_policy_uri(self) -> str | None:
        return self._policy_uri

    @property
    def job_ids(self) -> list[str]:
        """Job ids of every job submitted to this cluster, in submission order."""
        return list(self._job_ids)

    @property
    def is_started(self) -> bool:
        return self._started

    def start(self) -> dataproc_v1.Cluster:
        """Create the cluster. Idempotent: returns the existing cluster if already started."""
        if self._started and self._cluster is not None:
            return self._cluster

        cluster_config = self._build_cluster_config()
        print(f"Creating cluster '{self._name}' in {self._region}...")
        operation = self._cluster_client.create_cluster(
            request={
                'project_id': self._project,
                'region': self._region,
                'cluster': cluster_config,
            },
        )
        self._cluster = operation.result()
        self._started = True
        print(f"Cluster '{self._name}' created successfully.")
        return self._cluster

    def _build_cluster_config(self) -> dict[str, Any]:
        init_action_uri = (
            f'gs://hail-common/hailctl/dataproc/{self._hail_version}/init_notebook.py'
        )
        wheel_uri = (
            f'gs://hail-common/hailctl/dataproc/{self._hail_version}/'
            f'hail-{self._hail_version}-py3-none-any.whl'
        )

        disk_config = {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': self._boot_disk_size_gb,
        }

        config: dict[str, Any] = {
            'project_id': self._project,
            'cluster_name': self._name,
            'labels': dict(self._labels),
            'config': {
                'gce_cluster_config': {
                    'zone_uri': '',  # Let Dataproc auto-zone within region.
                    'metadata': {
                        'WHEEL': wheel_uri,
                        'PKGS': f'cpg_utils|hail=={self._hail_version}',
                    },
                },
                'master_config': {
                    'num_instances': 1,
                    'machine_type_uri': self._master_type,
                    'disk_config': dict(disk_config),
                },
                'worker_config': {
                    'num_instances': self._num_workers,
                    'machine_type_uri': self._worker_type,
                    'disk_config': dict(disk_config),
                },
                'software_config': {
                    'image_version': self._hail_image,
                    'properties': {
                        'spark:spark.serializer': (
                            'org.apache.spark.serializer.KryoSerializer'
                        ),
                        'spark:spark.kryo.registrator': (
                            'is.hail.kryo.HailKryoRegistrator'
                        ),
                        'dataproc:dataproc.logging.stackdriver.job.driver.enable': (
                            'true'
                        ),
                    },
                },
                'initialization_actions': [
                    {
                        'executable_file': init_action_uri,
                        'execution_timeout': {'seconds': self._init_timeout_seconds},
                    },
                ],
                'lifecycle_config': {
                    'auto_delete_ttl': {'seconds': self._max_age_seconds},
                    'idle_delete_ttl': {'seconds': self._max_idle_seconds},
                },
                'temp_bucket': self._temp_bucket.removeprefix('gs://'),
            },
        }

        if self._policy_uri:
            config['config']['autoscaling_config'] = {'policy_uri': self._policy_uri}

        # Add secondary (preemptible) workers if requested, or if autoscaling may scale them.
        if self._num_secondary_workers or self._policy_uri:
            config['config']['secondary_worker_config'] = {
                'num_instances': self._num_secondary_workers,
                'machine_type_uri': self._worker_type,
                'is_preemptible': True,
                'disk_config': dict(disk_config),
            }

        return config

    def upload(self, local_path: str, prefix: str | None = None) -> str:
        """Upload ``local_path`` to the staging bucket; return the ``gs://`` URI.

        Defaults to ``scripts/<cluster_name>/<basename>`` to keep artifacts for
        different clusters segregated. Pass an explicit ``prefix`` (including
        ``''`` for the bucket root) to override.
        """
        effective_prefix = prefix if prefix is not None else f'scripts/{self._name}'
        return upload_to_gcs(
            local_path=local_path,
            bucket=self._staging_bucket,
            prefix=effective_prefix,
            storage_client=self._storage_client,
        )

    def submit_job(
        self,
        script_uri: str,
        *,
        args: list[str] | None = None,
        file_uris: list[str] | None = None,
        properties: dict[str, str] | None = None,
    ) -> str:
        """Submit a PySpark job to the cluster; return the Dataproc job id.

        If the cluster has not been started, it is created first. The cluster
        is *not* shut down after the job completes — call :meth:`shutdown` (or
        use the context manager) when all jobs are done.

        ``file_uris`` are localised into the job's working directory on the
        cluster (matches the ``pyspark_job.file_uris`` Dataproc field).
        """
        if not self._started:
            self.start()

        pyspark_job: dict[str, Any] = {
            'main_python_file_uri': script_uri,
            # Jar paths are set by init_notebook.py in spark-defaults.conf.
            'properties': dict(properties) if properties else {},
        }
        if args:
            pyspark_job['args'] = list(args)
        if file_uris:
            pyspark_job['file_uris'] = list(file_uris)

        job = {
            'placement': {'cluster_name': self._name},
            'pyspark_job': pyspark_job,
        }

        print(f"Submitting Hail job to cluster '{self._name}'...")
        result = self._job_client.submit_job(
            request={
                'project_id': self._project,
                'region': self._region,
                'job': job,
            },
        )
        job_id = result.reference.job_id
        self._job_ids.append(job_id)
        print(f'Job submitted: {job_id}')
        return job_id

    def wait_for_job(
        self,
        job_id: str,
        *,
        stream_logs: bool = True,
    ) -> dataproc_v1.Job:
        """Poll ``job_id`` until it reaches a terminal state; return the final Job.

        Raises :class:`DataprocJobError` if the job reaches ``ERROR`` or
        ``CANCELLED`` instead of ``DONE``. When ``stream_logs`` is true (the
        default), the job's driver stdout is streamed to this process's stdout
        in a background thread so logs appear in orchestrators like Seqera in
        near-real time; the log-streaming thread is stopped in either exit
        path.
        """
        stop_event = threading.Event()
        log_thread: threading.Thread | None = None
        if stream_logs:
            log_thread = threading.Thread(
                target=_stream_driver_logs,
                args=(
                    self._job_client,
                    self._storage_client,
                    self._project,
                    self._region,
                    job_id,
                    stop_event,
                ),
                daemon=True,
            )
            log_thread.start()
        try:
            return _poll_job(self._job_client, self._project, self._region, job_id)
        finally:
            stop_event.set()
            if log_thread is not None:
                log_thread.join(timeout=30)

    def run_job(
        self,
        script_uri: str,
        *,
        args: list[str] | None = None,
        file_uris: list[str] | None = None,
        properties: dict[str, str] | None = None,
        stream_logs: bool = True,
    ) -> dataproc_v1.Job:
        """Submit a job and wait for it to reach a terminal state.

        Convenience wrapper over :meth:`submit_job` + :meth:`wait_for_job`.
        Raises :class:`DataprocJobError` on non-success terminal states so
        orchestrator scripts propagate a non-zero exit on failure.
        """
        job_id = self.submit_job(
            script_uri,
            args=args,
            file_uris=file_uris,
            properties=properties,
        )
        return self.wait_for_job(job_id, stream_logs=stream_logs)

    def shutdown(self) -> None:
        """Delete the cluster. Idempotent; never raises.

        Warnings are printed to stderr on failure so that this remains safe to
        call from a ``finally`` block or a signal handler.
        """
        if self._shutdown_called:
            return
        self._shutdown_called = True
        if not self._started:
            return
        print(f"Deleting cluster '{self._name}'...")
        try:
            operation = self._cluster_client.delete_cluster(
                request={
                    'project_id': self._project,
                    'region': self._region,
                    'cluster_name': self._name,
                },
            )
            operation.result()
            print(f"Cluster '{self._name}' deleted.")
        except Exception as e:  # noqa: BLE001 - cleanup path must not raise
            print(
                f"Warning: failed to delete cluster '{self._name}': {e}",
                file=sys.stderr,
            )

    def __enter__(self) -> 'HailDataprocCluster':
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.shutdown()


def install_sigterm_cleanup(cluster: HailDataprocCluster) -> None:
    """Install a SIGTERM handler that shuts down ``cluster`` and exits(1).

    Useful for pipeline orchestrators (e.g. Nextflow) that send SIGTERM on
    cancellation. Must be called from the main thread.
    """

    def handler(signum: int, _frame: FrameType | None) -> None:
        print(
            f'Received signal {signum}, shutting down cluster {cluster.name}...',
            file=sys.stderr,
        )
        try:
            cluster.shutdown()
        finally:
            sys.exit(1)

    signal.signal(signal.SIGTERM, handler)


def _tail_blob_to_stdout(blob: storage.Blob, offset: int) -> int:
    """Write any bytes beyond ``offset`` in ``blob`` to stdout; return new offset.

    Uses a metadata reload + range download so we transfer only the new bytes
    each tick. Raises :class:`google.api_core.exceptions.NotFound` if the blob
    doesn't exist yet — the caller decides whether to keep polling.
    """
    blob.reload()
    size = blob.size or 0
    if size <= offset:
        return offset
    chunk = blob.download_as_bytes(start=offset, end=size)
    sys.stdout.write(chunk.decode('utf-8', errors='replace'))
    sys.stdout.flush()
    return size


def _stream_driver_logs(
    job_client: dataproc_v1.JobControllerClient,
    storage_client: storage.Client,
    project: str,
    region: str,
    job_id: str,
    stop_event: threading.Event,
    poll_interval: float = DEFAULT_LOG_POLL_SECONDS,
) -> None:
    """Stream Dataproc job driver stdout from GCS to this process's stdout.

    Dataproc writes driver stdout to rolling numbered files at
    ``driver_output_resource_uri``: ``driveroutput.000000000``,
    ``driveroutput.000000001``, and so on. Mirrors ``gcloud dataproc jobs wait``.
    """
    file_index = 0
    offset = 0
    uri_prefix: str | None = None
    print(f'[log-stream] started for job {job_id}', flush=True)
    while not stop_event.is_set():
        if uri_prefix is None:
            try:
                job = job_client.get_job(
                    request={
                        'project_id': project,
                        'region': region,
                        'job_id': job_id,
                    },
                )
                uri_prefix = job.driver_output_resource_uri or None
            except Exception:  # noqa: BLE001 - log streamer must not kill the run
                pass
            if not uri_prefix:
                time.sleep(poll_interval)
                continue
            print(f'[log-stream] found driver output at {uri_prefix}', flush=True)

        try:
            path = uri_prefix.removeprefix('gs://')
            bucket_name, blob_prefix = path.split('/', 1)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(f'{blob_prefix}.{file_index:09d}')
            try:
                offset = _tail_blob_to_stdout(blob, offset)
            except gax_exceptions.NotFound:
                # Current-index blob hasn't been created yet; try again next tick.
                pass
            else:
                next_blob = bucket.blob(f'{blob_prefix}.{file_index + 1:09d}')
                if next_blob.exists():
                    # One final range-read of the current file: Dataproc may have
                    # appended bytes between our last download and the roll.
                    try:
                        offset = _tail_blob_to_stdout(blob, offset)
                    except gax_exceptions.NotFound:
                        pass
                    print(
                        f'[log-stream] rolling to output file {file_index + 1}',
                        flush=True,
                    )
                    file_index += 1
                    offset = 0
        except Exception as e:  # noqa: BLE001 - log streamer must not kill the run
            print(f'[log-stream] warning: {e}', file=sys.stderr)

        time.sleep(poll_interval)
    print(f'[log-stream] stopped for job {job_id}', flush=True)


def _poll_job(
    job_client: dataproc_v1.JobControllerClient,
    project: str,
    region: str,
    job_id: str,
    initial_interval: float = DEFAULT_JOB_POLL_INITIAL_SECONDS,
    max_interval: float = DEFAULT_JOB_POLL_MAX_SECONDS,
    backoff: float = DEFAULT_JOB_POLL_BACKOFF,
) -> dataproc_v1.Job:
    """Poll ``job_id`` until it reaches a terminal state.

    Returns the final :class:`~google.cloud.dataproc_v1.Job` on success
    (``DONE``); raises :class:`DataprocJobError` for non-success terminal
    states (``ERROR``, ``CANCELLED``) so orchestrators propagate a non-zero
    exit rather than silently continuing.
    """
    poll_interval = initial_interval
    while True:
        job = job_client.get_job(
            request={'project_id': project, 'region': region, 'job_id': job_id},
        )
        state = dataproc_v1.JobStatus.State(job.status.state).name
        if state in _TERMINAL_ERROR_STATES:
            raise DataprocJobError(job, state)
        if state == 'DONE':
            print(f'Job {job_id} completed successfully.')
            return job
        print(f'Job {job_id} state: {state} (polling in {poll_interval}s)')
        time.sleep(poll_interval)
        poll_interval = min(poll_interval * backoff, max_interval)
