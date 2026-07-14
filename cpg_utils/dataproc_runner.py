"""Dataproc client wrapper for running Hail jobs on ephemeral clusters.

A non-Hail alternative to cpg_utils.dataproc. Instead of wrapping the cluster
lifecycle inside a Hail Batch driver job and shelling out to hailctl, this
module drives the Dataproc REST API directly through the google-cloud-dataproc
library. It is intended for orchestrators such as Nextflow that need to submit
one or more Hail PySpark scripts to an ephemeral cluster and tear it down when
done.

Example:
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
        # Multiple jobs can be submitted to the same cluster before shutdown.
        cluster.run_job(script_uri, args=['--samplesheet', input_uri, '--stage', 'qc'])
"""

import os
import signal
import sys
import threading
import time
import uuid
from contextlib import suppress
from types import FrameType, TracebackType
from typing import Any

from cloudpathlib import GSClient, GSPath
from google.api_core import exceptions as gax_exceptions
from google.cloud import dataproc_v1, storage
from slugify import slugify

DEFAULT_HAIL_VERSION = '0.2.138'
DEFAULT_HAIL_IMAGE = '2.2-debian12'
DEFAULT_MASTER_TYPE = 'n1-highmem-4'
DEFAULT_WORKER_TYPE = 'n1-highmem-4'
DEFAULT_NUM_WORKERS = 2
DEFAULT_NUM_SECONDARY_WORKERS = 2
DEFAULT_MAX_IDLE_SECONDS = 900
DEFAULT_MAX_AGE_SECONDS = 86400
DEFAULT_BOOT_DISK_SIZE_GB = 100
DEFAULT_INIT_TIMEOUT_SECONDS = 600
DEFAULT_LOG_POLL_SECONDS = 5.0
DEFAULT_JOB_POLL_INITIAL_SECONDS = 10.0
DEFAULT_JOB_POLL_MAX_SECONDS = 60.0
DEFAULT_JOB_POLL_BACKOFF = 1.5

# GCP label keys/values: lowercase letters, digits, underscore, hyphen; <=63 chars.
# Keys must start with a lowercase letter. values may be empty.
_KEY_VALUE_INVALID = r'[^a-z0-9_-]'
# Cluster names are stricter: no underscores allowed, max length 52 chars.
_CLUSTER_INVALID = r'[^a-z0-9-]'

_TERMINAL_ERROR_STATES = frozenset({'ERROR', 'CANCELLED'})


class DataprocJobError(RuntimeError):
    """Raised when a Dataproc job reaches a non-DONE terminal state.

    Attributes:
        job: The failing Dataproc Job returned by the API.
        state: The terminal state name, either ERROR or CANCELLED.
    """

    def __init__(self, job: dataproc_v1.Job, state: str) -> None:
        details = job.status.details or 'No details available'
        super().__init__(
            f'Dataproc job {job.reference.job_id} failed with state '
            f'{state}: {details}',
        )
        self.job = job
        self.state = state


def unique_cluster_name(name: str) -> str:
    """Build a unique Dataproc cluster name from a prefix.

    Dataproc cluster names allow only lowercase letters, digits, and hyphens,
    and must be at most 51 characters. The sanitised prefix is truncated to
    42 characters and a hyphen plus eight hex characters of uuid4 are
    appended so the total stays within the 51 character limit.

    Args:
        name: The desired cluster name prefix.

    Returns:
        A sanitised name with a unique eight-character hex suffix.

    (full result cannot be doctested due to the unique component)
    >>> unique_cluster_name('My_Cluster_1')[:12]
    'my-cluster-1'
    """
    # sanitised name, truncated to 42 chars - the hyphen and uuid add 9 chars, for a 51-char dataproc max length name
    sanitised_name = slugify(name, regex_pattern=_CLUSTER_INVALID, max_length=42)
    if not sanitised_name[0].isalpha():
        raise ValueError(
            f'Invalid cluster name: {sanitised_name} - must start with a lowercase letter.',
        )

    return f'{sanitised_name}-{uuid.uuid4().hex[:8]}'


def sanitise_labels(labels: dict[str, str]) -> dict[str, str]:
    """Return a copy of labels with keys and values sanitised for GCP.

    Entries whose sanitised key does not start with a lowercase letter causes an error.

    Args:
        labels: Raw label mapping from caller input.

    Returns:
        A new dictionary containing only the valid sanitised entries.

    >>> sanitise_labels({'AR-GUID': 'Abc123', 'Bad Key!': 'x'})
    {'ar-guid': 'abc123', 'bad-key': 'x'}
    """
    failing_labels: dict[str, str] = {}
    result: dict[str, str] = {}
    for raw_key, raw_value in labels.items():
        key = slugify(raw_key, regex_pattern=_KEY_VALUE_INVALID, max_length=63)
        value = slugify(str(raw_value), regex_pattern=_KEY_VALUE_INVALID)
        if not key or not key[0].isalpha():
            failing_labels[key] = value
            continue
        result[key] = value
    if failing_labels:
        fail_string = f"""\
        During key-value sanitisation on the provided labels, the following failed to conform to requirements:
        Input: {labels}
        Failures: {failing_labels}\n
        Note: keys must start with a lowercase letter and contain only letters, digits, underscores, and hyphens.
        See https://docs.cloud.google.com/resource-manager/docs/labels-overview
        """
        raise ValueError(fail_string)
    return result


def parse_label_kvs(items: list[str]) -> dict[str, str]:
    """Parse key=value strings into a sanitised label dictionary.

    Intended to format CLI-passed arguments. Malformed entries missing an equals sign cause an error.

    Args:
        items: Raw key=value strings from the command line.

    Returns:
        A sanitised label dictionary.

    >>> parse_label_kvs(['ar-guid=abc', 'Stage=Align'])
    {'ar-guid': 'abc', 'stage': 'align'}
    """
    parsed: dict[str, str] = {}
    failing_kv: list[str] = []
    for item in items:
        if '=' not in item:
            failing_kv.append(item)
            continue
        key, value = item.split('=', 1)
        parsed[key] = value

    if failing_kv:
        fail_string = f"""\
        One or more key=value parameters did not contain an equals character, so they could not be parsed:
        {failing_kv}
        """
        raise ValueError(fail_string)
    return sanitise_labels(parsed)


def resolve_autoscaling_policy_uri(
    project: str | None,
    region: str | None,
    policy_ref: str,
) -> str:
    """Return the full resource URI for an autoscaling policy.

    Accepts either a bare policy ID or an already-qualified resource URI. If
    policy_ref already starts with projects/ it is returned unchanged.

    Args:
        project: GCP project ID owning the policy. Can be None if policy_ref is a full path.
        region: Dataproc region hosting the policy. Can be None if policy_ref is a full path.
        policy_ref: Bare policy ID or full resource URI.

    Returns:
        The fully qualified autoscaling policy resource URI.

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
    if any(value is None for value in [project, region, policy_ref]):
        raise ValueError(
            f'Invalid autoscaling policy components: {project}, {region}, {policy_ref}',
        )
    return f'projects/{project}/regions/{region}/autoscalingPolicies/{policy_ref}'


def upload_to_gcs(
    local_path: str,
    bucket: str,
    prefix: str = '',
    client: GSClient | None = None,
) -> str:
    """
    Upload a local file to GCS and return the gs:// URI.

    The uploaded blob path is bucket/prefix/basename. Passing an empty prefix
    uploads to the bucket root.

    Args:
        local_path: Path to the file on the local filesystem.
        bucket: Target bucket, either as gs://bucket-name or bucket-name.
        prefix: Blob path prefix within the bucket.
        client: Optional pre-built storage client, mainly for tests.

    Returns:
        The gs:// URI of the uploaded blob.
    """
    bucket_name = bucket.removeprefix('gs://').rstrip('/')
    dest = (
        GSPath(f'gs://{bucket_name}', client=client)
        / prefix.strip('/')
        / os.path.basename(local_path)
    )
    dest.upload_from(local_path)
    return str(dest)

def populate_packages(packages: list[str] | None = None, hail_version: str = DEFAULT_HAIL_VERSION) -> list[str]:
    """
    Blend any requested packages with default packages

    Args:
        packages:
        hail_version: str, the version of hail to use

    Returns:
        a list of packages. At minimum this will contain cpg-utils and a version of hail
        if hail was not user specified, it will be added at the default version pin.
    """
    new_packages: list[str] = []

    # two key packages
    cpg_utils_specified: bool = False
    hail_specified: bool = False

    # iterate over the provided package list
    for each_package in packages or []:
        if each_package.startswith('hail'):
            hail_specified = True
        elif each_package.startswith('cpg-utils'):
            cpg_utils_specified = True
        new_packages.append(each_package)

    if not cpg_utils_specified:
        new_packages.append('cpg-utils')
    if not hail_specified:
        new_packages.append(f'hail=={hail_version}')

    return new_packages


class HailDataprocCluster:
    """Ephemeral Dataproc cluster preconfigured for Hail PySpark jobs.

    Construction does not create the cluster. Call start, enter the object as
    a context manager, or invoke submit_job to lazily create it. submit_job
    returns the Dataproc job id without waiting. wait_for_job polls a
    submitted job until it reaches a terminal state and by default streams
    driver stdout to this process. run_job is a convenience wrapper that
    submits and then waits. Multiple jobs may be submitted to the same
    cluster before shutdown, and each submit_job call reuses the running
    cluster. shutdown deletes the cluster and is idempotent. The context
    manager runs shutdown on exit, and install_sigterm_cleanup wires it to
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
        packages: list[str] | None = None,
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
        self._packages = populate_packages(packages, hail_version=hail_version)
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
        self._gs_client = GSClient(storage_client=self._storage_client)

        self._started = False
        self._shutdown_called = False
        self._cluster: dataproc_v1.Cluster | None = None
        self._job_ids: list[str] = []

    @property
    def name(self) -> str:
        """The generated cluster name, prefix plus eight random hex characters."""
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

    @property
    def hail_version(self) -> str:
        return self._hail_version

    def start(self) -> dataproc_v1.Cluster:
        """
        Create the cluster if not already started and return it.

        Idempotent. If start has already been called, the existing cluster is returned without another API call.

        Returns:
            The created or already-running Dataproc Cluster.
        """
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
        # set started = True before launching. If the cluster times out during the creation it can enter an error state
        # by setting started=True early, any failures will be caught and managed by the context handler
        # ERROR state dataproc clusters need to be deleted manually, the idle TTL does not apply
        self._started = True
        self._cluster = operation.result()
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

        if self._packages:
            pkgs = '|'.join(self._packages)
            config['config']['gce_cluster_config']['metadata']['PKGS'] = pkgs
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
        """Upload a local file to the staging bucket and return the gs:// URI.

        The default prefix is scripts/<cluster_name>, which keeps artifacts
        from different clusters segregated. Pass an explicit prefix, including
        the empty string for the bucket root, to override.

        Args:
            local_path: Path to the file on the local filesystem.
            prefix: Optional override for the blob path prefix.

        Returns:
            The gs:// URI of the uploaded blob.
        """
        effective_prefix = prefix if prefix is not None else f'scripts/{self._name}'
        return upload_to_gcs(
            local_path=local_path,
            bucket=self._staging_bucket,
            prefix=effective_prefix,
            client=self._gs_client,
        )

    def submit_job(
        self,
        script_uri: str,
        *,
        args: list[str] | None = None,
        file_uris: list[str] | None = None,
        properties: dict[str, str] | None = None,
    ) -> str:
        """
        Submit a PySpark job to the cluster and return the Dataproc job id.

        Starts the cluster first if it has not been started. The cluster is not shut down after the job completes.
        Call shutdown or use the context manager when all jobs are done.

        Args:
            script_uri: gs:// URI of the main Python script to run.
            args: Positional arguments passed to the script.
            file_uris: Extra files localised into the job working directory
                on the cluster. Matches the Dataproc pyspark_job.file_uris field.
            properties: Extra Spark or Dataproc job properties.

        Returns:
            The Dataproc job id assigned by the API.
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
        """
        Poll a submitted job until it reaches a terminal state.

        When stream_logs is true, the job driver stdout is streamed to this process on a background thread
        so logs appear in orchestrators like Seqera in near-real time.
        The streaming thread is stopped whether the poll returns normally or raises.

        Args:
            job_id: The Dataproc job id returned by submit_job.
            stream_logs: Whether to stream driver stdout in the background.

        Returns:
            The final Dataproc Job on success.

        Raises:
            DataprocJobError: If the job reaches ERROR or CANCELLED instead
                of DONE.
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

        Convenience wrapper over submit_job followed by wait_for_job.

        Args:
            script_uri: gs:// URI of the main Python script to run.
            args: Positional arguments passed to the script.
            file_uris: Extra files localised into the job working directory.
            properties: Extra Spark or Dataproc job properties.
            stream_logs: Whether to stream driver stdout in the background.

        Returns:
            The final Dataproc Job on success.

        Raises:
            DataprocJobError: If the job fails, so orchestrator scripts
                propagate a non-zero exit on failure.
        """
        job_id = self.submit_job(
            script_uri,
            args=args,
            file_uris=file_uris,
            properties=properties,
        )
        return self.wait_for_job(job_id, stream_logs=stream_logs)

    def shutdown(self) -> None:
        """Delete the cluster.

        Idempotent and never raises. Warnings are printed to stderr on failure
        so this remains safe to call from a finally block or a signal handler.
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
    """Install a SIGTERM handler that shuts down a cluster and exits with code 1.

    Useful for pipeline orchestrators such as Nextflow that send SIGTERM on
    cancellation. Must be called from the main thread because Python signal
    registration only works there.

    Args:
        cluster: The cluster to shut down when SIGTERM is received.
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
    """Write any bytes beyond offset in blob to stdout and return the new offset.

    Uses a metadata reload followed by a range download so only the new bytes
    are transferred each tick.

    Args:
        blob: The GCS blob to read from.
        offset: Number of bytes already streamed from this blob.

    Returns:
        The updated byte offset after any new bytes were written.

    Raises:
        google.api_core.exceptions.NotFound: If the blob does not yet exist.
            The caller decides whether to keep polling.
    """
    blob.reload()
    size = blob.size or 0
    if size <= offset:
        return offset
    chunk = blob.download_as_bytes(start=offset, end=size - 1)
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
    """Stream Dataproc job driver stdout from GCS to this process.

    Dataproc writes driver stdout to rolling numbered files under the job
    driver_output_resource_uri, ending in .000000000, .000000001, and so on.
    Mirrors the behaviour of gcloud dataproc jobs wait.

    Args:
        job_client: Dataproc job controller client used to look up the driver
            output URI.
        storage_client: GCS storage client used to read the driver output.
        project: GCP project id.
        region: Dataproc region.
        job_id: Job id whose driver output should be streamed.
        stop_event: Event set by the caller to signal the streamer to exit.
        poll_interval: Seconds to sleep between polls.
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
                print(f'[log-stream] failed to get job {job_id}', flush=True)
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
                    with suppress(gax_exceptions.NotFound):
                        offset = _tail_blob_to_stdout(blob, offset)
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
    """Poll a job id until it reaches a terminal state.

    Args:
        job_client: Dataproc job controller client used to fetch the job.
        project: GCP project id.
        region: Dataproc region.
        job_id: Dataproc job id.
        initial_interval: Seconds to sleep between the first two polls.
        max_interval: Ceiling on the polling interval.
        backoff: multiple applied to the interval after each poll, up to max_interval.

    Returns:
        The final Dataproc Job on success.

    Raises:
        DataprocJobError: If the job reaches a non-DONE terminal state, so
            orchestrators propagate a non-zero exit rather than silently
            continuing.
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
