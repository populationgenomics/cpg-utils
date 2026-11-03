# Dataproc Runner

[dataproc_runner.py](../cpg_utils/dataproc_runner.py) is a less-Hail alternative
to [dataproc.py](../cpg_utils/dataproc.py). Instead of wrapping the cluster
lifecycle inside a Hail Batch driver job and shelling out to `hailctl`, it
drives the Dataproc REST API directly through the `google-cloud-dataproc`
library. This makes it suitable for orchestrators such as Nextflow that need
to submit one or more Hail PySpark scripts to an ephemeral cluster and tear
it down when done, without any dependency on a running Hail Batch.

The module is built around `HailDataprocCluster`, which can be used directly
or as a context manager. Construction does not create anything in GCP;
calling `start()` (or entering the context manager) creates the cluster,
`upload()` copies local files to the cluster's staging bucket, and
`submit_job`/`run_job` submit a PySpark script to the running cluster —
`run_job` additionally waits for the job to reach a terminal state and
streams driver stdout back to this process. Multiple jobs can be submitted to
the same cluster before it is shut down. `shutdown()` deletes the cluster and
is idempotent, so it's safe to call from a `finally` block, and
`install_sigterm_cleanup` wires it up as a SIGTERM handler for orchestrators
that cancel jobs by sending SIGTERM.

## Running a single job on an ephemeral cluster

```python
from cpg_utils.dataproc_runner import HailDataprocCluster

with HailDataprocCluster(
    project='my-project',
    region='australia-southeast1',
    cluster_name_prefix='my-pipeline',
    staging_bucket='gs://my-staging',
    temp_bucket='gs://my-temp',
    labels={'ar-guid': 'abc123', 'stage': 'combine'},
) as cluster:
    script_uri = cluster.upload('local_script.py')
    input_uri = cluster.upload('samplesheet.tsv')
    cluster.run_job(
        script_uri,
        args=['--samplesheet', input_uri, '--out', 'gs://out/vds'],
    )
    # Multiple jobs can be submitted to the same cluster before shutdown.
    cluster.run_job(script_uri, args=['--samplesheet', input_uri, '--stage', 'qc'])
```

## Shipping helper modules with a job (pyfiles)

A script that imports its own helper modules needs them on the PYTHONPATH of
the job. `upload_pyfiles` takes local package directories and `.py`, `.egg`
or `.zip` files, uploads them to the staging bucket and returns their URIs.
Pass those URIs to `submit_job` or `run_job` as `python_file_uris`. This is
the Dataproc `pyspark_job.python_file_uris` field, the equivalent of
`hailctl dataproc submit --pyfiles` in [dataproc.py](../cpg_utils/dataproc.py).

```python
with HailDataprocCluster(...) as cluster:
    script_uri = cluster.upload('scripts/densify.py')
    pyfile_uris = cluster.upload_pyfiles(['src/my_helpers', 'src/vcf_utils.py'])
    cluster.run_job(script_uri, python_file_uris=pyfile_uris, args=[...])
```

- **Package directories** are zipped with the directory as the top-level
  entry, so `src/my_helpers/` becomes `my_helpers.zip` and the script can use
  `import my_helpers.annotations`. The directory name must be a valid Python
  identifier. `__pycache__` directories and `.pyc`/`.pyo` files are left out,
  and the zip has fixed timestamps, so the same code always gives the same
  bytes. `zip_python_package` exposes the zipping step on its own.
- **Upload location.** Files go to
  `<staging_bucket>/scripts/<cluster_name>/pyfiles/<content-hash>/<name>`, or
  `<prefix>/<content-hash>/<name>` if you pass `prefix=`. Unchanged code gets
  the same URI each time. Changed code under the same name gets a new URI, so
  it cannot overwrite files that an earlier job on the same cluster is still
  using.
- **Validation.** `python_file_uris` must end in `.py`, `.egg` or `.zip`, and
  no two may share a basename, because Spark copies them into one working
  directory. `submit_job` checks this before starting the cluster. URI schemes
  are not checked: Dataproc accepts any HCFS URI, though `gs://` is normal.
- **Driver and executors.** Spark puts these files on the PYTHONPATH of both
  the driver and the executors. Plain Hail code only needs them on the driver,
  because executors run Hail's JVM backend, not user Python. Python UDF-style
  code, such as `rdd.map(my_helpers.fn)`, also works because the executors
  receive the files too.
- **pyfiles belong to one job.** Pass them to every job that needs them. A
  later job on the same cluster does not inherit them.

### Choosing between `python_file_uris`, `file_uris` and `packages`

| Option                     | Scope                     | Use for                                                           |
|----------------------------|---------------------------|-------------------------------------------------------------------|
| `python_file_uris`         | one job, on PYTHONPATH    | your own unpublished helper code that changes with the pipeline   |
| `file_uris`                | one job, working dir only | data or config files the job reads; they are **not** importable   |
| `packages=` (constructor)  | whole cluster, pip, boot  | published PyPI packages, including their dependencies             |

## Submitting without blocking, and cleaning up on SIGTERM

`submit_job` returns immediately with the Dataproc job id, and
`install_sigterm_cleanup` ensures the cluster is torn down if an orchestrator
like Nextflow cancels the run:

```python
from cpg_utils.dataproc_runner import HailDataprocCluster, install_sigterm_cleanup

cluster = HailDataprocCluster(
    project='my-project',
    region='australia-southeast1',
    cluster_name_prefix='my-pipeline',
    staging_bucket='gs://my-staging',
    temp_bucket='gs://my-temp',
)
install_sigterm_cleanup(cluster)

script_uri = cluster.upload('local_script.py')
job_id = cluster.submit_job(script_uri, args=['--stage', 'combine'])

# ... do other work, or let the orchestrator poll ...

cluster.wait_for_job(job_id)
cluster.shutdown()
```
