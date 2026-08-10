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
