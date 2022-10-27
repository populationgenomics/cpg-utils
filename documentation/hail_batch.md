# Hail Batch methods

The primary execution environment for CPG code is cloud infrastructure, with workflows
scheduled using Hail Batch. This collection of methods is designed to make interactions
with batches, environments, and authentication protocols simpler.

The `init_batch` method is a wrapper for hail.init_batch, used to start a hail query batch
using the correct billing project and remote temporary directory sourced from the execution
environment.

```python
from cpg_utils.hail_batch import init_batch


def main():
    # initiate a hail query back end
    init_batch()

    # hail object methods
    ...
```

When running a Hail Batch workflow using the analysis-runner, the core Driver image will
have authentication set up, but separate child batches will start with fresh jobs.
When setting up those additional environments, helper methods are available to assist in
finding template images to use for child batches, and activating authentication within
those environments.

```python
import os
import hailtop.batch as hb

from cpg_utils.hail_batch import (
    authenticate_cloud_credentials_in_job,
    copy_common_env,
    output_path,
    image_path,
    remote_tmpdir
)


def main():
    service_backend = hb.ServiceBackend(
        billing_project=os.getenv('HAIL_BILLING_PROJECT'),
        remote_tmpdir=remote_tmpdir(),
    )
    batch = hb.Batch(
        name='create new batch',
        backend=service_backend,
        cancel_after_n_failures=1,
    )

    job = batch.new_job(name='job_1')

    # select the image to run the command inside
    job.image(image_path('docker_tag:1.1.1'))

    # authenticate the gcloud service user account
    authenticate_cloud_credentials_in_job(job=job)

    # copy all Driver image environment variables into the new environment
    copy_common_env(job=job)

    # run a command inside the container, creating an output file
    job.command(f'gsutil cp gs://a/file/path {job.outfile}')

    # create a permanent path to write to. This requires `CPG_DATASET`, `CPG_ACCESS_LEVEL`,
    # and `CPG_OUTPUT_PREFIX` to be set, which is done by analysis_runner.
    output_file = output_path('location_to_write_to', category='web')
    # .. creates path `gs://DATASET-ACCESS_LEVEL-web/location_to_write_to`

    batch.write_output(job.outfile, output_file)
```
