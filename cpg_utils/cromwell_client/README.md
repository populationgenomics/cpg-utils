# `cpg_utils.cromwell_client`

## What it is

A Cromwell client that works under **any workflow manager**: Nextflow, a GCP Batch
task, or a plain Python script. It submits a Workflow, watches it, aborts it on a time
limit, logs a report when it finishes, and copies its outputs to where you want them.

It is **GCP only**. It uses Cromwell's GCP Batch backend, Google ID tokens and GCP
Secret Manager.

It runs alongside `cpg_utils.cromwell`, which is untouched. Inside Hail Batch,
`cpg_utils.cromwell` is still the right choice, because it turns each step into a Hail
job. This package runs everything in the calling process instead.

Nothing here imports `hail`, `hailtop`, `cpg_utils.config`, `cpg_utils.hail_batch` or
`cpg_utils.cromwell`. Every value is passed in as an argument (see
[Layering](#layering)).

## Quick start

```python
import logging

from cpg_utils.cromwell_client import (
    CromwellClient,
    build_workflow_options,
    run_workflow,
    zip_dependencies,
)

logging.basicConfig(level=logging.INFO)  # the client logs; it never prints

dataset = 'my-dataset'
access_level = 'standard'
dataset_gcp_project = 'my-dataset-project'
prefix = 'align/v1'

options = build_workflow_options(
    service_account_secret=f'{dataset}-cromwell-{access_level}-key',
    secret_project=dataset_gcp_project,
    google_project=dataset_gcp_project,
    logs_dir=f'gs://cpg-{dataset}-{access_level}-analysis/cromwell_logs/{prefix}',
    gcs_root=f'gs://cpg-{dataset}-{access_level}-tmp/cromwell',
    outputs_dir=f'gs://cpg-{dataset}-{access_level}/{prefix}',
    labels={'stage': 'align'},
    ar_guid='0f7d2a51-0000-0000-0000-000000000000',
)

result = run_workflow(
    CromwellClient(),
    'wdl/Align.wdl',
    options=options,
    input_files=['wdl/defaults.json'],
    inputs={'Align.sample_id': 'sample-1', 'Align.cram': 'gs://bucket/sample-1.cram'},
    dependencies=zip_dependencies('wdl', ['tasks', 'Structs.wdl']),
    destinations={
        # a single file
        'Align.metrics': f'gs://cpg-{dataset}-{access_level}/{prefix}/metrics.txt',
        # an Array[File], matched element by element
        'Align.plots': [
            f'gs://cpg-{dataset}-{access_level}/{prefix}/plot-1.png',
            f'gs://cpg-{dataset}-{access_level}/{prefix}/plot-2.png',
        ],
        # a BAM and its index: a "resource group" is just two keys
        'Align.bam': f'gs://cpg-{dataset}-{access_level}/{prefix}/sample-1.bam',
        'Align.bai': f'gs://cpg-{dataset}-{access_level}/{prefix}/sample-1.bam.bai',
    },
    time_limit_seconds=12 * 60 * 60,
)

print(result.workflow_id, result.reused)
print(result.collected['Align.bam'])  # where the BAM was copied to
print(result.outputs['Align.vcf'])  # a raw output that wasn't collected
```

## Layering

| Module       | What it does                                                    | Backend-specific? |
|--------------|-----------------------------------------------------------------|-------------------|
| `client`     | `CromwellClient` (REST calls) and `CachedTokenProvider` (auth)  | no                |
| `watch`      | `watch_workflow`: poll, time limit, finish report               | no                |
| `collect`    | `collect_outputs`: copy outputs to Destinations                 | no                |
| `deps`       | `zip_dependencies`: a reproducible dependencies zip             | no                |
| `submission` | Submission key and `run_workflow`                               | no                |
| `gcp_batch`  | `build_workflow_options` for the GCP Batch backend              | **yes**           |
| `errors`     | the exception hierarchy                                         | no                |

The core takes Workflow options as a ready-made dict, so it doesn't depend on any
backend. `gcp_batch` is the only module that knows what GCP Batch options look like.

There's no dependency on `cpg_utils.config` because callers under other workflow
managers get configuration from other sources. Values that the old wrapper read from
config, such as the dataset's project and buckets, are explicit arguments here. The
only defaults are `CROMWELL_URL` and `CROMWELL_AUDIENCE` from `cpg_utils.constants`,
which come from environment variables with hard-coded fallbacks.

## API reference

### `CromwellClient`

```python
from cpg_utils.cromwell_client import CachedTokenProvider, CromwellClient

# defaults: CROMWELL_URL, and a cached Google ID token for CROMWELL_AUDIENCE
client = CromwellClient()

# another server and audience
client = CromwellClient(
    'https://cromwell.example.org',
    token_provider=CachedTokenProvider('1234-abcd.apps.googleusercontent.com'),
    timeout_seconds=30,
)

# any zero-argument callable returning a bearer token works
client = CromwellClient(token_provider=lambda: 'my-token')

workflow_id = client.submit(
    b'version 1.0\nworkflow hello {}\n',
    workflow_filename='hello.wdl',
    input_documents=[b'{"hello.name": "world"}'],
    options={'backend': 'batch'},
)
print(client.status(workflow_id))
print(client.metadata(workflow_id, include_keys=('status', 'submission')))
print(client.outputs(workflow_id))
print(client.query_by_label('cpg-submission-key', 'abc123'))
client.abort(workflow_id)
```

Each method makes exactly one request and **never retries**. Failures map onto the
[exception hierarchy](#failures). After an HTTP 401, the client calls the token
provider's `invalidate()` method if it has one, so the next call mints a fresh token.
`CachedTokenProvider` reuses a token until five minutes before the `exp` in its JWT
payload. If it can't read `exp`, it keeps the token for 50 minutes.

### `build_workflow_options`

```python
from cpg_utils.cromwell_client import build_workflow_options

options = build_workflow_options(
    service_account_secret='my-dataset-cromwell-main-key',
    secret_project='my-dataset-project',
    google_project='my-dataset-project',
    logs_dir='gs://cpg-my-dataset-main-analysis/cromwell_logs/align/v1',
    gcs_root='gs://cpg-my-dataset-main-tmp/cromwell',
    outputs_dir=None,  # don't have Cromwell copy final outputs anywhere
    labels={'stage': 'align'},
    ar_guid=None,
)
```

This reads the service account key from Secret Manager, and Cromwell runs the
Workflow's jobs as that account. The secret's project can differ from
`google_project`. `google_labels` always gets `compute-category: cromwell`, and also
`ar-guid` when `ar_guid` is given. The result matches what `run_cromwell_workflow`
built, key for key and in the same order. A parity test enforces this.

### `zip_dependencies`

```python
from cpg_utils.cromwell_client import zip_dependencies

# the same entries as `cd wdl && zip -r deps.zip tasks/ Structs.wdl`
deps = zip_dependencies('wdl', ['tasks', 'Structs.wdl'])
```

Each lib must exist and lie inside `cwd`. The zip is byte-for-byte reproducible: it
has sorted entries, a fixed timestamp and fixed permissions. It contains regular files
only, and symlinked directories aren't followed. Being reproducible keeps the
Submission key stable from one launch to the next.

### `watch_workflow`

```python
from cpg_utils.cromwell_client import CromwellClient, watch_workflow

client = CromwellClient()

# re-attach to a Workflow submitted earlier, by anyone
outputs = watch_workflow(
    client,
    'b2bd3f54-0000-0000-0000-000000000000',
    time_limit_seconds=6 * 60 * 60,
    max_poll_interval=120,
)
```

It returns the Workflow outputs once the status is `Succeeded`. Polling starts every
`min_poll_interval` seconds and ramps up to every `max_poll_interval` seconds over
`exponential_decrease_seconds`. The ramp and the time limit both count from the
Workflow's own `submission` time, so re-attaching doesn't restart either. When the
Workflow finishes, a report built from its metadata is logged at INFO.

Transient errors are retried until `max_sequential_exception_count` of them happen in
a row. Every successful status poll resets that count.

### `collect_outputs`

```python
from cpg_utils.cromwell_client import collect_outputs

outputs = {
    'Align.bam': 'gs://cromwell-exec/Align/abc/call-Align/sample-1.bam',
    'Align.bai': 'gs://cromwell-exec/Align/abc/call-Align/sample-1.bam.bai',
    'Align.read_count': 123456,
}
collected = collect_outputs(
    outputs,
    {
        'Align.bam': 'gs://cpg-my-dataset-main/align/sample-1.bam',
        'Align.bai': 'gs://cpg-my-dataset-main/align/sample-1.bam.bai',
    },
    threads=8,
)
```

### `run_workflow`

```python
from cpg_utils.cromwell_client import CromwellClient, run_workflow

options = {'backend': 'batch', 'google_project': 'my-dataset-project'}
result = run_workflow(
    CromwellClient(),
    'wdl/Hello.wdl',
    options=options,
    inputs={'Hello.name': 'world'},
    reuse_existing=True,
    collect_threads=4,
    # anything else goes to watch_workflow
    time_limit_seconds=3600,
    abort_on_interrupt=False,
)
```

This reads the WDL and the inputs up front. It then looks up an existing Workflow by
[Submission key](#resubmission-and-submission-keys), or submits a new one, then
watches it and collects the outputs if `destinations` is given. Input documents are
sent in this order: `input_files`, then `inputs` serialised as canonical JSON.
Cromwell applies later documents over earlier ones, so `inputs` wins. An unknown
keyword argument raises `TypeError` before any request is sent.

### `WorkflowResult`

```python
from cpg_utils.cromwell_client import WorkflowResult

result = WorkflowResult(
    workflow_id='b2bd3f54-0000-0000-0000-000000000000',
    outputs={'Hello.greeting': 'gs://cromwell-exec/hello.txt'},  # raw Workflow outputs
    collected={},  # collect_outputs(...) result, or {} without destinations
    reused=False,  # True if attached to an existing Workflow via the Submission key
)
```

## Collecting

`destinations` maps a **full output name**, such as `'Align.bam'`, to a Destination.
A Destination is a `str`, a `pathlib.Path` or a `CloudPath`:

- **Single file.** One path. The output must not be an array or an object.
- **`Array[File]`.** A list of paths, the same length as the array, matched element by
  element.
- **Resource group,** such as a BAM and its index: several keys.
- **Path-only outputs:** leave them out of `destinations` and read them from
  `result.outputs`.

Collecting is **strict**. Every destination is checked before anything is copied.
These are problems:

- a missing output
- a `null` output
- an array whose length doesn't match the list of destinations
- a single destination for an array output, or a list for a single value

All of them are reported together in one `CollectError`. The old wrapper wrote the
literal string `null` instead.

A **non-file output**, such as a number or a string that isn't a `gs://` path, is
returned unchanged with a WARNING. Nothing is written for it.

If an output is **already at its destination**, for example because `outputs_dir`
points there, the copy is skipped.

Copies run in `threads` parallel threads. A gs→gs copy uses `Blob.rewrite`, so
multi-GB objects copy correctly across buckets, locations and storage classes. Every
copy is attempted even if some fail. Collecting again overwrites the destinations.

## Failures

```text
CromwellError
├── CromwellUnavailable    # retrying later is sensible
├── CromwellRequestError   # .status_code; fix the request, don't retry
│   └── CromwellNotFound   # 404: unknown Workflow ID
├── WorkflowFailed         # .workflow_id, .status, .failures
├── WorkflowTimedOut       # .workflow_id, .time_limit_seconds, .abort_confirmed
└── CollectError           # .problems
```

| Exception | Meaning | Retry? |
|---|---|---|
| `CromwellUnavailable` | Connection error, timeout, HTTP 5xx/408/429/401, a non-JSON body, or a watcher's error budget ran out | Yes. It's transient, or the server is down. |
| `CromwellRequestError` | Any other 4xx | No. The request is wrong. |
| `CromwellNotFound` | 404 | No. Check the Workflow ID and server URL. |
| `WorkflowFailed` | The Workflow ended `Failed` or `Aborted`. `failures` holds Cromwell's messages, flattened through `causedBy`. | Only after fixing the cause. A rerun with the same content submits a new Workflow. |
| `WorkflowTimedOut` | The time limit passed and an abort was requested. `abort_confirmed=False` means the abort request itself never succeeded, so check the Workflow by hand. | Only with a longer limit. |
| `CollectError` | Outputs were missing, null or the wrong shape, or copies failed. | Yes for failed copies: rerunning re-attaches to the `Succeeded` Workflow and collects again. |

`ValueError` (bad arguments, a missing secret) and `TypeError` (an unknown
`watch_workflow` argument) are raised before anything is submitted.

## Resubmission and Submission keys

If a watcher is preempted and its task is retried, `run_workflow` finds the Workflow
it already submitted and doesn't submit a duplicate.

**The key** is the first 63 hex characters of a sha256 over:

- the WDL bytes
- the dependencies zip, or none
- each input document, in the order it is sent
- the Workflow options **except** `user_service_account_json` and `google_labels`

So a rotated service account key or a new AR GUID doesn't change the key. A different
input, WDL, project, or log/output location does change it. The key is sent on every
submission as the Cromwell label `cpg-submission-key`, and found again with
`/query?label=cpg-submission-key:<key>`. Nothing is stored on the caller's side.

**Lookup.** If several Workflows match, the newest `submission` wins:

| Newest match                        | What `run_workflow` does                                                          |
|-------------------------------------|-----------------------------------------------------------------------------------|
| none                                | submit                                                                            |
| `Submitted` / `Running` / `On Hold` | attach and watch it (`reused=True`)                                               |
| `Succeeded`                         | attach; outputs come back at once and are collected again                         |
| `Aborting`                          | wait up to 15 minutes for `Aborted`, then submit; otherwise raise `CromwellError` |
| `Failed` / `Aborted`                | submit                                                                            |

If the lookup itself fails, the error is raised. It isn't treated as "none", because
that could cause exactly the duplicate the key is there to prevent.

**`reuse_existing=False`** skips the lookup and always submits. The label is still
sent, so later runs can find this Workflow.

**`abort_on_interrupt` defaults to `False`.** When a watcher is preempted, the Workflow
should keep running so that the retried watcher can re-attach to it. Set it to `True`
for interactive use, where Ctrl-C should also stop the Workflow.

**The time limit counts from the Workflow's `submission` time**, not from when this
watcher started. A watcher that re-attaches doesn't get a fresh allowance. The limit
only applies while the Workflow is unfinished: a re-attach to a Workflow that already
succeeded returns its outputs.

**Known limitation:** Cromwell has no compare-and-set. Two watchers launched at the
same moment with identical content can both find nothing and both submit.
Preemption followed by a retry happens one after the other, so that case is covered.

## Migrating from `cpg_utils.cromwell`

| Old                                                                                                               | New                                                                                                                                                                                                 |
|-------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `run_cromwell_workflow(job, dataset, access_level, workflow, cwd, libs, output_prefix, ...)`                      | `build_workflow_options(...)` + `zip_dependencies(cwd, libs)` + `client.submit(...)`, or all of it via `run_workflow(...)`                                                                          |
| `watch_workflow(...)` / `watch_workflow_and_get_output(b, job_prefix, workflow_id_file, outputs_to_collect, ...)` | `watch_workflow(client, workflow_id, ...)` + `collect_outputs(outputs, destinations)`. The polling arguments keep their names and defaults.                                                         |
| `run_cromwell_workflow_from_repo_and_get_outputs(b, ..., repo, commit, cwd, ...)`                                 | Check out the repo yourself, then call `run_workflow(client, f'{cwd}/{workflow}', options=..., dependencies=zip_dependencies(cwd, libs), destinations=...)`. There is no "commit is on main" check. |
| `CromwellOutputType.single('w.out')`                                                                              | `{'w.out': dst}`                                                                                                                                                                                    |
| `CromwellOutputType.array('w.outs', length=n)`                                                                    | `{'w.outs': [dst_1, …, dst_n]}`                                                                                                                                                                     |
| `CromwellOutputType.single_resource_group(name, {'bam': 'w.bam', 'bai': 'w.bai'})`                                | `{'w.bam': bam_dst, 'w.bai': bai_dst}`                                                                                                                                                              |
| `CromwellOutputType.array_resource_group(name, n, {'bam': 'w.bams', 'bai': 'w.bais'})`                            | `{'w.bams': [bam_1, …], 'w.bais': [bai_1, …]}`                                                                                                                                                      |
| `CromwellOutputType.single_path('w.out')`                                                                         | leave it out of `destinations`; read `result.outputs['w.out']`                                                                                                                                      |
| `CromwellOutputType.array_path('w.outs', n)`                                                                      | leave it out of `destinations`; read `result.outputs['w.outs']`                                                                                                                                     |
| `get_cromwell_oauth_token()` (shells out to `gcloud`)                                                             | `CachedTokenProvider()` (pure Python, cached)                                                                                                                                                       |
| `copy_outputs_to_gcp=True`                                                                                        | `outputs_dir=dataset_path(output_prefix, dataset=dataset)`                                                                                                                                          |
| `copy_outputs_to_gcp=False`                                                                                       | `outputs_dir=None`                                                                                                                                                                                  |
| secret `{dataset}-cromwell-{access_level}-key` read from `get_gcp_project()`                                      | `service_account_secret=f'{dataset}-cromwell-{access_level}-key'`, `secret_project=get_gcp_project()`                                                                                               |
| `project=None` → `get_gcp_project()`                                                                              | `google_project=project or get_gcp_project()`                                                                                                                                                       |
| `dataset_path(f'cromwell_logs/{output_prefix}', dataset=dataset, category='analysis')`                            | `logs_dir=...`                                                                                                                                                                                      |
| `join(dataset_path('', dataset=dataset, category='tmp'), 'cromwell')`                                             | `gcs_root=...`                                                                                                                                                                                      |
| `ar_guid_override` or `try_get_ar_guid()`                                                                         | `ar_guid=...`                                                                                                                                                                                       |
| `input_paths` then `input_dict`                                                                                   | `input_files` then `inputs`, in the same order                                                                                                                                                      |
| the watcher returning silently after a time-limit abort                                                           | `WorkflowTimedOut` is raised                                                                                                                                                                        |
| a `null` output written as the string `null`                                                                      | `CollectError`                                                                                                                                                                                      |

The old wrapper resolved the right-hand values from `cpg_utils.config`. Under
`analysis-runner`, you can still resolve them that way and pass them in. That code
lives in your pipeline, not in this package.
