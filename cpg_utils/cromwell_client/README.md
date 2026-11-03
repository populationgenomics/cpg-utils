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
    google_labels={'stage': 'align'},
    ar_guid='0f7d2a51-0000-0000-0000-000000000000',
)

result = run_workflow(
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
print(result.outputs['Align.bam'])  # where the BAM was copied to
print(result.outputs['Align.vcf'])  # an output that wasn't collected: its raw value
```

## Layering

| Module       | What it does                                                    | Backend-specific? |
|--------------|-----------------------------------------------------------------|-------------------|
| `client`     | `CromwellClient` (REST calls) and `CachedTokenProvider` (auth)  | no                |
| `watch`      | `watch_workflow`: poll, time limit, finish report               | no                |
| `collect`    | `collect_outputs`: copy outputs to Destinations                 | no                |
| `deps`       | `zip_dependencies`: a reproducible dependencies zip             | no                |
| `submission` | Submission key and `run_workflow`                               | no                |
| `many`       | `run_workflows`: many Workflows from one process, in threads    | no                |
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
    google_labels={'stage': 'align'},
    ar_guid=None,
)
```

This reads the service account key from Secret Manager, and Cromwell runs the
Workflow's jobs as that account. The secret's project can differ from
`google_project`. The directories can be strings or paths. `google_labels` are
attached to the cloud resources the Workflow creates; they aren't Cromwell workflow
labels. They always get `compute-category: cromwell`, and also `ar-guid` when
`ar_guid` is given. The result matches what `run_cromwell_workflow`
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
from cpg_utils.cromwell_client import CromwellClient, Polling, watch_workflow

client = CromwellClient()

# re-attach to a Workflow submitted earlier, by anyone
outputs = watch_workflow(
    client,
    'b2bd3f54-0000-0000-0000-000000000000',
    time_limit_seconds=6 * 60 * 60,
    polling=Polling(max_seconds=120),
)
```

It returns the Workflow outputs once the status is `Succeeded`. When the Workflow
finishes, a report built from its metadata is logged at INFO.

`Polling` holds the settings that rarely need changing. Its defaults match
`cpg_utils.cromwell`:

| Field                    | Default | Meaning                                                    |
|--------------------------|---------|------------------------------------------------------------|
| `min_seconds`            | 5       | the first wait between polls                               |
| `max_seconds`            | 60      | the wait once the ramp is over                             |
| `ramp_seconds`           | 1200    | how long the wait takes to ramp up (a cosine curve)        |
| `max_consecutive_errors` | 25      | give up after this many errors in a row talking to Cromwell |

The ramp and the time limit both count from the Workflow's own `submission` time, so
re-attaching doesn't restart either. Every successful status poll resets the error
count.

### `collect_outputs`

```python
from cpg_utils.cromwell_client import collect_outputs

outputs = {
    'Align.bam': 'gs://cromwell-exec/Align/abc/call-Align/sample-1.bam',
    'Align.bai': 'gs://cromwell-exec/Align/abc/call-Align/sample-1.bam.bai',
    'Align.read_count': 123456,
}
located = collect_outputs(
    outputs,
    {
        'Align.bam': 'gs://cpg-my-dataset-main/align/sample-1.bam',
        'Align.bai': 'gs://cpg-my-dataset-main/align/sample-1.bam.bai',
    },
    threads=8,
)
# {'Align.bam': 'gs://cpg-my-dataset-main/align/sample-1.bam',
#  'Align.bai': 'gs://cpg-my-dataset-main/align/sample-1.bam.bai',
#  'Align.read_count': 123456}
```

It returns every output where it now lives: the destination for each collected
output, and the raw value for the rest.

### `run_workflow`

```python
from datetime import timedelta

from cpg_utils.cromwell_client import CromwellClient, Polling, run_workflow

options = {'backend': 'batch', 'google_project': 'my-dataset-project'}
result = run_workflow(
    'wdl/Hello.wdl',
    options=options,
    inputs={'Hello.name': 'world'},
    time_limit_seconds=3600,  # passed to watch_workflow
    abort_on_interrupt=False,  # passed to watch_workflow
    reuse_existing=True,
    reuse_succeeded_within=timedelta(days=2),  # default: one week
    polling=Polling(max_seconds=120),
    collect_threads=4,
    client=CromwellClient('https://cromwell.example.org'),  # default: CromwellClient()
)
```

`watch_workflow` and `run_workflow` also take `stop`, a `threading.Event`. Setting it
from another thread ends any wait at once with `Stopped`, and nothing new is submitted.
With `abort_on_interrupt=True`, the Workflow being watched is aborted on the way out.
`run_workflows` uses this to stop its threads.

This reads the WDL and the inputs up front. It then looks up an existing Workflow by
[Submission key](#resubmission-and-submission-keys), or submits a new one, then
watches it and collects the outputs if `destinations` is given. Input documents are
sent in this order: `input_files`, then `inputs` serialised as canonical JSON.
Cromwell applies later documents over earlier ones, so `inputs` wins.

### `run_workflows`

Runs many Workflows from one process, for example one per sample. Each Workflow goes
through `run_workflow` in its own thread, so it gets the same lookup, watching, time
limit and collecting. The threads mostly sleep between polls, so one process can watch
dozens of Workflows.

```python
import logging
import signal
import sys

from cpg_utils.cromwell_client import (
    WorkflowRequest,
    WorkflowResult,
    build_workflow_options,
    run_workflows,
    zip_dependencies,
)

logger = logging.getLogger(__name__)

options = build_workflow_options(
    service_account_secret='my-dataset-cromwell-main-key',
    secret_project='my-dataset-project',
    google_project='my-dataset-project',
    logs_dir='gs://cpg-my-dataset-main-analysis/cromwell_logs/align/v1',
    gcs_root='gs://cpg-my-dataset-main-tmp/cromwell',
)
deps = zip_dependencies('wdl', ['tasks'])
prefix = 'gs://cpg-my-dataset-main/align/v1'


def register(request: WorkflowRequest, result: WorkflowResult) -> None:
    # runs in that Workflow's thread as soon as its outputs are collected
    logger.info(f'{request.name}: {result.outputs["Align.cram"]}')


# make SIGTERM interrupt the main thread, as Ctrl-C does
signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))

outcomes = run_workflows(
    [
        WorkflowRequest(
            name=sample,
            workflow='wdl/Align.wdl',
            options=options,
            inputs={'Align.sample_id': sample},
            dependencies=deps,
            destinations={'Align.cram': f'{prefix}/{sample}.cram'},
            time_limit_seconds=12 * 60 * 60,
        )
        for sample in ['sample-1', 'sample-2', 'sample-3']
    ],
    max_in_flight=50,  # default: all of them at once
    submit_interval_seconds=1.0,  # space out lookups and submissions
    on_result=register,
    abort_on_interrupt=True,
)
for outcome in outcomes:  # in request order
    if not outcome.ok:
        print(outcome.name, outcome.error)
```

- **`WorkflowRequest`** holds `run_workflow`'s per-Workflow arguments (`workflow`,
  `options`, `inputs`, `input_files`, `dependencies`, `destinations`,
  `time_limit_seconds`, `polling`) plus a `name`. Names must be unique. The arguments
  that apply to every request (`reuse_existing`, `reuse_succeeded_within`,
  `collect_threads`, `client`) are arguments of `run_workflows` itself.
- **Checked first.** Every request is read and its Submission key computed before
  anything is sent. Repeated names, two requests with the same Submission key (both
  would find nothing and submit), too many input documents and unreadable files all
  raise before any request.
- **Independent.** A Workflow that fails, or whose `on_result` raises, never stops the
  others. Each `WorkflowOutcome` has `name`, `result` (once the Workflow succeeded and
  was collected) and `error`. Both are set when only `on_result` raised. `ok` is true
  when `error` is `None`.
- **`on_result`** runs in that Workflow's thread, so it must be thread-safe.
- **Interrupts.** If the calling thread is interrupted while it waits, by
  `KeyboardInterrupt` or by anything raised from your own signal handler, every thread
  is told to stop (see `stop` above). Waiting threads stop at once, nothing new is
  submitted, and queued requests never start. With `abort_on_interrupt=True`, each
  thread aborts the Workflow it was watching. `run_workflows` waits for the threads,
  then re-raises the interrupt, so no outcomes are returned. A thread that is
  collecting finishes its copies first. `run_workflows` installs no signal handlers:
  Python turns SIGINT into `KeyboardInterrupt`, but SIGTERM needs a handler that
  raises, as in the example.

### `WorkflowResult`

```python
from cpg_utils.cromwell_client import WorkflowResult

result = WorkflowResult(
    workflow_id='b2bd3f54-0000-0000-0000-000000000000',
    # every output where it now lives: collected outputs point at their copies
    outputs={'Hello.greeting': 'gs://cpg-my-dataset-main/hello.txt', 'Hello.n': 3},
    reused=False,  # True if attached to an existing Workflow via the Submission key
)
```

`outputs` isn't exactly what Cromwell reported: collected outputs point at their
destinations. For the original locations, call `client.outputs(result.workflow_id)`.

## Collecting

`destinations` maps a **full output name**, such as `'Align.bam'`, to a Destination.
A Destination is a `str`, a `pathlib.Path` or a `CloudPath`:

- **Single file.** One path. The output must not be an array or an object.
- **`Array[File]`.** A list of paths, the same length as the array, matched element by
  element.
- **Resource group,** such as a BAM and its index: several keys.
- **Path-only outputs and non-file outputs:** leave them out of `destinations`. They
  keep their raw values in `result.outputs`.

Collecting is **strict**. Every destination is checked before anything is copied.
These are problems:

- a missing output
- a `null` output
- an array whose length doesn't match the list of destinations
- a single destination for an array output, or a list for a single value
- a non-file output, such as a number or a string that isn't a `gs://` path

All of them are reported together in one `CollectError`. The old wrapper wrote the
literal string `null` instead.

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
├── Stopped                # the caller set the stop event
└── CollectError           # .problems
```

| Exception | Meaning | Retry? |
|---|---|---|
| `CromwellUnavailable` | Connection error, timeout, HTTP 5xx/408/429/401, a non-JSON body, or a watcher's error budget ran out | Yes. It's transient, or the server is down. |
| `CromwellRequestError` | Any other 4xx | No. The request is wrong. |
| `CromwellNotFound` | 404 | No. Check the Workflow ID and server URL. |
| `WorkflowFailed` | The Workflow ended `Failed` or `Aborted`. `failures` holds Cromwell's messages, flattened through `causedBy`. | Only after fixing the cause. A rerun with the same content submits a new Workflow. |
| `WorkflowTimedOut` | The time limit passed and an abort was requested. `abort_confirmed=False` means the abort request itself never succeeded, so check the Workflow by hand. | Only with a longer limit. |
| `Stopped` | The caller set `stop` (`run_workflows` does this when interrupted). The Workflow keeps running unless it was aborted on the way out. | Yes. A rerun re-attaches. |
| `CollectError` | Outputs were missing, null, not files or the wrong shape, or copies failed. | Yes for failed copies: rerunning re-attaches to the `Succeeded` Workflow and collects again. |

`ValueError` (bad arguments, a missing secret) is raised before anything is
submitted.

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
| `Succeeded`, finished within a week | attach; outputs come back at once and are collected again                         |
| `Succeeded`, finished longer ago    | submit                                                                            |
| `Aborting`                          | wait up to 15 minutes for `Aborted`, then submit; otherwise raise `CromwellError` |
| `Failed` / `Aborted`                | submit                                                                            |

If the lookup itself fails, the error is raised. It isn't treated as "none", because
that could cause exactly the duplicate the key is there to prevent.

**`reuse_succeeded_within`** (default one week, `None` for no limit) bounds how old a
`Succeeded` Workflow can be and still be reused. Its age counts from its `end` time.
There are two reasons for the limit. The key hashes the inputs document, not the
files it points to, so a file rewritten in place doesn't change the key. And the
Workflow's outputs may since have been deleted by a lifecycle rule on the bucket.
Active Workflows are always reused, whatever their age.

**`reuse_existing=False`** skips the lookup and always submits. The label is still
sent, so later runs can find this Workflow. Use it to force a fresh run, for
example when a reused `Succeeded` Workflow's outputs can no longer be collected.

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
| `watch_workflow(...)` / `watch_workflow_and_get_output(b, job_prefix, workflow_id_file, outputs_to_collect, ...)` | `watch_workflow(client, workflow_id, ...)` + `collect_outputs(outputs, destinations)`. The polling arguments move to `Polling`; see the next row.                                                   |
| `min_poll_interval`, `max_poll_interval`, `exponential_decrease_seconds`, `max_sequential_exception_count`        | `polling=Polling(min_seconds=..., max_seconds=..., ramp_seconds=..., max_consecutive_errors=...)`, with the same defaults                                                                           |
| `run_cromwell_workflow_from_repo_and_get_outputs(b, ..., repo, commit, cwd, ...)`                                 | Check out the repo yourself, then call `run_workflow(f'{cwd}/{workflow}', options=..., dependencies=zip_dependencies(cwd, libs), destinations=...)`. There is no "commit is on main" check.         |
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
| `labels={...}` (became `google_labels`)                                                                           | `google_labels={...}`                                                                                                                                                                               |
| `input_paths` then `input_dict`                                                                                   | `input_files` then `inputs`, in the same order                                                                                                                                                      |
| the watcher returning silently after a time-limit abort                                                           | `WorkflowTimedOut` is raised                                                                                                                                                                        |
| a `null` output written as the string `null`                                                                      | `CollectError`                                                                                                                                                                                      |

The old wrapper resolved the right-hand values from `cpg_utils.config`. Under
`analysis-runner`, you can still resolve them that way and pass them in. That code
lives in your pipeline, not in this package.
