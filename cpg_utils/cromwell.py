# ruff: noqa: DTZ005
"""
Cromwell module contains helper code for submitting + watching
jobs from within Hail batch.
"""

import json
import os
import subprocess
from enum import Enum
from shlex import quote
from typing import Any

from hailtop.batch import Batch, Resource
from hailtop.batch.job import BashJob, Job

from cpg_utils.cloud import read_secret
from cpg_utils.config import (
    AR_GUID_NAME,
    dataset_path,
    get_access_level,
    get_driver_image,
    get_gcp_project,
    try_get_ar_guid,
)
from cpg_utils.constants import (
    CROMWELL_AUDIENCE,
    CROMWELL_URL,
    GCLOUD_ACTIVATE_AUTH,
)
from cpg_utils.git import (
    get_git_commit_ref_of_current_repository,
    get_git_default_remote,
    get_repo_name_from_remote,
)
from cpg_utils.hail_batch import prepare_git_job, query_command


class CromwellBackend(Enum):
    # the string value here, is the key of the backend in our cromwell.conf
    batch = 'batch'
    pipelines_api = 'papi'


DEFAULT_BACKEND = CromwellBackend.batch


class CromwellOutputType:
    """Declares output type for cromwell -> hail batch glue"""

    def __init__(
        self,
        name: str,
        copy_file_into_batch: bool,
        array_length: int | None,
        resource_group: dict[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.copy_file_into_batch = copy_file_into_batch
        self.array_length = array_length
        self.resource_group = resource_group

    @staticmethod
    def single(name: str) -> 'CromwellOutputType':
        """Single file"""
        return CromwellOutputType(
            name=name,
            array_length=None,
            copy_file_into_batch=True,
        )

    @staticmethod
    def single_resource_group(
        name: str,
        resource_group: dict[str, Any],
    ) -> 'CromwellOutputType':
        """
        Specify a resource group you want to return, where resource_group has the format:
            {<read-group-name>: <corresponding-output-in-cromwell>}
        Eg:
        outputs_to_collect={
            "<this-key-only-exists-in-output-dict>": CromwellOutputType.single_resource_group({
                # The hello workflow has two outputs: output_bam, output_bam_index
                'bam': 'hello.output_bam',
                'bai': 'hello.output_bam_index'
            })
        }
        """
        return CromwellOutputType(
            name=name,
            array_length=None,
            copy_file_into_batch=True,
            resource_group=resource_group,
        )

    @staticmethod
    def array(name: str, length: int) -> 'CromwellOutputType':
        """Array of simple files"""
        return CromwellOutputType(
            name=name,
            array_length=length,
            copy_file_into_batch=True,
        )

    @staticmethod
    def array_resource_group(
        name: str,
        length: int,
        resource_group: dict[str, Any],
    ) -> 'CromwellOutputType':
        """
        Select an array of resource groups. In this case, the outputs
        you select within the resource group are zipped.
        Resource_group has the format:
            {<read-group-name>: <corresponding-output-in-cromwell>}
        Eg:
        outputs_to_collect={
            "<this-key-only-exists-in-output-dict>": CromwellOutputType.array_resource_group({
                'bam': 'hello.output_bams',
                'bai': 'hello.output_bam_indexes'
            }, length=2)
        }

        # You get
        # {"<this-key-only-exists-in-output-dict>":  [__resource_group1, __resource_group2]}
        """
        return CromwellOutputType(
            name=name,
            array_length=length,
            copy_file_into_batch=True,
            resource_group=resource_group,
        )

    @staticmethod
    def single_path(name: str) -> 'CromwellOutputType':
        """Return the file path of the output in a file"""
        return CromwellOutputType(
            name=name,
            array_length=None,
            copy_file_into_batch=False,
        )

    @staticmethod
    def array_path(name: str, length: int) -> 'CromwellOutputType':
        """Return a list of file paths of the outputs (one path per file)"""
        return CromwellOutputType(
            name=name,
            array_length=length,
            copy_file_into_batch=False,
        )


def run_cromwell_workflow(  # noqa: C901
    job: BashJob,
    dataset: str,
    access_level: str,
    workflow: str,
    cwd: str | None,
    libs: list[str],
    output_prefix: str,
    labels: dict[str, str] | None = None,
    input_dict: dict[str, Any] | None = None,
    input_paths: list[str] | None = None,
    project: str | None = None,
    copy_outputs_to_gcp: bool = True,
    ar_guid_override: str | None = None,
    backend: CromwellBackend = DEFAULT_BACKEND,
):
    """
    Run a cromwell workflow, and return a Batch.ResourceFile
    that contains the workflow ID
    """

    # embed this here so people can't call it
    def get_cromwell_key(dataset: str, access_level: str) -> str:
        """Get Cromwell key from secrets"""

        secret_name = f'{dataset}-cromwell-{access_level}-key'
        value = read_secret(get_gcp_project(), secret_name)
        if not value:
            raise ValueError(f"Couldn't find secret: {secret_name}")
        return value

    if cwd:
        job.command(f'cd {quote(cwd)}')

    deps_path = None
    if libs:
        deps_path = 'tools.zip'
        job.command('zip -r tools.zip ' + ' '.join(quote(s + '/') for s in libs))

    cromwell_post_url = CROMWELL_URL + '/api/workflows/v1'

    google_labels = {}

    if labels:
        google_labels.update(labels)

    google_labels.update({'compute-category': 'cromwell'})

    if ar_guid_override:
        google_labels[AR_GUID_NAME] = ar_guid_override
    else:
        ar_guid = try_get_ar_guid()
        if ar_guid:
            google_labels[AR_GUID_NAME] = ar_guid

    service_account_json = get_cromwell_key(dataset=dataset, access_level=access_level)
    # use the email specified by the service_account_json again
    service_account_dict = json.loads(service_account_json)
    service_account_email = service_account_dict.get('client_email')
    _project = project
    if _project is None:
        _project = get_gcp_project()

    if not service_account_email:
        raise ValueError("The service_account didn't contain an entry for client_email")

    # test/main should be implicit from the config
    intermediate_dir = os.path.join(
        dataset_path('', dataset=dataset, category='tmp'),
        'cromwell',
    )
    workflow_output_dir = dataset_path(output_prefix, dataset=dataset)
    logging_output_dir = dataset_path(
        f'cromwell_logs/{output_prefix}',
        dataset=dataset,
        category='analysis',
    )

    workflow_options = {
        # michael configured these manually in config to match the enum values
        'backend': backend.value,
        # pass the user-service-account-json to cromwell to submit jobs as this user
        'user_service_account_json': service_account_json,
        'google_compute_service_account': service_account_email,
        'google_project': _project,
        # other config options that are useful
        'google_labels': google_labels,
        'final_call_logs_dir': logging_output_dir,
        'final_workflow_log_dir': logging_output_dir,
    }

    if backend == CromwellBackend.pipelines_api:
        workflow_options['jes_gcs_root'] = intermediate_dir

    if backend == CromwellBackend.batch:
        # cromwell: /supportedBackends/google/batch/src/main/scala/cromwell/backend/google/batch/models/GcpBatchWorkflowPaths.scala#L20
        # this was undocumented at the time of writing
        workflow_options['gcp_batch_gcs_root'] = intermediate_dir

    # if required, export the workflow outputs to GCS
    if copy_outputs_to_gcp:
        workflow_options['final_workflow_outputs_dir'] = workflow_output_dir

    input_paths = input_paths or []
    if input_dict:
        tmp_input_json_path = '$TMPDIR/inputs.json'
        job.command(f"echo '{json.dumps(input_dict)}' > {tmp_input_json_path}")
        input_paths.append(tmp_input_json_path)

    inputs_cli = []
    for idx, value in enumerate(input_paths):
        key = 'workflowInputs'
        if idx > 0:
            key += f'_{idx + 1}'

        inputs_cli.append(f'-F "{key}=@{value}"')

    output_workflow_id = job.out_workflow_id
    job.command(
        f"""
    set +x
    echo '{json.dumps(workflow_options)}' > workflow-options.json
    access_token=$(gcloud auth print-identity-token --audiences={CROMWELL_AUDIENCE})
    wid=$(curl -X POST "{cromwell_post_url}" \\
        -H "Authorization: Bearer $access_token" \\
        -H "accept: application/json" \\
        -H "Content-Type: multipart/form-data" \\
        -F "workflowSource=@{workflow}" \\
        {' '.join(inputs_cli)} \\
        -F "workflowOptions=@workflow-options.json;type=application/json" \\
        {f'-F "workflowDependencies=@{deps_path}"' if deps_path else ''})

    echo "Submitted workflow with ID $wid"
    echo $wid | jq -r .id >> {output_workflow_id}
    """,
    )

    return output_workflow_id


def run_cromwell_workflow_from_repo_and_get_outputs(
    b: Batch,
    job_prefix: str,
    dataset: str,
    workflow: str,
    outputs_to_collect: dict[str, CromwellOutputType],
    libs: list[str],
    output_prefix: str,
    labels: dict[str, str] | None = None,
    input_dict: dict[str, Any] | None = None,
    input_paths: list[str] | None = None,
    repo: str | None = None,
    commit: str | None = None,
    cwd: str | None = None,
    driver_image: str | None = None,
    project: str | None = None,
    copy_outputs_to_gcp: bool = True,
    min_watch_poll_interval: int = 5,
    max_watch_poll_interval: int = 60,
    time_limit_seconds: int | None = None,
    backend: CromwellBackend = DEFAULT_BACKEND,
) -> tuple[Job, dict[str, Resource | list[Resource]]]:
    """
    This function needs to know the structure of the outputs you
    want to collect. It currently only supports:
        - a single value, or
        - a list of values

    Eg: outputs_to_collect={
        'hello.out': None, # single output
        'hello.outs': 5, # array output of length=5
    }

    If the starts with "gs://", we'll copy it as a resource file,
    otherwise write the value into a file which will be a batch resource.

    If copy_outputs_to_gcp is True, the outputs will be copied to GCS.
    Workflows may then choose to copy these outputs to a final destination.

    Returns a submit Job object, and a dict of output Resource objects.

    Optionally override min/max poll interval for the watch job.
    This alters how often the Watch job pings Cromwell for Status updates

    time_limit_seconds is optional, and will cause the workflow to be aborted if
    the time limit is exceeded
    """

    _driver_image = driver_image or get_driver_image()
    access_level = get_access_level()

    submit_job = b.new_job(f'{job_prefix}_submit')
    submit_job.image(_driver_image)
    prepare_git_job(
        job=submit_job,
        repo_name=(repo or get_repo_name_from_remote(get_git_default_remote())),
        commit=(commit or get_git_commit_ref_of_current_repository()),
        is_test=access_level == 'test',
    )

    workflow_id_file = run_cromwell_workflow(
        job=submit_job,
        dataset=dataset,
        access_level=access_level,
        workflow=workflow,
        cwd=cwd,
        libs=libs,
        output_prefix=output_prefix,
        input_dict=input_dict,
        input_paths=input_paths,
        labels=labels,
        project=project,
        copy_outputs_to_gcp=copy_outputs_to_gcp,
        backend=backend,
    )

    outputs_dict = watch_workflow_and_get_output(
        b,
        job_prefix=job_prefix,
        workflow_id_file=workflow_id_file,
        outputs_to_collect=outputs_to_collect,
        driver_image=_driver_image,
        max_poll_interval=max_watch_poll_interval,
        min_poll_interval=min_watch_poll_interval,
        time_limit_seconds=time_limit_seconds,
    )

    return submit_job, outputs_dict


def watch_workflow(  # noqa: C901
    workflow_id_file: str,
    max_sequential_exception_count: int,
    min_poll_interval: int,
    max_poll_interval: int,
    exponential_decrease_seconds: int,
    output_json_path: str,
    time_limit_seconds: int | None = None,
):
    """
    INNER Python function to watch workflow status, and write
    output paths to output_json_path on success.

    Re-importing dependencies here so the function is self-contained
    and can be run in a Hail bash job.

    Args:
        workflow_id_file (str): file containing the Cromwell WF ID only
        max_sequential_exception_count (int): Fail after X consecutive errors
        min_poll_interval (int): minimum polling wait
        max_poll_interval (int): maximum polling wait
        exponential_decrease_seconds (int): expo curve for interval generation
        output_json_path (str): where to write output results file
        time_limit_seconds (int): kill if not completed before X seconds pass
    """

    import json
    import logging
    import math
    import subprocess
    import time
    from datetime import datetime

    import requests
    from cloudpathlib.anypath import to_anypath

    from cpg_utils.constants import (
        CROMWELL_AUDIENCE,
        CROMWELL_URL,
        GCLOUD_ACTIVATE_AUTH_BASE,
    )
    from cpg_utils.cromwell_model import WorkflowMetadataModel

    # Create a logger, ensure info statements are actually printed
    logger = logging.getLogger('cromwell_watcher')
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    class CromwellError(Exception):
        """Cromwell status error"""

    # Also re-defining this function that uses subprocess, for the same reason.
    def _get_cromwell_oauth_token() -> str:
        """Get oath token for cromwell, specific to audience"""
        token_command = [
            'gcloud',
            'auth',
            'print-identity-token',
            f'--audiences={CROMWELL_AUDIENCE}',
        ]
        return subprocess.check_output(token_command).decode().strip()  # noqa: S603

    def _get_wait_interval(
        start: datetime,
        min_poll_interval: int = 5,
        max_poll_interval: int = 60,
        exponential_decrease_seconds: int = 1200,
    ) -> int:
        """
        Get wait time between {min_poll_interval} and {max_poll_interval} seconds
        curved between 0s and {exponential_decrease_seconds}.
        """
        factor = (datetime.now() - start).total_seconds() / exponential_decrease_seconds
        if factor > 1:
            return max_poll_interval
        return max(
            min_poll_interval,
            int((1 - math.cos(math.pi * factor)) * max_poll_interval // 2),
        )

    with open(workflow_id_file, encoding='utf-8') as f:
        workflow_id = f.read().strip()
    logger.info(f'Received workflow ID: {workflow_id}')

    failed_statuses = {'failed', 'aborted'}
    terminal_statuses = {'succeeded'} | failed_statuses
    status_reported = False
    subprocess.check_output(GCLOUD_ACTIVATE_AUTH_BASE)  # noqa: S603
    cromwell_workflow_root = f'{CROMWELL_URL}/api/workflows/v1/{workflow_id}'
    abort_url = f'{cromwell_workflow_root}/abort'
    metadata_url = f'{cromwell_workflow_root}/metadata'
    outputs_url = f'{cromwell_workflow_root}/outputs'
    status_url = f'{cromwell_workflow_root}/status'
    _remaining_exceptions = max_sequential_exception_count
    start = datetime.now()

    while True:
        # kill the workflow if the maximum wait has elapsed
        if (
            time_limit_seconds
            and (datetime.now() - start).total_seconds() >= time_limit_seconds
        ):
            # time to die, Mr. Cromwell
            logger.info(
                f'This job has exceeded the max permitted runtime ({time_limit_seconds}s), and will be aborted',
            )
            auth_header = {'Authorization': f'Bearer {_get_cromwell_oauth_token()}'}
            r = requests.post(abort_url, headers=auth_header, timeout=10)
            if not r.ok:
                logger.info(
                    f"Abort failed, error code {r.status_code}, WF ID: {workflow_id}",
                )
                _remaining_exceptions -= 1
                continue
            # quit if we successfully aborted
            logger.info(f'Successfully aborted workflow {workflow_id}')
            return

        if _remaining_exceptions <= 0:
            raise CromwellError('Unreachable')

        wait_time = _get_wait_interval(
            start,
            min_poll_interval,
            max_poll_interval,
            exponential_decrease_seconds,
        )

        try:
            auth_header = {'Authorization': f'Bearer {_get_cromwell_oauth_token()}'}
            r = requests.get(status_url, headers=auth_header, timeout=60)
            if not r.ok:
                _remaining_exceptions -= 1
                logger.warning(
                    f'Received "not okay" (status={r.status_code}) from cromwell '
                    f'(waiting={wait_time}): {r.text}',
                )
                time.sleep(wait_time)
                continue
            status = r.json().get('status')

            # if workflow has concluded print logging to hail batch log
            if status.lower() in terminal_statuses and not status_reported:
                logger.info('Cromwell workflow has concluded - fetching log')
                # don't report multiple times if we fail fetching output
                # also don't fail the whole run if we can't fetch metadata
                status_reported = True
                response = requests.get(metadata_url, headers=auth_header, timeout=60)
                if response.ok:
                    meta_json = response.json()
                    print(WorkflowMetadataModel.parse(meta_json).display())
                else:
                    print('Failed to collect run Metadata')

            if status.lower() == 'succeeded':
                logger.info('Cromwell workflow moved to succeeded state')
                _remaining_exceptions = max_sequential_exception_count
                # process outputs here
                r_outputs = requests.get(outputs_url, headers=auth_header, timeout=60)
                if not r_outputs.ok:
                    logger.warning(
                        'Received error when fetching cromwell outputs, '
                        f'will retry in {wait_time} seconds',
                    )
                    time.sleep(wait_time)
                    continue
                outputs = r_outputs.json()
                logger.info(f'Received outputs from Cromwell: {outputs}')
                with to_anypath(output_json_path).open('w') as fh:
                    json.dump(outputs.get('outputs'), fh)
                break
            if status.lower() in failed_statuses:
                logger.error(f'Got failed cromwell status: {status}')
                raise CromwellError(status)
            logger.info(f'Got cromwell status: {status} (sleeping={wait_time})')
            time.sleep(wait_time)
        except CromwellError:
            # pass through
            raise
        except Exception as e:  # noqa: BLE001
            _remaining_exceptions -= 1
            logger.error(
                f'Cromwell status watch caught general exception (sleeping={wait_time}): {e}',
            )
            time.sleep(wait_time)


def watch_workflow_and_get_output(
    b: Batch,
    job_prefix: str,
    workflow_id_file: Resource,
    outputs_to_collect: dict[str, CromwellOutputType],
    driver_image: str | None = None,
    min_poll_interval: int = 5,  # 5 seconds
    max_poll_interval: int = 60,  # 1 minute
    exponential_decrease_seconds: int = 1200,  # 20 minutes
    max_sequential_exception_count: int = 25,
    time_limit_seconds: int | None = None,
):
    """
    This is a little bit tricky, but the process is:

    - Wait for a cromwell workflow to finish,
    - If it succeeds, get the outputs (as a json)
    - (Hard) Get the value of the output back into Hail Batch as a resource file.

    Getting the value of the output back into hail batch because the:
        - outputs to collect +
        - number of outputs to collect must be known up-front.

    So unfortunately, this function needs to know the structure of the outputs you
    want to collect. It currently only supports:
        - a single value, or
        - a list of values

    If the starts with "gs://", we'll copy it as a resource file,
    otherwise write the value into a file which will be a batch resource.

    :param driver_image: If specified, must contain python3 (w/ requests), gcloud, jq
    :param b: Batch object
    :param job_prefix: Prefix for the job name
    :param workflow_id_file: File containing the workflow ID
    :param outputs_to_collect: dict of output name -> CromwellOutputType
    :param min_poll_interval: Min time to wait between polls
    :param max_poll_interval: Maximum time to wait between polls
    :param exponential_decrease_seconds: Exponential decrease in wait time
    :param max_sequential_exception_count: Maximum number of exceptions before giving up
    :param time_limit_seconds: a maximum runtime before abort is triggered
    """

    driver_image = driver_image or get_driver_image()

    watch_job = b.new_job(job_prefix + '_watch')
    watch_job.cpu(0.25)

    watch_job.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')
    watch_job.env('PYTHONUNBUFFERED', '1')  # makes the logs go quicker
    watch_job.image(driver_image)  # need an image with python3 + requests

    watch_job.command(
        query_command(
            watch_workflow,
            watch_workflow.__name__,
            str(workflow_id_file),
            max_sequential_exception_count,
            min_poll_interval,
            max_poll_interval,
            exponential_decrease_seconds,
            str(watch_job.output_json_path),
            time_limit_seconds,
            setup_gcp=True,
            setup_hail=False,
        ),
    )

    rdict = watch_job.output_json_path

    out_file_map = {}
    for oname, output in outputs_to_collect.items():
        output_name = output.name
        array_length = output.array_length
        if array_length is None:
            # is single
            j = b.new_bash_job(f'{job_prefix}_collect_{output_name}')
            j.image(driver_image)
            if output.resource_group:
                # is single resource group
                out_file_map[oname] = _copy_resource_group_into_batch(
                    j=j,
                    rdict=rdict,
                    output=output,
                    idx=None,
                )
            else:
                # is single file / value
                out_file_map[oname] = _copy_basic_file_into_batch(
                    j=j,
                    rdict=rdict,
                    output_name=output_name,
                    idx=None,
                    copy_file_into_batch=output.copy_file_into_batch,
                )
        else:
            # is array
            outs: list[Resource] = []
            for idx in range(array_length):
                j = b.new_bash_job(f'{job_prefix}_collect_{output_name}[{idx}]')
                j.image(driver_image)
                if output.resource_group:
                    # is array output group
                    outs.append(
                        _copy_resource_group_into_batch(
                            j=j,
                            rdict=rdict,
                            output=output,
                            idx=idx,
                        ),
                    )
                else:
                    outs.append(
                        _copy_basic_file_into_batch(
                            j=j,
                            rdict=rdict,
                            output_name=output_name,
                            idx=idx,
                            copy_file_into_batch=output.copy_file_into_batch,
                        ),
                    )

            out_file_map[oname] = outs

    return out_file_map


def _copy_basic_file_into_batch(
    j: BashJob,
    *,
    rdict: Resource,
    output_name: str,
    idx: int | None,
    copy_file_into_batch: bool,
) -> Resource:
    """
    1. Take the file-pointer to the dictionary `rdict`,
    2. the output name `output`,
    3. check that the value we select is a string,
    4. either:
        (a) gcloud storage cp it into `output_filename`
        (b) write the value into `output_filename`
    """
    output_filename = j.out

    if idx is None:
        # if no index, select the value as-is
        error_description = output_name
        # wrap this in quotes, because output often contains a '.', which has to be escaped in jq
        jq_el = f'"{output_name}"'
    else:
        # if we're supplied an index, grab the value, then get the index, eg: '.hello[5]'
        error_description = f'{output_name}[{idx}]'
        # wrap this in quotes, because output often contains a '.', which has to be escaped in jq
        jq_el = f'"{output_name}"[{idx}]'

    # activate to gcloud storage cp
    j.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')
    j.command(GCLOUD_ACTIVATE_AUTH)

    # this has to be in bash unfortunately :(
    # we want to check that the output we get is a string
    # if it starts with gs://, then we'll `gcloud storage cp` it into output_filename
    # otherwise write the value into output_filename.

    # in future, add s3://* or AWS handling here

    j.command(
        f"""
OUTPUT_TYPE=$(cat {rdict} | jq '.{jq_el}' | jq -r type)
if [ $OUTPUT_TYPE != "string" ]; then
    echo "The element {error_description} was not of type string, got $OUTPUT_TYPE";
    # exit 1;
fi
""",
    )
    if copy_file_into_batch:
        j.command(
            f"""
OUTPUT_VALUE=$(cat {rdict} | jq -r '.{jq_el}')
if [[ "$OUTPUT_VALUE" == gs://* ]]; then
    echo "Copying file from $OUTPUT_VALUE";
    gcloud storage cp $OUTPUT_VALUE {output_filename};
else
    # cleaner to directly pipe into file
    cat {rdict} | jq -r '.{jq_el}' > {output_filename}
fi
    """,
        )
    else:
        # directly pipe result into a file
        j.command(f"cat {rdict} | jq -r '.{jq_el}' > {output_filename}")

    return output_filename


def _copy_resource_group_into_batch(
    j: BashJob,
    *,
    rdict: Resource,
    output: CromwellOutputType,
    idx: int | None,
) -> Resource:
    """
    For a job, construct a set of commands that copy a resource group into the batch
    """
    rg = output.resource_group

    if not rg:
        raise ValueError('Resource group must be specified')

    j.declare_resource_group(
        out={part_name: f'{{root}}.{part_name}' for part_name in rg},
    )

    output_filename = j.out

    if idx is None:
        # if no index, select the value as-is
        error_descriptions = list(rg.keys())

        # wrap this in quotes, because output often contains a '.', which has to be escaped in jq
        jq_els = [f'"{output_source}"' for output_source in rg.values()]
    else:
        # if we're supplied an index, grab the value, then get the index, eg: '.hello[5]'
        error_descriptions = [f'{output_name}[{idx}]' for output_name in rg]
        # wrap this in quotes, because output often contains a '.', which has to be escaped in jq
        jq_els = [f'"{output_source}"[{idx}]' for output_source in rg.values()]

    # activate to use a gcloud cp
    j.env('GOOGLE_APPLICATION_CREDENTIALS', '/gsa-key/key.json')
    j.command(GCLOUD_ACTIVATE_AUTH)

    # this has to be in bash unfortunately :(
    # we want to check that the output we get is a string
    # if it starts with gs://, then we'll `gcloud storage cp` it into output_filename
    # otherwise write the value into output_filename.

    # in future, add s3://* or AWS handling here

    for jq_el, error_description, output_name in zip(
        jq_els,
        error_descriptions,
        rg.keys(),
    ):
        j.command(
            f"""
        OUTPUT_TYPE=$(cat {rdict} | jq '.{jq_el}' | jq -r type)
        if [ $OUTPUT_TYPE != "string" ]; then
            echo "The element {error_description} was not of type string, got $OUTPUT_TYPE";
            # exit 1;
        fi

        OUTPUT_VALUE=$(cat {rdict} | jq -r '.{jq_el}')
        if [[ "$OUTPUT_VALUE" == gs://* ]]; then
            echo "Copying file from $OUTPUT_VALUE";
            gcloud storage cp $OUTPUT_VALUE {output_filename}.{output_name};
        else
            # cleaner to directly pipe into file
            cat {rdict} | jq -r '.{jq_el}' > {output_filename}.{output_name};
        fi
            """,
        )

    return output_filename


def get_cromwell_oauth_token():
    """Get oath token for cromwell, specific to audience"""
    token_command = [
        'gcloud',
        'auth',
        'print-identity-token',
        f'--audiences={CROMWELL_AUDIENCE}',
    ]
    return subprocess.check_output(token_command).decode().strip()  # noqa: S603
