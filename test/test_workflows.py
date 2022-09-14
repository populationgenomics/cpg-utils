"""
Test Hail Query functions.
"""

import hail as hl
import toml

from cpg_utils import to_path
from cpg_utils.config import set_config_paths
from cpg_utils.workflows.batch import get_batch
from cpg_utils.workflows.inputs import get_cohort
from cpg_utils.workflows.targets import Sample, Cohort
from cpg_utils.workflows.utils import timestamp
from cpg_utils.workflows.workflow import (
    Workflow,
    SampleStage,
    StageInput,
    StageOutput,
    ExpectedResultT,
    CohortStage,
    stage,
)
from cpg_utils.hail_batch import dataset_path, command

tmp_dir_path = to_path(__file__).parent / 'results' / timestamp()
tmp_dir_path = tmp_dir_path.absolute()
tmp_dir_path.mkdir(parents=True, exist_ok=True)

config_toml = f"""
[workflow]
dataset_gcp_project = 'fewgenomes'
access_level = 'test'
dataset = 'fewgenomes'
check_inputs = false
check_intermediates = false
check_expected_outputs = false
path_scheme = 'local'
local_dir = '{str(tmp_dir_path)}'

[hail]
billing_project = 'fewgenomes'
delete_scratch_on_exit = false
backend = 'local'
"""


def _set_config():
    config_path = tmp_dir_path / 'config.toml'
    with config_path.open('w') as f:
        toml.dump(toml.loads(config_toml), f)
    set_config_paths([str(config_path)])


_set_config()


def test_batch_job():
    """
    Test creating a job and running a batch.
    """
    b = get_batch('Test batch job')
    j1 = b.new_job('Jo b1')
    text = 'success'
    cmd = f"""\
    echo {text} > {j1.output}
    """
    j1.command(command(cmd))
    output1_path = dataset_path('output1.txt')
    b.write_output(j1.output, str(output1_path))

    j2 = b.new_job('Job 2')
    j2.command(f'touch {j2.output}')
    j2.command(f'cat {b.read_input(output1_path)} >> {j2.output}')
    j2.depends_on(j1)
    output2_path = dataset_path('output2.txt')
    b.write_output(j2.output, str(output2_path))

    b.run()
    with to_path(output2_path).open() as fh:
        assert fh.read().strip() == text


def test_batch_python_job():
    """
    Testing calling a python job.
    """
    b = get_batch('Test batch python job')
    j = b.new_python_job('Test python job')

    input_tsv_path = to_path(dataset_path('input.tsv'))
    input_tsv_path.parent.mkdir(parents=True, exist_ok=True)
    with input_tsv_path.open('w') as f:
        f.write('col1\tcol2\n1\t2')

    def query_fn(tsv_path: str, out_ht_path: str):
        ht = hl.import_table(tsv_path, types={'col1': hl.tint, 'col2': hl.tint})
        ht.show()
        ht = ht.annotate(col3=ht.col1 + ht.col2)
        ht.write(out_ht_path, overwrite=True)

    output_ht_path = dataset_path('output.ht')
    j.call(query_fn, str(input_tsv_path), output_ht_path)
    b.run()

    hl.init_local(log=dataset_path('hail-log.txt'))
    result = hl.read_table(str(output_ht_path)).col3.collect()[0]
    assert result == 3, result


def test_cohort(mocker):
    """
    Testing creating a Cohort object from metamist mocks.
    """

    def mock_get_samples(  # pylint: disable=unused-argument
        *args, **kwargs
    ) -> list[dict]:
        return [
            {'id': 'CPG01', 'external_id': 'SAMPLE1'},
            {'id': 'CPG02', 'external_id': 'SAMPLE2'},
        ]

    def mock_get_sequences_by_sample_ids(  # pylint: disable=unused-argument
        *args, **kwargs
    ) -> list[dict]:
        return [
            {
                'id': 0,
                'sample_id': 'CPG01',
                'type': 'genome',
                'status': 'completed',
                'meta': {'reads': {'location': 'mock'}, 'read_type': 'bam'},
            },
            {
                'id': 1,
                'sample_id': 'CPG02',
                'type': 'genome',
                'status': 'completed',
                'meta': {'reads': {'location': 'mock'}, 'read_type': 'bam'},
            },
        ]

    def mock_get_external_participant_id_to_internal_sample_id(  # pylint: disable=unused-argument
        *args, **kwargs
    ) -> list[list]:
        return [['CPG01', 'PART1'], ['CPG02', 'PART2']]

    def mock_get_families(*args, **kwargs):  # pylint: disable=unused-argument
        return []

    def mock_get_pedigree(*args, **kwargs):  # pylint: disable=unused-argument
        return []

    def mock_query_analyses(*args, **kwargs):  # pylint: disable=unused-argument
        return []

    mocker.patch(
        'sample_metadata.apis.SampleApi.get_samples',
        mock_get_samples,
    )
    mocker.patch(
        'sample_metadata.apis.SequenceApi.get_sequences_by_sample_ids',
        mock_get_sequences_by_sample_ids,
    )
    mocker.patch(
        'sample_metadata.apis.ParticipantApi.get_external_participant_id_to_internal_sample_id',
        mock_get_external_participant_id_to_internal_sample_id,
    )
    mocker.patch(
        'sample_metadata.apis.FamilyApi.get_families',
        mock_get_families,
    )
    mocker.patch(
        'sample_metadata.apis.FamilyApi.get_pedigree',
        mock_get_pedigree,
    )
    mocker.patch(
        'sample_metadata.apis.AnalysisApi.query_analyses',
        mock_query_analyses,
    )

    cohort = get_cohort()
    assert cohort.get_samples()[0].id == 'CPG01'


def test_workflow(mocker):
    """
    Testing running a workflow from a mock cohort.
    """

    def mock_create_cohort() -> Cohort:
        c = Cohort()
        ds = c.create_dataset('my_dataset')
        ds.add_sample('CPG01', external_id='SAMPLE1')
        ds.add_sample('CPG02', external_id='SAMPLE2')
        return c

    mocker.patch('cpg_utils.workflows.inputs.create_cohort', mock_create_cohort)

    output_path = to_path(dataset_path('cohort.tsv'))

    assert len(get_cohort().get_samples()) == 2

    @stage
    class MySampleStage(SampleStage):
        """
        Just a sample-level stage.
        """

        def expected_outputs(self, sample: Sample) -> ExpectedResultT:
            return dataset_path(f'{sample.id}.tsv')

        def queue_jobs(self, sample: Sample, inputs: StageInput) -> StageOutput | None:
            j = self.b.new_job('Sample job', self.get_job_attrs(sample))
            j.command(f'echo {sample.id}_done >> {j.output}')
            self.b.write_output(j.output, str(self.expected_outputs(sample)))
            print(f'Writing to {self.expected_outputs(sample)}')
            return self.make_outputs(sample, self.expected_outputs(sample))

    @stage(required_stages=MySampleStage)
    class MyCohortStage(CohortStage):
        """
        Just a cohort-level stage.
        """

        def expected_outputs(self, cohort: Cohort) -> ExpectedResultT:
            return output_path

        def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
            path_by_sample = inputs.as_path_by_target(MySampleStage)
            assert len(path_by_sample) == len(cohort.get_samples())
            j = self.b.new_job('Cohort job', self.get_job_attrs(cohort))
            j.command(f'touch {j.output}')
            for _, sample_result_path in path_by_sample.items():
                input_file = self.b.read_input(str(sample_result_path))
                j.command(f'cat {input_file} >> {j.output}')
            self.b.write_output(j.output, str(self.expected_outputs(cohort)))
            print(f'Writing to {self.expected_outputs(cohort)}')
            return self.make_outputs(cohort, self.expected_outputs(cohort))

    workflow = Workflow(stages=[MyCohortStage])
    workflow.run()

    print(f'Checking result in {output_path}:')
    with output_path.open() as f:
        result = f.read()
        print(result)
        assert result.split() == ['CPG01_done', 'CPG02_done'], result
