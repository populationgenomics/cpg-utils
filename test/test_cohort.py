"""
Test workflows.cohort
"""

import toml
from pytest_mock import MockFixture

from cpg_utils import to_path, Path
from cpg_utils.config import set_config_paths, update_dict
from cpg_utils.workflows.inputs import get_cohort
from cpg_utils.workflows.utils import timestamp

tmp_dir_path = to_path(__file__).parent / 'results' / timestamp()
tmp_dir_path = tmp_dir_path.absolute()
tmp_dir_path.mkdir(parents=True, exist_ok=True)

DEFAULT_CONF = f"""
[workflow]
dataset_gcp_project = 'fewgenomes'
access_level = 'test'
dataset = 'fewgenomes'
sequencing_type = 'genome'

check_inputs = false
check_intermediates = false
check_expected_outputs = false
path_scheme = 'local'

[hail]
billing_project = 'fewgenomes'
delete_scratch_on_exit = false
backend = 'local'
"""


def _set_config(dir_path: Path, extra_conf: dict | None = None):
    d = toml.loads(DEFAULT_CONF)
    d['workflow']['local_dir'] = str(dir_path)
    if extra_conf:
        update_dict(d, extra_conf)
    config_path = dir_path / 'config.toml'
    with config_path.open('w') as f:
        toml.dump(d, f)
    set_config_paths([str(config_path)])


def test_cohort(mocker: MockFixture):
    """
    Testing creating a Cohort object from metamist mocks.
    """
    _set_config(tmp_dir_path)

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
