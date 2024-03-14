import json
import unittest
from unittest.mock import MagicMock, patch

from cpg_utils.cromwell import run_cromwell_workflow


class TestCromwellWrapper(unittest.TestCase):

    @patch('cpg_utils.config.get_config')
    @patch('cpg_utils.cromwell.read_secret')
    def test_workflow_options(
        self,
        mock_read_secret: MagicMock,
        mock_get_config: MagicMock,
    ):
        """
        Test that the workflow options are correctly generatede by cromwell

        Args:
            mock_read_secret (MagicMock): _description_
            mock_get_config (MagicMock): _description_
        """

        dataset = 'test-dataset'
        dataset_buckets = {
            'default': 'test://default-bucket',
            'analysis': 'test://analysis-bucket',
            'tmp': 'test://tmp-bucket',
        }

        mock_get_config.return_value = {
            'workflow': {
                'ar-guid': '<test-ar-guid>',
                'access_level': 'test',
                'dataset_gcp_project': 'test-gcp-project',
            },
            'storage': {
                'default': dataset_buckets,
                dataset: dataset_buckets,
            },
        }

        mock_read_secret.return_value = '{"client_email": "<secret-client-email>"}'

        job: MagicMock = MagicMock()

        _ = run_cromwell_workflow(
            job=job,
            dataset=dataset,
            access_level='test',
            workflow='test-workflow',
            cwd=None,
            libs=[],
            output_prefix='output-prefix',
        )

        mock_read_secret.assert_called_once_with(
            'test-gcp-project',
            f'{dataset}-cromwell-test-key',
        )

        # no inputs, hence there will only be one call to job.command

        args, _ = job.command.call_args
        command: str = args[0].strip()

        # echo '{json.dumps(workflow_options)}' > workflow-options.json
        wf_options_line = command.splitlines()[1]
        wf_options_str = wf_options_line.split("'")[1]
        wf_options = json.loads(wf_options_str)

        self.assertEqual(
            wf_options['jes_gcs_root'],
            'test://tmp-bucket/cromwell',
        )
        self.assertEqual(
            wf_options['final_call_logs_dir'],
            'test://analysis-bucket/cromwell_logs/output-prefix',
        )
        self.assertEqual(
            wf_options['final_workflow_log_dir'],
            'test://analysis-bucket/cromwell_logs/output-prefix',
        )
        self.assertEqual(
            wf_options['final_workflow_outputs_dir'],
            'test://default-bucket/output-prefix',
        )
