"""
test case for the `hail/default_timeout` setting with the local Hail Batch backend
"""

import tempfile
from pathlib import Path
from unittest import TestCase

from cpg_utils.config import set_config_paths
from cpg_utils.hail_batch import get_batch, reset_batch

CONFIG = """
[workflow]
access_level = 'test'
dataset = 'fewgenomes'
name = 'test timeout'
driver_image = 'ubuntu:24.04'

[hail]
backend = 'local'
billing_project = 'fewgenomes'
default_timeout = 3600

[storage.default]
tmp = 'gs://cpg-fewgenomes-test-tmp'
"""


class TestDefaultTimeoutOnLocalBackend(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.addCleanup(set_config_paths, [])
        self.addCleanup(reset_batch)
        reset_batch()

        path = Path(self._tmp.name) / 'config.toml'
        path.write_text(CONFIG)
        set_config_paths([str(path)])

    def test_jobs_can_be_created(self):
        """
        Hail Batch only supports job timeouts on the service backend, so a default
        timeout must not be applied to jobs on the local backend.
        """
        with self.assertLogs(level='WARNING') as logs:
            job = get_batch().new_job('no timeout on the local backend')

        self.assertIsNone(job._timeout)
        self.assertIn('hail/default_timeout is ignored', '\n'.join(logs.output))
