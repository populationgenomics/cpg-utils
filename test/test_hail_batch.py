"""
test cases for cpg-utils.hail_batch
"""

import pytest

from cpg_utils.hail_batch import (
    dataset_path,
    get_batch,
    output_path,
    reset_batch,
    ConfigError,
)


# pylint: disable=unused-argument


def test_batch_creation(test_conf):
    """
    create a local hail batch, check that it works as expected
    Parameters
    ----------
    test_conf : config fixture, includes local backend
    """
    # pylint: disable=W0212
    reset_batch()
    batch = get_batch()
    assert batch._backend.__class__.__name__ == 'LocalBackend'
    job1 = batch.new_bash_job(name='test_job1')
    job1.command('echo "I am a test"')
    assert len(batch._jobs) == 1
    batch.run(wait=False)


def test_reset_batch(test_conf):
    """
    test that the reset_batch function works as expected
    Parameters
    ----------
    test_conf :
    """
    # pylint: disable=W0212
    reset_batch()
    batch = get_batch('first')
    reset_batch()
    batch_2 = get_batch('second')
    assert id(batch) != id(batch_2)
    assert batch._backend.__class__.__name__ == 'LocalBackend'
    assert batch_2._backend.__class__.__name__ == 'LocalBackend'
    job1 = batch.new_bash_job(name='test_job1')
    job2 = batch_2.new_bash_job(name='test_job2')
    job1.command('echo "I am a test"')
    job2.command('echo "I am a second test"')
    assert len(batch._jobs) == 1
    assert batch._jobs[0].__dict__ == job1.__dict__
    assert len(batch_2._jobs) == 1
    assert batch_2._jobs[0].__dict__ == job2.__dict__
    batch.run(wait=False)
    batch_2.run(wait=False)


def test_output_path(test_conf):
    """
    test_conf : test TOML configuration
    """
    assert (
        output_path('myfile.txt')
        == 'gs://cpg-mito-disease-test/this_is_a_test/myfile.txt'
    )
    assert (
        output_path('myfile.txt', 'web')
        == 'gs://cpg-mito-disease-test-web/this_is_a_test/myfile.txt'
    )
    assert (
        output_path('myfile.txt', 'analysis')
        == 'gs://cpg-mito-disease-test-analysis/this_is_a_test/myfile.txt'
    )
    assert (
        output_path('myfile.txt')
        == 'gs://cpg-mito-disease-test/this_is_a_test/myfile.txt'
    )
    assert (
        output_path('myfile.txt', 'web')
        == 'gs://cpg-mito-disease-test-web/this_is_a_test/myfile.txt'
    )
    assert (
        output_path('myfile.txt', 'web', test=True)
        == 'gs://cpg-mito-disease-test-web/this_is_a_test/myfile.txt'
    )


def test_output_path_prod(prod_conf):
    """
    prod_conf : prod TOML configuration
    """

    assert (
        output_path('myfile.txt', 'analysis')
        == 'gs://cpg-mito-disease-main-analysis/this_is_a_test/myfile.txt'
    )
    assert (
        output_path('myfile.txt')
        == 'gs://cpg-mito-disease-main/this_is_a_test/myfile.txt'
    )
    assert (
        output_path('myfile.txt', 'web')
        == 'gs://cpg-mito-disease-main-web/this_is_a_test/myfile.txt'
    )
    assert (
        output_path('myfile.txt', 'web', test=True)
        == 'gs://cpg-mito-disease-test-web/this_is_a_test/myfile.txt'
    )


def test_dataset_path_prod(prod_conf):
    """
    prod_conf : test fixture containing default and -test paths
    """

    assert (
        dataset_path('myfile.txt', 'analysis')
        == 'gs://cpg-mito-disease-main-analysis/myfile.txt'
    )
    assert dataset_path('myfile.txt') == 'gs://cpg-mito-disease-main/myfile.txt'
    assert (
        dataset_path('myfile.txt', 'web') == 'gs://cpg-mito-disease-main-web/myfile.txt'
    )
    assert (
        dataset_path('myfile.txt', 'web', test=True)
        == 'gs://cpg-mito-disease-test-web/myfile.txt'
    )
    assert (
        dataset_path('myfile.txt', 'web', dataset='mito-disease', test=True)
        == 'gs://cpg-mito-disease-test-web/myfile.txt'
    )
    with pytest.raises(ConfigError):
        dataset_path('myfile.txt', dataset='not-mito-disease')


def test_dataset_path_test(test_conf):
    """
    test_conf : test fixture containing default and -test paths
    """

    assert (
        dataset_path('myfile.txt', 'analysis')
        == 'gs://cpg-mito-disease-test-analysis/myfile.txt'
    )
    assert dataset_path('myfile.txt') == 'gs://cpg-mito-disease-test/myfile.txt'
    assert (
        dataset_path('myfile.txt', 'web') == 'gs://cpg-mito-disease-test-web/myfile.txt'
    )
    assert (
        dataset_path('myfile.txt', 'web', test=True)
        == 'gs://cpg-mito-disease-test-web/myfile.txt'
    )
    assert (
        dataset_path('myfile.txt', 'web', dataset='mito-disease', test=True)
        == 'gs://cpg-mito-disease-test-web/myfile.txt'
    )
    with pytest.raises(ConfigError):
        dataset_path('myfile.txt', dataset='not-mito-disease')
