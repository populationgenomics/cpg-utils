import pytest

from cpg_utils.hail_batch import output_path, dataset_path, ConfigError


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
    tests for dataset_path

    prod_conf : test fixture containing default and -test paths
    """

    assert (
        dataset_path('myfile.txt', 'analysis')
        == 'gs://cpg-mito-disease-main-analysis/myfile.txt'
    )
    assert (
        dataset_path('myfile.txt')
        == 'gs://cpg-mito-disease-main/myfile.txt'
    )
    assert (
        dataset_path('myfile.txt', 'web')
        == 'gs://cpg-mito-disease-main-web/myfile.txt'
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
    tests for dataset_path

    test_conf : test fixture containing default and -test paths
    """

    assert (
        dataset_path('myfile.txt', 'analysis')
        == 'gs://cpg-mito-disease-test-analysis/myfile.txt'
    )
    assert (
        dataset_path('myfile.txt')
        == 'gs://cpg-mito-disease-test/myfile.txt'
    )
    assert (
        dataset_path('myfile.txt', 'web')
        == 'gs://cpg-mito-disease-test-web/myfile.txt'
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

