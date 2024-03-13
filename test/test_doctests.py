import doctest
import pkgutil
import unittest

import cpg_utils


def load_tests(
    loader: unittest.TestLoader,
    tests: unittest.TestSuite,
    ignore: str | None,
) -> unittest.TestSuite:

    for module in pkgutil.iter_modules(cpg_utils.__path__):
        tests.addTests(doctest.DocTestSuite('cpg_utils.' + module.name))
    # tests.addTests(doctest.DocTestSuite(cpg_utils.config))
    # tests.addTests(doctest.DocTestSuite(cpg_utils.cromwell_model))

    return tests


if __name__ == "__main__":
    unittest.main()
