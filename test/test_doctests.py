import doctest
import pkgutil
import unittest

import cpg_utils


def load_tests(
    loader: unittest.TestLoader,  # noqa: ARG001
    tests: unittest.TestSuite,
    ignore: str | None,  # noqa: ARG001
) -> unittest.TestSuite:

    # Load all doctests from the cpg_utils package
    for module in pkgutil.iter_modules(cpg_utils.__path__):
        tests.addTests(doctest.DocTestSuite('cpg_utils.' + module.name))

    return tests


if __name__ == "__main__":
    unittest.main()
