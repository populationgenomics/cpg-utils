# cpg-utils

This is a Python library containing convenience functions that are specific to the CPG.

In order to install the library in a conda environment, run:

```bash
conda install -c cpg cpg-utils
```

To use the library, import functions like this:

```python
from cpg_utils.cloud import is_google_group_member
```

We use `bumpversion` for incrementing the library's semantic version. A new conda package gets published automatically in the `cpg` conda channel whenever a version bump commit is merged with the `main` branch.
