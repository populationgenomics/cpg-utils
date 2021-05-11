# cpg-utils

This is a Python library containing convenience functions that are specific to the CPG.

On every merge with the `main` branch, a new version gets published in the `cpg` conda
channel. In order to install the library in a conda environment, run:

```bash
conda install -c cpg cpg-utils
```

To use the library, import functions like this:

```python
from cpg_utils.cloud import is_google_group_member
```
