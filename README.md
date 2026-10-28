# cpg-utils

This is a Python library containing convenience functions that are specific to the CPG.

To use the library, import functions like this:

```python
from cpg_utils.cloud import email_from_id_token

_email_string = email_from_id_token(id_token_jwt='TOKEN_STRING')
```

We use `bump-my-version` for incrementing the library's semantic version. A new PyPi package gets published whenever a version bump commit is merged with the `main` branch.


## Contents

- [Methods to facilitate cloud computing](documentation/cloud.md)
- [Helper functions for Hail Batch jobs](documentation/hail_batch.md)
- [Cloning git repositories inside Hail Batch jobs](documentation/git.md)
