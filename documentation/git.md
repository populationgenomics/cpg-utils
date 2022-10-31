# Git Methods

For some purposes we need to run scripts from a repository inside the
container of a hail batch job. To do this we have to clone the repo
containing the code inside the blank container. This example shows how
the methods present in [git.py](../cpg_utils/git.py) can be used to:

- identify the name of the current repository
- identify the organisation/username of the current repository
- identify the current commit
- clone the repository at that point

## Checking out a git repository at the current commit

```python
import hailtop.batch as hb
from cpg_utils.git import (
  get_git_commit_ref_of_current_repository,
  get_organisation_name_from_current_directory,
  get_repo_name_from_current_directory,
  prepare_git_job,
)

batch = hb.Batch('do-some-analysis')
job = batch.new_job('checkout_repo')

# then pull the current git repository inside the container
# at the exact same commit we are currently on
prepare_git_job(
  job=job,
  # you can specify the organisation/user here, e.g. 'populationgenomics'
  organisation=get_organisation_name_from_current_directory(),
  # you could specify a name here, like 'analysis-runner'
  repo_name=get_repo_name_from_current_directory(),
  # you could specify the specific commit here, eg: '1be7bb44de6182d834d9bbac6036b841f459a11a'
  commit=get_git_commit_ref_of_current_repository(),
)

# Now, the working directory of j is the checkout out repository
job.command('examples/bash/hello.sh')
```
