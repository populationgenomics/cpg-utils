"""
Helper functions for working with Git repositories
"""

import os
import re
import subprocess

from cpg_utils.constants import DEFAULT_GITHUB_ORGANISATION


def get_output_of_command(command: list[str], description: str) -> str:
    """
    subprocess.check_output wrapper that returns string output and raises detailed
    exceptions on error.

    Parameters
    ----------
    command:     list of strings creating a full command
    description: meaningful message for error logging

    Returns
    -------
    stripped output string if command was successful

    """
    try:
        return subprocess.check_output(command).decode().strip()  # noqa: S603
    # Handle and rethrow KeyboardInterrupt error to stop global exception catch

    except KeyboardInterrupt:
        raise
    except subprocess.CalledProcessError as e:
        raise OSError(
            f"Couldn't call {description} by calling '{' '.join(command)}', {e}",
        ) from e
    except Exception as e:  # noqa: BLE001
        raise type(e)(
            f"Couldn't process {description} through calling '{' '.join(command)}', {e}",
        ) from e


def get_relative_script_path_from_git_root(script_name: str) -> str:
    """
    If we're in a subdirectory, get the relative path from the git root
    to the current directory, and append the script path.
    For example, the relative path to this script (from git root) is:
        cpg-utils/git.py

    Parameters
    ----------
    script_name

    Returns
    -------
    fully qualified script path from repo root
    """
    base = get_relative_path_from_git_root()
    return os.path.join(base, script_name)


def get_relative_path_from_git_root() -> str:
    """
    If we're in a subdirectory, get the relative path from the git root
    to the current directory. Relpath returns "." if cwd is a git root.
    """
    root = get_git_repo_root()
    return os.path.relpath(os.getcwd(), root)


def get_git_default_remote() -> str:
    """
    Returns the git remote of 'origin',
    e.g. https://github.com/populationgenomics/cpg-utils

    Returns
    -------

    """
    command = ['git', 'remote', 'get-url', 'origin']
    return get_output_of_command(command, 'get Git remote of origin')


def get_git_repo_root() -> str:
    """
    Returns the git repository directory root,
    e.g. /Users/foo/repos/cpg-utils

    Returns
    -------

    """
    command = ['git', 'rev-parse', '--show-toplevel']
    return get_output_of_command(command, 'get Git repo directory')


def get_git_commit_ref_of_current_repository() -> str:
    """
    Returns the commit SHA at the current HEAD

    Returns
    -------

    """
    command = ['git', 'rev-parse', 'HEAD']
    return get_output_of_command(command, 'get latest Git commit')


def get_git_branch_name() -> str | None:
    """Returns the current branch name."""
    command = ['git', 'rev-parse', '--abbrev-ref', 'HEAD']
    try:
        value = subprocess.check_output(command).decode().strip()  # noqa: S603
        if value:
            return value
    except Exception:  # noqa: BLE001
        return None

    return None


def get_repo_name_from_current_directory() -> str:
    """
    Gets the repo name from the default remote

    Returns
    -------

    """
    return get_repo_name_from_remote(get_git_default_remote())


def get_organisation_name_from_current_directory() -> str:
    """
    Gets the organisation name from the default remote
    Returns
    -------

    """
    return get_organisation_name_from_remote(get_git_default_remote())


def get_organisation_name_from_remote(remote_name: str) -> str:
    """
    Takes the GitHub repo path and obtains the source organisation
    based on its remote URL e.g.:
    >>> get_organisation_name_from_remote(\
        'git@github.com:populationgenomics/cpg-utils.git'\
    )
    'populationgenomics'

    >>> get_organisation_name_from_remote(\
        'https://github.com/populationgenomics/cpg-utils.git'\
    )
    'populationgenomics'

    Parameters
    ----------
    remote_name

    Returns
    -------
    the organisation name
    """

    organisation = None

    try:
        if remote_name.startswith('http'):
            match = re.match(
                r'http[s]?:\/\/[A-Za-z0-9._-]+?\/(?P<org>.+?)\/.+$',
                remote_name,
            )
            if match:
                organisation = match.group('org')

        elif remote_name.startswith('git@'):
            match = re.match(r'git@[A-Aa-z0-9.+-]+?:(?P<org>.+?)\/.+$', remote_name)
            if match:
                organisation = match.group('org')
    except AttributeError as ae:
        raise ValueError(f'Unsupported remote format: "{remote_name}"') from ae

    if organisation is None:
        raise ValueError(f'Unsupported remote format: "{remote_name}"')

    return organisation


def get_repo_name_from_remote(remote_name: str) -> str:
    """
    Get the name of a GitHub repo from a supported organisation
    based on its remote URL e.g.:
    >>> get_repo_name_from_remote(\
        'git@github.com:populationgenomics/cpg-utils.git'\
    )
    'cpg-utils'
    >>> get_repo_name_from_remote(\
        'https://github.com/populationgenomics/cpg-utils.git'\
    )
    'cpg-utils'

    removed check for the authorised organisation(s) here - we receive
    a full repository path, so we trust users will not attempt to run
    potentially harmful code
    """

    repo = None
    try:
        if remote_name.startswith('http'):
            match = re.match(
                r'http[s]?:\/\/[A-Za-z0-9_.-]+?\/.+?\/(?P<repo>.+)$',
                remote_name,
            )
            if match:
                repo = match.group('repo')

        elif remote_name.startswith('git@'):
            match = re.match(r'git@[A-Za-z0-9_.-]+?:.+?/(?P<repo>.+)$', remote_name)
            if match:
                repo = match.group('repo')
    except AttributeError as ae:
        raise ValueError(f'Unsupported remote format: "{remote_name}"') from ae

    if repo is None:
        raise ValueError(f'Unsupported remote format: "{remote_name}"')

    if repo.endswith('.git'):
        repo = repo[:-4]

    return repo


def check_if_commit_is_on_remote(commit: str) -> bool:
    """
    Returns 'True' if the commit is available on a remote branch.
    This relies on the current environment to be up-to-date.
    It asks if the local environment knows a remote branch with the commit.

    Parameters
    ----------
    commit

    Returns
    -------

    """
    command = ['git', 'branch', '-r', '--contains', commit]
    try:
        ret = subprocess.check_output(command)  # noqa: S603
        return bool(ret)
    except subprocess.CalledProcessError:
        return False


def guess_script_name_from_script_argument(script: list[str]) -> str | None:
    """
    Guess the script name from the first argument of the script.
    If the first argument is an executable, try the second param

    >>> guess_script_name_from_script_argument(['python', 'main.py'])
    'main.py'

    >>> guess_script_name_from_script_argument(['./main.sh'])
    'main.sh'

    >>> guess_script_name_from_script_argument(['main.sh'])
    'main.sh'

    >>> guess_script_name_from_script_argument(['./test/path/main.sh', 'arg1', 'arg2'])
    'test/path/main.sh'

    >>> guess_script_name_from_script_argument(['gcloud', 'cp' 'test']) is None
    True

    """
    executables = {'python', 'python3', 'bash', 'sh', 'rscript'}
    _script = script[0]
    if _script.lower() in executables:
        _script = script[1]

    if _script.startswith('./'):
        return _script[2:]

    # a very bad check if it follows format "file.ext"
    if '.' in _script:
        return _script

    return None


def guess_script_github_url_from(
    *,
    repo: str | None,
    commit: str | None,
    cwd: str | None,
    script: list[str],
    organisation: str = DEFAULT_GITHUB_ORGANISATION,
) -> str | None:
    """
    Guess the GitHub URL of the script from the given arguments.
    """
    guessed_script_name = guess_script_name_from_script_argument(script)
    if not guessed_script_name:
        return None

    url = f'https://github.com/{organisation}/{repo}/tree/{commit}'

    if cwd == '.' or cwd is None:
        return f'{url}/{guessed_script_name}'

    return os.path.join(url, cwd, guessed_script_name)
