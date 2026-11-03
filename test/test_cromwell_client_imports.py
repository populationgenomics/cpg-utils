"""
Import isolation for cpg_utils.cromwell_client (spec convention C1).

The package must import cleanly in a process where hail, hailtop, cpg_utils.config,
cpg_utils.hail_batch and cpg_utils.cromwell are all unimportable.
"""

import subprocess
import sys
import textwrap

BLOCKED = (
    'hail',
    'hailtop',
    'cpg_utils.config',
    'cpg_utils.hail_batch',
    'cpg_utils.cromwell',
)

_SCRIPT = textwrap.dedent(
    """
    import importlib
    import pkgutil
    import sys

    BLOCKED = {blocked!r}

    class _Blocker:
        def find_spec(self, name, path=None, target=None):
            for blocked in BLOCKED:
                if name == blocked or name.startswith(blocked + '.'):
                    raise ImportError(f'blocked import: {{name}}')
            return None

    sys.meta_path.insert(0, _Blocker())

    import cpg_utils.cromwell_client as pkg

    names = [m.name for m in pkgutil.iter_modules(pkg.__path__)]
    assert names, 'no submodules found'
    for name in names:
        importlib.import_module(f'cpg_utils.cromwell_client.{{name}}')

    leaked = [m for m in sys.modules if any(
        m == b or m.startswith(b + '.') for b in BLOCKED
    )]
    assert not leaked, leaked
    print('imported:', ', '.join(sorted(names)))
    """,
)


def _run(script: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, '-c', script],  # noqa: S603
        capture_output=True,
        text=True,
        check=False,
    )


def test_cromwell_client_imports_without_hail_or_config():
    result = _run(_SCRIPT.format(blocked=BLOCKED))
    assert result.returncode == 0, result.stderr


def test_blocker_catches_a_forbidden_import():
    """Guard against a blocker that silently lets everything through."""
    script = _SCRIPT.format(blocked=BLOCKED) + '\nimport cpg_utils.config\n'
    result = _run(script)
    assert result.returncode != 0
    assert 'blocked import: cpg_utils.config' in result.stderr
