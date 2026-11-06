"""Every DBImport module and launcher can be imported on its own.

Each one is imported in a new Python process, so a name that is missing
at import time, or an import that only works after another module was
imported first, fails here. The launchers are loaded under a name other
than "__main__", so only their imports and definitions run. An
integration test because Server.restServer connects to the configuration
database when it is imported.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

BIN_DIR = Path(__file__).resolve().parents[2] / "bin"
# Alembic loads the migration scripts itself; they are not importable.
NOT_IMPORTED = {"SchemaUpgrade"}
PACKAGES = sorted(
    d.name
    for d in BIN_DIR.iterdir()
    if d.is_dir() and d.name not in NOT_IMPORTED and any(d.glob("*.py"))
)
MODULES = [
    f"{package}.{path.stem}"
    for package in PACKAGES
    for path in sorted((BIN_DIR / package).glob("*.py"))
    if path.stem != "__init__"
]
LAUNCHERS = sorted(
    p.name
    for p in BIN_DIR.iterdir()
    if p.is_file()
    and not p.suffix
    and p.read_text().startswith("#!/usr/bin/env python")
)

IMPORT_MODULE = "import importlib, sys; importlib.import_module(sys.argv[1])"
LOAD_LAUNCHER = (
    "import importlib.machinery, importlib.util, sys;"
    "loader = importlib.machinery.SourceFileLoader('launcher', sys.argv[1]);"
    "module = importlib.util.module_from_spec("
    "importlib.util.spec_from_loader('launcher', loader));"
    "loader.exec_module(module)"
)


def _run(code: str, arg: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PYTHONPATH": str(BIN_DIR)}
    return subprocess.run(
        [sys.executable, "-c", code, arg],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )


@pytest.mark.parametrize("name", MODULES)
def test_module_import_in_new_process_succeeds(
    clean_config_db: None, name: str
) -> None:
    result = _run(IMPORT_MODULE, name)

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("name", LAUNCHERS)
def test_launcher_load_in_new_process_succeeds(
    clean_config_db: None, name: str
) -> None:
    result = _run(LOAD_LAUNCHER, str(BIN_DIR / name))

    assert result.returncode == 0, result.stderr
