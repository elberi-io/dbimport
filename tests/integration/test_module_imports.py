"""Every DBImport module can be imported.

Guards import changes (explicit imports, removed and sorted imports): a
missing name used at import time, or an import cycle, fails here. An
integration test because Server.restServer connects to the configuration
database when it is imported.
"""

import importlib
import pkgutil
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
    f"{package}.{module.name}"
    for package in PACKAGES
    for module in pkgutil.iter_modules([str(BIN_DIR / package)])
]


@pytest.mark.parametrize("name", MODULES)
def test_module_imports(clean_config_db: None, name: str) -> None:
    importlib.import_module(name)
