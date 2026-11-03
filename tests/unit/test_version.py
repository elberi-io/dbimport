import tomllib
from pathlib import Path

from common import constants

PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"


def test_pyproject_version_matches_dbimport_version() -> None:
    project = tomllib.loads(PYPROJECT.read_text())["project"]

    assert project["version"] == constants.VERSION
