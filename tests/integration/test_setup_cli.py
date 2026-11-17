import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa

SETUP = Path(__file__).resolve().parents[2] / "bin" / "setup"
# A revision of the migration chain before the baseline (0.80.008).
OLD_REVISION = "af71422a72a2"
VERSION_QUERY = sa.text("SELECT version_num FROM alembic_version")


def _setup(option: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SETUP), option],
        capture_output=True,
        text=True,
        timeout=120,
        env=os.environ.copy(),
        check=False,
    )


@pytest.fixture
def old_revision(clean_config_db: None, db_connection: sa.Connection) -> Iterator[None]:
    """The database stamped with OLD_REVISION; the head is restored after
    the test, because the schema is built once per session."""
    head = db_connection.execute(VERSION_QUERY).scalar_one()
    update = sa.text("UPDATE alembic_version SET version_num = :revision")
    db_connection.execute(update, {"revision": OLD_REVISION})
    try:
        yield
    finally:
        db_connection.execute(update, {"revision": head})


def test_setup_downgrade_db_fails_because_the_method_is_missing(
    clean_config_db: None,
) -> None:
    # Known bug: bin/setup calls initialize.downgradeDB(), which does not
    # exist. This records the current behaviour. It is not an expected
    # failure on purpose: a fixed command would downgrade the test schema.
    result = _setup("--downgradeDB")

    assert result.returncode == 1
    assert "AttributeError" in result.stderr
    assert "downgradeDB" in result.stderr


def test_setup_upgrade_db_at_a_revision_before_the_baseline_stops(
    old_revision: None, db_connection: sa.Connection
) -> None:
    result = _setup("--upgradeDB")

    assert result.returncode == 1
    assert "upgraded with DBImport 1.00.006" in result.stdout + result.stderr
    assert db_connection.execute(VERSION_QUERY).scalar_one() == OLD_REVISION
