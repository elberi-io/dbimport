from pathlib import Path
from typing import Any

import sqlalchemy as sa
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory

from dbimport.config_db import config_schema

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_UPGRADE_DIR = REPO_ROOT / "src" / "dbimport" / "migrations"
KNOWN_DRIFT_FILE = REPO_ROOT / "tests" / "fixtures" / "orm_schema_drift.txt"


def _describe(diff: Any) -> list[str]:
    """One stable line per difference reported by compare_metadata()."""
    if isinstance(diff, list):
        # modify_type, modify_nullable, modify_comment, modify_default
        return [f"{op[0]} {op[2]}.{op[3]}" for op in diff]
    kind = diff[0]
    if kind in ("add_table", "remove_table"):
        return [f"{kind} {diff[1].name}"]
    if kind in ("add_column", "remove_column"):
        return [f"{kind} {diff[2]}.{diff[3].name}"]
    if kind in ("add_index", "remove_index"):
        return [f"{kind} {diff[1].table.name}.{diff[1].name}"]
    return [f"{kind} {diff[1]!r}"]


def _known_drift() -> list[str]:
    lines = KNOWN_DRIFT_FILE.read_text().splitlines()
    return sorted(line for line in lines if line and not line.startswith("#"))


def test_create_db_on_empty_database_reaches_alembic_head(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    expected_head = ScriptDirectory(str(SCHEMA_UPGRADE_DIR)).get_current_head()

    versions = list(
        db_connection.execute(
            sa.text("SELECT version_num FROM alembic_version")
        ).scalars()
    )

    assert versions == [expected_head]


def test_create_db_seeds_configuration_drivers_and_environments(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    def count(table: str) -> int:
        return int(
            db_connection.execute(sa.text(f"SELECT COUNT(*) FROM {table}")).scalar_one()
        )

    environments = set(
        db_connection.execute(
            sa.text("SELECT environment FROM jdbc_connections_environments")
        ).scalars()
    )

    assert count("configuration") == 74
    assert count("jdbc_connections_drivers") == 12
    assert environments == {
        "Production",
        "Integration Test",
        "System Test",
        "Development",
        "Sandbox",
    }


def test_orm_differs_from_migrated_schema_only_by_known_drift(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    context = MigrationContext.configure(db_connection)
    diffs = compare_metadata(context, config_schema.Base.metadata)

    actual = sorted(line for diff in diffs for line in _describe(diff))

    assert actual == _known_drift()
