import sqlalchemy as sa
from alembic import command
from alembic.script import ScriptDirectory

from tests.configdb import REPO_ROOT, alembic_config, schema_rows

SCHEMA_UPGRADE_DIR = REPO_ROOT / "src" / "dbimport" / "migrations"


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


def test_alembic_check_against_migrated_schema_finds_no_difference(
    clean_config_db: None,
) -> None:
    command.check(alembic_config())


def test_orm_creates_the_migrated_schema(
    clean_config_db: None,
    db_connection: sa.Connection,
    orm_db_connection: sa.Connection,
) -> None:
    assert schema_rows(orm_db_connection) == schema_rows(db_connection)
