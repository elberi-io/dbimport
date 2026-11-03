from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.script import ScriptDirectory

pytestmark = pytest.mark.integration

SCHEMA_UPGRADE_DIR = Path(__file__).resolve().parents[2] / "bin" / "SchemaUpgrade"


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
