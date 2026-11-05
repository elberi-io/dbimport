"""How import_config resolves import_tables settings into import and ETL phases."""

import pytest
import sqlalchemy as sa

from common.Exceptions import invalidConfiguration
from DBImportConfig import common_config, import_config
from tests.configdb import insert_row, set_driver_path

HIVE_DB = "testdb"
HIVE_TABLE = "testtable"
DBALIAS = "test_mysql"
# Settings that every incremental import needs.
INCREMENTAL = {"incr_column": "id", "incr_mode": "append"}


def _add_connection(
    conn: sa.Connection, username: str, password: str, configure_driver: bool = True
) -> None:
    if configure_driver:
        set_driver_path(conn, "MySQL")
    insert_row(
        conn,
        "jdbc_connections",
        dbalias=DBALIAS,
        jdbc_url="jdbc:mysql://source.example:3306/sourcedb",
    )
    common_config.config().encryptUserPassword(DBALIAS, username, password)


def _add_import_table(conn: sa.Connection, **settings: str | None) -> None:
    insert_row(
        conn,
        "import_tables",
        **{
            "hive_db": HIVE_DB,
            "hive_table": HIVE_TABLE,
            "dbalias": DBALIAS,
            "source_schema": "src",
            "source_table": "orders",
            **settings,
        },
    )


def _resolve(conn: sa.Connection, **settings: str | None) -> import_config.config:
    _add_connection(conn, "user", "secret")
    _add_import_table(conn, **settings)
    config = import_config.config(HIVE_DB, HIVE_TABLE)
    config.getImportConfig()
    return config


@pytest.mark.parametrize(
    ("import_phase_type", "etl_phase_type", "import_tool"),
    [
        ("full", "truncate_insert", "spark"),
        ("full", "insert", "spark"),
        ("full", "merge", "spark"),
        ("full", "merge_history_audit", "spark"),
        ("full", "none", "spark"),
        ("full", "external", "spark"),
        ("incr", "insert", "spark"),
        ("incr", "merge", "spark"),
        ("incr", "merge_history_audit", "spark"),
        ("oracle_flashback", "merge", "sqoop"),
        ("oracle_flashback", "merge_history_audit", "sqoop"),
        ("mssql_change_tracking", "merge", "spark"),
        ("mssql_change_tracking", "merge_history_audit", "spark"),
    ],
)
def test_get_import_config_valid_combination_sets_import_and_etl_phase(
    clean_config_db: None,
    db_connection: sa.Connection,
    import_phase_type: str,
    etl_phase_type: str,
    import_tool: str,
) -> None:
    config = _resolve(
        db_connection,
        import_phase_type=import_phase_type,
        etl_phase_type=etl_phase_type,
        import_tool=import_tool,
        etl_engine="hive",
        **(INCREMENTAL if import_phase_type == "incr" else {}),
    )

    assert (config.importPhase, config.etlPhase) == (import_phase_type, etl_phase_type)


@pytest.mark.parametrize(
    ("import_phase_type", "etl_phase_type"),
    [
        ("incr", "truncate_insert"),
        ("incr", "none"),
        ("incr", "external"),
        ("oracle_flashback", "insert"),
        ("mssql_change_tracking", "truncate_insert"),
    ],
)
def test_get_import_config_invalid_combination_raises(
    clean_config_db: None,
    db_connection: sa.Connection,
    import_phase_type: str,
    etl_phase_type: str,
) -> None:
    with pytest.raises(invalidConfiguration):
        _resolve(
            db_connection,
            import_phase_type=import_phase_type,
            etl_phase_type=etl_phase_type,
            import_tool="spark",
            etl_engine="hive",
        )


@pytest.mark.parametrize(
    ("import_type", "expected_phases"),
    [
        ("full", ("full", "truncate_insert")),
        ("full_insert", ("full", "insert")),
        ("full_append", ("full", "insert")),
        ("full_merge", ("full", "merge")),
        ("full_merge_direct", ("full", "merge")),
        ("full_history", ("full", "merge_history_audit")),
        ("full_merge_direct_history", ("full", "merge_history_audit")),
        ("incr", ("incr", "insert")),
        ("merge_acid", ("incr", "merge")),
        ("incr_merge_direct", ("incr", "merge")),
    ],
)
def test_get_import_config_legacy_import_type_maps_to_phases(
    clean_config_db: None,
    db_connection: sa.Connection,
    import_type: str,
    expected_phases: tuple[str, str],
) -> None:
    config = _resolve(
        db_connection,
        import_type=import_type,
        import_phase_type=None,
        etl_phase_type=None,
        import_tool="spark",
        etl_engine="hive",
        **(INCREMENTAL if expected_phases[0] == "incr" else {}),
    )

    assert (config.importPhase, config.etlPhase) == expected_phases


def test_get_import_config_incremental_import_without_incr_column_raises(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    with pytest.raises(invalidConfiguration, match="incr_column"):
        _resolve(
            db_connection,
            import_phase_type="incr",
            etl_phase_type="insert",
            import_tool="spark",
            etl_engine="hive",
            incr_mode="append",
        )


def test_get_import_config_legacy_and_new_import_type_together_raises(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    with pytest.raises(invalidConfiguration):
        _resolve(
            db_connection,
            import_type="full",
            import_phase_type="full",
            etl_phase_type="truncate_insert",
            import_tool="spark",
            etl_engine="hive",
        )


def test_get_import_config_sqoop_with_spark_etl_engine_uses_hive(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    config = _resolve(
        db_connection,
        import_phase_type="full",
        etl_phase_type="truncate_insert",
        import_tool="sqoop",
        etl_engine="spark",
    )

    assert config.etlEngine == "hive"


@pytest.mark.parametrize(
    ("import_phase_type", "etl_phase_type", "import_tool", "etl_engine"),
    [
        ("oracle_flashback", "merge", "spark", "hive"),
        ("mssql_change_tracking", "merge", "spark", "spark"),
    ],
)
def test_get_import_config_unsupported_tool_or_engine_raises(
    clean_config_db: None,
    db_connection: sa.Connection,
    import_phase_type: str,
    etl_phase_type: str,
    import_tool: str,
    etl_engine: str,
) -> None:
    with pytest.raises(invalidConfiguration):
        _resolve(
            db_connection,
            import_phase_type=import_phase_type,
            etl_phase_type=etl_phase_type,
            import_tool=import_tool,
            etl_engine=etl_engine,
        )


def test_get_import_config_without_import_tables_row_raises(
    clean_config_db: None,
) -> None:
    with pytest.raises(invalidConfiguration):
        import_config.config(HIVE_DB, HIVE_TABLE).getImportConfig()


def test_lookup_connection_alias_returns_decrypted_credentials(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    _add_connection(db_connection, "user", "secret")

    config = common_config.config()
    config.lookupConnectionAlias(DBALIAS)

    assert (config.jdbc_username, config.jdbc_password) == ("user", "secret")


def test_lookup_connection_alias_with_seeded_driver_placeholder_raises(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    # setup seeds the driver classpath as "add path to JAR file".
    _add_connection(db_connection, "user", "secret", configure_driver=False)

    with pytest.raises(invalidConfiguration):
        common_config.config().lookupConnectionAlias(DBALIAS)


@pytest.mark.xfail(
    reason="Known bug: stored credentials are split on spaces, so a password "
    "containing a space is truncated",
    raises=AssertionError,
    strict=True,
)
def test_lookup_connection_alias_keeps_password_with_space(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    _add_connection(db_connection, "user", "pass word")

    config = common_config.config()
    config.lookupConnectionAlias(DBALIAS)

    assert config.jdbc_password == "pass word"
