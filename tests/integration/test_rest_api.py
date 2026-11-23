"""Basic behaviour of the REST API in dbimport.server.rest_server."""

import importlib
import sys
from collections.abc import Iterator
from datetime import datetime, timedelta
from types import ModuleType

import pytest
import sqlalchemy as sa
from fastapi.testclient import TestClient

from dbimport.common import constants
from tests.configdb import insert_row

USERNAME = "tester"
PASSWORD = "secret"
ADMIN_KEYS = [
    "restserver_admin_user",
    "restserver_authentication_method",
    "restserver_token_ttl",
]


@pytest.fixture
def rest_server(clean_config_db: None) -> ModuleType:
    # Imported here, not at module level: dbimport.server.rest_server connects to the
    # configuration database when it is imported. Reloaded for every later
    # test, because its module-level objects keep the DBImport singletons
    # that are closed after each test.
    module = sys.modules.get("dbimport.server.rest_server")
    if module is None:
        return importlib.import_module("dbimport.server.rest_server")
    return importlib.reload(module)


@pytest.fixture
def client(rest_server: ModuleType) -> Iterator[TestClient]:
    with TestClient(rest_server.app) as test_client:
        yield test_client


@pytest.fixture
def user(rest_server: ModuleType, db_connection: sa.Connection) -> None:
    db_connection.execute(
        sa.text("INSERT INTO auth_users (username, password) VALUES (:user, :hash)"),
        {"user": USERNAME, "hash": rest_server.get_password_hash(PASSWORD)},
    )


@pytest.fixture
def auth_headers(client: TestClient, user: None) -> dict[str, str]:
    response = client.post(
        "/oauth2/access_token", data={"username": USERNAME, "password": PASSWORD}
    )
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


def _add_import_table(conn: sa.Connection, hive_db: str, dbalias: str) -> None:
    insert_row(
        conn,
        "import_tables",
        hive_db=hive_db,
        hive_table="orders",
        dbalias=dbalias,
        source_schema="src",
        source_table="orders",
        import_phase_type="full",
        etl_phase_type="truncate_insert",
    )


def test_status_returns_ok_and_version(client: TestClient) -> None:
    response = client.get("/status")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": constants.VERSION}


def test_protected_route_without_token_returns_401(client: TestClient) -> None:
    response = client.get("/connection")

    assert response.status_code == 401


def test_login_with_valid_credentials_returns_bearer_token(
    client: TestClient, user: None
) -> None:
    response = client.post(
        "/oauth2/access_token", data={"username": USERNAME, "password": PASSWORD}
    )

    assert response.status_code == 200
    assert response.json()["token_type"] == "bearer"
    assert response.json()["access_token"]


def test_login_with_wrong_password_returns_401(client: TestClient, user: None) -> None:
    response = client.post(
        "/oauth2/access_token", data={"username": USERNAME, "password": "wrong"}
    )

    assert response.status_code == 401


def test_protected_route_with_token_returns_200(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    response = client.get("/connection", headers=auth_headers)

    assert response.status_code == 200
    assert response.json() == []


def test_import_tables_on_connection_lists_tables_of_that_connection(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    _add_import_table(db_connection, hive_db="salesdb", dbalias="src_conn")

    response = client.get("/import/table/src_conn", headers=auth_headers)

    assert response.status_code == 200
    assert [(t["database"], t["connection"]) for t in response.json()] == [
        ("salesdb", "src_conn")
    ]


@pytest.mark.xfail(
    reason="Known bug: GET /import/table/{database} has the same path as "
    "GET /import/table/{connection}, so the database lookup is unreachable",
    raises=AssertionError,
    strict=True,
)
def test_import_tables_in_database_lists_tables_of_that_database(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    _add_import_table(db_connection, hive_db="salesdb", dbalias="src_conn")

    response = client.get("/import/table/salesdb", headers=auth_headers)

    assert [t["database"] for t in response.json()] == ["salesdb"]


@pytest.mark.xfail(
    reason="Known bug: authenticate_user reads the user before checking that "
    "it exists, so an unknown user causes HTTP 500 instead of 401",
    raises=AssertionError,
    strict=True,
)
def test_login_with_unknown_user_returns_401(rest_server: ModuleType) -> None:
    with TestClient(rest_server.app, raise_server_exceptions=False) as client:
        response = client.post(
            "/oauth2/access_token", data={"username": "nobody", "password": "x"}
        )

    assert response.status_code == 401


def test_get_config_does_not_return_the_secret_key(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    response = client.get("/config/getConfig", headers=auth_headers)

    assert response.status_code == 200
    assert "restserver_secret_key" not in response.json()


def test_get_config_hides_the_admin_keys_from_other_users(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    response = client.get("/config/getConfig", headers=auth_headers)

    assert [response.json()[key] for key in ADMIN_KEYS] == [None, None, None]
    assert response.json()["timezone"] == "UTC"


def test_get_config_ignores_rows_of_unknown_keys(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    insert_row(
        db_connection, "configuration", configKey="sqoop_max_mappers", valueInt=4
    )

    response = client.get("/config/getConfig", headers=auth_headers)

    assert response.status_code == 200
    assert "sqoop_max_mappers" not in response.json()


@pytest.mark.parametrize(
    ("change", "status_code"),
    [
        ({"airflow_major_version": 3}, 400),
        ({"restserver_authentication_method": "ldap"}, 400),
        ({"timezone": " "}, 400),
        ({"restserver_admin_user": "tester"}, 403),
        ({"restserver_authentication_method": "pam"}, 403),
        ({"restserver_token_ttl": 60}, 403),
    ],
)
def test_update_config_invalid_or_admin_only_change_is_rejected(
    client: TestClient,
    auth_headers: dict[str, str],
    change: dict[str, object],
    status_code: int,
) -> None:
    response = client.post("/config/updateConfig", json=change, headers=auth_headers)

    assert response.status_code == status_code


def test_update_config_stores_the_new_values(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    response = client.post(
        "/config/updateConfig",
        json={"rest_timeout": 9, "airflow_disable": True, "timezone": "UTC+1"},
        headers=auth_headers,
    )

    assert response.status_code == 200
    rows = db_connection.execute(
        sa.text(
            "SELECT configKey, valueInt, valueStr FROM configuration "
            "WHERE configKey IN ('rest_timeout', 'airflow_disable', 'timezone')"
        )
    )
    assert {row[0]: row[1:] for row in rows} == {
        "rest_timeout": (9, None),
        "airflow_disable": (1, None),
        "timezone": (None, "UTC+1"),
    }


def test_update_config_leaves_the_other_keys_unchanged(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    query = sa.text(
        "SELECT configKey, valueInt, valueStr FROM configuration "
        "WHERE configKey NOT IN ('rest_timeout', 'timezone') ORDER BY configKey"
    )
    before = db_connection.execute(query).all()

    response = client.post(
        "/config/updateConfig",
        json={"rest_timeout": 9, "timezone": "UTC+1"},
        headers=auth_headers,
    )

    assert response.status_code == 200
    assert db_connection.execute(query).all() == before


def test_import_table_update_stores_the_changed_settings(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    insert_row(db_connection, "jdbc_connections", dbalias="src_conn", jdbc_url="x")
    table = {
        "connection": "src_conn",
        "sourceSchema": "src",
        "sourceTable": "orders",
        "importPhaseType": "full",
        "etlPhaseType": "truncate_insert",
        "importTool": "spark",
        "etlEngine": "spark",
    }
    changes = {"validationMethod": "customQuery", "mergeCompactionMethod": "major"}

    created = client.post(
        "/import/table/salesdb/orders", json=table, headers=auth_headers
    )
    updated = client.post(
        "/import/table/salesdb/orders", json=table | changes, headers=auth_headers
    )

    assert (created.status_code, updated.status_code) == (200, 200)
    row = db_connection.execute(
        sa.text(
            "SELECT validationMethod, mergeCompactionMethod FROM import_tables "
            "WHERE hive_db = 'salesdb' AND hive_table = 'orders'"
        )
    ).one()
    assert tuple(row) == ("customQuery", "major")


def test_export_table_update_stores_the_changed_settings(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    insert_row(db_connection, "jdbc_connections", dbalias="tgt_conn", jdbc_url="x")
    table = {
        "exportType": "full",
        "exportTool": "spark",
        "database": "salesdb",
        "table": "orders",
    }
    changes = {"validationMethod": "customQuery", "forceCreateTempTable": True}

    created = client.post(
        "/export/table/tgt_conn/tgt/orders", json=table, headers=auth_headers
    )
    updated = client.post(
        "/export/table/tgt_conn/tgt/orders", json=table | changes, headers=auth_headers
    )

    assert (created.status_code, updated.status_code) == (200, 200)
    row = db_connection.execute(
        sa.text(
            "SELECT validationMethod, forceCreateTempTable FROM export_tables "
            "WHERE dbalias = 'tgt_conn' AND target_table = 'orders'"
        )
    ).one()
    assert tuple(row) == ("customQuery", 1)


@pytest.fixture
def admin_headers(client: TestClient) -> dict[str, str]:
    # The REST server creates the admin user with password "admin" at start.
    response = client.post(
        "/oauth2/access_token", data={"username": "admin", "password": "admin"}
    )
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


@pytest.mark.parametrize(
    ("details", "stored"),
    [
        ({}, ("", "", "")),
        (
            {"fullname": "New User", "department": "Data", "email": "new@example.org"},
            ("New User", "Data", "new@example.org"),
        ),
    ],
)
def test_create_user_stores_the_given_or_empty_details(
    client: TestClient,
    admin_headers: dict[str, str],
    db_connection: sa.Connection,
    details: dict[str, str],
    stored: tuple[str, str, str],
) -> None:
    response = client.post(
        "/users/createUser",
        json={"username": "newuser", "password": "pw", "disabled": False} | details,
        headers=admin_headers,
    )

    assert response.status_code == 200
    row = db_connection.execute(
        sa.text(
            "SELECT fullname, department, email FROM auth_users "
            "WHERE username = 'newuser'"
        )
    ).one()
    assert tuple(row) == stored


def test_get_jdbc_drivers_lists_the_drivers_in_database_type_order(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    response = client.get("/config/getJDBCdrivers", headers=auth_headers)

    assert response.status_code == 200
    drivers = response.json()
    # database_type is an ENUM: the database sorts by the order of its values.
    assert [driver["databaseType"] for driver in drivers] == [
        "DB2 AS400",
        "DB2 UDB",
        "MySQL",
        "Oracle",
        "PostgreSQL",
        "Progress DB",
        "SQL Server",
        "SQL Server",
        "CacheDB",
        "Snowflake",
        "Informix",
        "SQL Anywhere",
    ]
    assert {
        "databaseType": "PostgreSQL",
        "version": "default",
        "driver": "org.postgresql.Driver",
        "classpath": "add path to JAR file",
    } in drivers


DRIVER_ROW = sa.text(
    "SELECT driver, classpath FROM jdbc_connections_drivers "
    "WHERE database_type = 'PostgreSQL' AND version = 'default'"
)


def test_update_jdbc_driver_stores_driver_and_classpath(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    driver = {
        "databaseType": "PostgreSQL",
        "version": "default",
        "driver": "org.example.Driver",
        "classpath": "/opt/jdbc/postgresql.jar",
    }

    response = client.post(
        "/config/updateJDBCdriver", json=driver, headers=auth_headers
    )

    assert response.status_code == 200
    assert tuple(db_connection.execute(DRIVER_ROW).one()) == (
        "org.example.Driver",
        "/opt/jdbc/postgresql.jar",
    )


def test_update_jdbc_driver_of_unknown_version_returns_400(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    driver = {
        "databaseType": "PostgreSQL",
        "version": "no_such_version",
        "driver": "org.example.Driver",
        "classpath": "/opt/jdbc/postgresql.jar",
    }

    response = client.post(
        "/config/updateJDBCdriver", json=driver, headers=auth_headers
    )

    assert response.status_code == 400
    assert (
        db_connection.execute(
            sa.text("SELECT COUNT(*) FROM jdbc_connections_drivers")
        ).scalar_one()
        == 12
    )


def test_update_jdbc_driver_with_its_current_values_returns_200(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    driver = {
        "databaseType": "PostgreSQL",
        "version": "default",
        "driver": "org.postgresql.Driver",
        "classpath": "add path to JAR file",
    }

    response = client.post(
        "/config/updateJDBCdriver", json=driver, headers=auth_headers
    )

    assert response.status_code == 200


MYSQL_URL = "jdbc:mysql://source.example:3306/sourcedb"


def _add_connections(conn: sa.Connection) -> None:
    insert_row(conn, "jdbc_connections", dbalias="sales_mysql", jdbc_url=MYSQL_URL)
    insert_row(conn, "jdbc_connections", dbalias="hr_other", jdbc_url="x")
    insert_row(
        conn,
        "jdbc_connections",
        dbalias="sales_other",
        jdbc_url="x",
        owner="team",
    )


def test_get_all_connections_lists_names_urls_and_server_types_by_name(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    _add_connections(db_connection)

    response = client.get("/connection", headers=auth_headers)

    assert response.status_code == 200
    assert response.headers["content-rows"] == "3"
    assert response.json() == [
        {"name": "hr_other", "connectionString": "x", "serverType": None},
        {"name": "sales_mysql", "connectionString": MYSQL_URL, "serverType": "MySQL"},
        {"name": "sales_other", "connectionString": "x", "serverType": None},
    ]


def test_get_all_connections_with_list_only_name_leaves_out_the_details(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    _add_connections(db_connection)

    response = client.get("/connection?listOnlyName=true", headers=auth_headers)

    assert response.json() == [
        {"name": "hr_other"},
        {"name": "sales_mysql"},
        {"name": "sales_other"},
    ]


@pytest.mark.parametrize(
    ("search", "expected", "total"),
    [
        ({"name": "sales*"}, ["sales_mysql", "sales_other"], "2"),
        ({"name": "sales_other"}, ["sales_other"], "1"),
        ({"name": "sales*", "owner": "team"}, ["sales_other"], "1"),
        ({"name": "*", "owner": ""}, ["hr_other", "sales_mysql", "sales_other"], "3"),
    ],
)
def test_search_connections_filters_by_value_or_wildcard(
    client: TestClient,
    auth_headers: dict[str, str],
    db_connection: sa.Connection,
    search: dict[str, str],
    expected: list[str],
    total: str,
) -> None:
    _add_connections(db_connection)

    response = client.post("/connection/search", json=search, headers=auth_headers)

    assert response.status_code == 200
    assert sorted(c["name"] for c in response.json()) == expected
    assert response.headers["content-total-rows"] == total


CONNECTION_ROW = sa.text(
    "SELECT jdbc_url, owner, force_string, create_datalake_import, "
    "create_foreign_keys, atlas_discovery, timewindow_start, description "
    "FROM jdbc_connections WHERE dbalias = 'new_conn'"
)


def test_update_connection_creates_a_connection_with_defaults(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    connection = {
        "name": "new_conn",
        "connectionString": MYSQL_URL,
        "owner": "team",
        "timeWindowStart": "08:00:00",
    }

    response = client.post("/connection", json=connection, headers=auth_headers)

    assert response.status_code == 200
    assert tuple(db_connection.execute(CONNECTION_ROW).one()) == (
        MYSQL_URL,
        "team",
        0,
        1,
        1,
        1,
        timedelta(hours=8),
        None,
    )


def test_update_connection_replaces_every_column_of_an_existing_connection(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    insert_row(
        db_connection,
        "jdbc_connections",
        dbalias="new_conn",
        jdbc_url="x",
        owner="team",
        description="old",
        force_string=1,
    )
    connection = {"name": "new_conn", "connectionString": MYSQL_URL}

    response = client.post("/connection", json=connection, headers=auth_headers)

    assert response.status_code == 200
    assert tuple(db_connection.execute(CONNECTION_ROW).one()) == (
        MYSQL_URL,
        None,
        0,
        1,
        1,
        1,
        None,
        None,
    )


def test_update_connection_with_invalid_time_returns_400_and_stores_nothing(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    connection = {
        "name": "new_conn",
        "connectionString": MYSQL_URL,
        "timeWindowStart": "8:00",
    }

    response = client.post("/connection", json=connection, headers=auth_headers)

    assert response.status_code == 400
    assert response.json() == "Invalid value for timeWindowStart"
    assert (
        db_connection.execute(
            sa.text("SELECT COUNT(*) FROM jdbc_connections")
        ).scalar_one()
        == 0
    )


def test_get_connection_returns_its_details(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    insert_row(
        db_connection,
        "jdbc_connections",
        dbalias="sales_mysql",
        jdbc_url=MYSQL_URL,
        credentials="encrypted",
        datalake_source="sales",
        max_import_sessions=4,
        timewindow_start="22:00:00",
        timewindow_stop="02:30:00",
        timewindow_timezone="UTC",
        operator_notes="notes",
        contact_info="contact",
        description="description",
        owner="team",
        environment="Production",
        seed_file="/opt/seed",
        atlas_include_filter="sales.*",
        atlas_last_discovery="2026-01-15 09:00:00",
    )

    response = client.get("/connection/sales_mysql", headers=auth_headers)

    assert response.status_code == 200
    assert response.json() == {
        "name": "sales_mysql",
        "connectionString": MYSQL_URL,
        "privateKeyPath": None,
        "publicKeyPath": None,
        "credentials": "encrypted",
        "source": "sales",
        "forceString": 0,
        "maxSessions": 4,
        "createDatalakeImport": True,
        "timeWindowStart": "22:00:00",
        "timeWindowStop": "02:30:00",
        "timeWindowTimezone": "UTC",
        "operatorNotes": "notes",
        "contactInformation": "contact",
        "description": "description",
        "owner": "team",
        "environment": "Production",
        "seedFile": "/opt/seed",
        "createForeignKey": True,
        "atlasDiscovery": True,
        "atlasIncludeFilter": "sales.*",
        "atlasExcludeFilter": None,
        "atlasLastDiscovery": "2026-01-15 09:00:00",
    }


def test_saving_a_connection_with_the_body_of_get_keeps_every_column(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    # The UI saves a connection with the body that GET returned.
    insert_row(
        db_connection,
        "jdbc_connections",
        dbalias="sales_mysql",
        jdbc_url=MYSQL_URL,
        private_key_path="/keys/private.pem",
        public_key_path="/keys/public.pem",
        credentials="encrypted",
        datalake_source="sales",
        max_import_sessions=4,
        force_string=1,
        create_datalake_import=0,
        timewindow_start="22:00:00",
        timewindow_stop="02:30:00",
        timewindow_timezone="UTC",
        operator_notes="notes",
        create_foreign_keys=0,
        contact_info="contact",
        description="description",
        owner="team",
        atlas_discovery=0,
        atlas_include_filter="sales.*",
        atlas_exclude_filter="tmp.*",
        atlas_last_discovery="2026-01-15 09:00:00",
        environment="Production",
        seed_file="/opt/seed",
    )
    row = sa.text("SELECT * FROM jdbc_connections")
    before = db_connection.execute(row).one()
    details = client.get("/connection/sales_mysql", headers=auth_headers).json()

    response = client.post("/connection", json=details, headers=auth_headers)

    assert response.status_code == 200
    assert db_connection.execute(row).one() == before


def test_get_connection_of_unknown_connection_returns_404_and_ends_its_transaction(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    response = client.get("/connection/no_such_conn", headers=auth_headers)

    # An open transaction on the table would make the TRUNCATE wait.
    db_connection.execute(
        sa.text(
            "SET STATEMENT lock_wait_timeout = 2, foreign_key_checks = 0 "
            "FOR TRUNCATE TABLE jdbc_connections"
        )
    )
    assert response.status_code == 404


@pytest.mark.parametrize(
    ("dbalias", "status_code", "remaining"), [("hr_other", 200, 2), ("nope", 404, 3)]
)
def test_delete_connection_removes_it_or_returns_404_when_unknown(
    client: TestClient,
    auth_headers: dict[str, str],
    db_connection: sa.Connection,
    dbalias: str,
    status_code: int,
    remaining: int,
) -> None:
    _add_connections(db_connection)

    response = client.delete(f"/connection/{dbalias}", headers=auth_headers)

    assert response.status_code == status_code
    assert (
        db_connection.execute(
            sa.text("SELECT COUNT(*) FROM jdbc_connections")
        ).scalar_one()
        == remaining
    )


def _add_table(
    conn: sa.Connection,
    hive_db: str = "salesdb",
    hive_table: str = "orders",
    /,
    **values: object,
) -> int:
    insert_row(
        conn,
        "import_tables",
        **{
            "hive_db": hive_db,
            "hive_table": hive_table,
            "dbalias": "src_conn",
            "source_schema": "src",
            "source_table": hive_table,
            "import_phase_type": "full",
            "etl_phase_type": "truncate_insert",
            **values,
        },
    )
    table_id: int = conn.execute(
        sa.text(
            "SELECT table_id FROM import_tables WHERE hive_db = :d AND hive_table = :t"
        ),
        {"d": hive_db, "t": hive_table},
    ).scalar_one()
    return table_id


def _tables(conn: sa.Connection) -> list[tuple[str, str]]:
    query = "SELECT hive_db, hive_table FROM import_tables ORDER BY hive_db, hive_table"
    return [(row[0], row[1]) for row in conn.execute(sa.text(query))]


def test_get_all_import_databases_summarises_each_database(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    _add_table(
        db_connection,
        "salesdb",
        "orders",
        last_update_from_source="2026-01-15 09:00:00",
        sqoop_last_size=100,
        sqoop_last_rows=10,
    )
    _add_table(
        db_connection,
        "salesdb",
        "lines",
        last_update_from_source="2026-01-16 09:00:00",
        sqoop_last_size=50,
        sqoop_last_rows=5,
    )
    _add_table(db_connection, "hrdb", "staff")

    response = client.get("/import/db", headers=auth_headers)

    assert response.status_code == 200
    assert sorted(response.json(), key=lambda db: db["name"]) == [
        {"name": "hrdb", "tables": 1, "lastImport": "", "lastSize": 0, "lastRows": 0},
        {
            "name": "salesdb",
            "tables": 2,
            "lastImport": "2026-01-16 09:00:00",
            "lastSize": 150,
            "lastRows": 15,
        },
    ]
    assert response.headers["content-rows"] == "2"


def _add_search_tables(conn: sa.Connection) -> None:
    _add_table(conn, "salesdb", "orders")
    _add_table(conn, "salesdb", "lines", import_phase_type="incr")
    _add_table(
        conn,
        "hrdb",
        "staff",
        dbalias="hr_conn",
        include_in_airflow=0,
        operator_notes="hr team",
    )


@pytest.mark.parametrize(
    ("search", "expected"),
    [
        ({"database": "salesdb"}, [("salesdb", "lines"), ("salesdb", "orders")]),
        ({"database": "sales*"}, [("salesdb", "lines"), ("salesdb", "orders")]),
        ({"table": "%e%"}, [("salesdb", "lines"), ("salesdb", "orders")]),
        ({"importPhaseType": "incr"}, [("salesdb", "lines")]),
        ({"connection": "hr_conn", "table": "*"}, [("hrdb", "staff")]),
        ({"includeInAirflow": False}, [("hrdb", "staff")]),
        (
            {
                "database": "hrdb",
                "table": "staff",
                "connection": "hr_conn",
                "sourceSchema": "src",
                "sourceTable": "staff",
                "importPhaseType": "full",
                "etlPhaseType": "truncate_insert",
                "importTool": "spark",
                "etlEngine": "hive",
                "includeInAirflow": False,
                "operatorNotes": "hr*",
            },
            [("hrdb", "staff")],
        ),
        (
            {"database": ""},
            [("hrdb", "staff"), ("salesdb", "lines"), ("salesdb", "orders")],
        ),
    ],
)
def test_search_import_tables_filters_by_value_or_wildcard(
    client: TestClient,
    auth_headers: dict[str, str],
    db_connection: sa.Connection,
    search: dict[str, object],
    expected: list[tuple[str, str]],
) -> None:
    _add_search_tables(db_connection)

    response = client.post("/import/search", json=search, headers=auth_headers)

    assert response.status_code == 200
    found = sorted((t["database"], t["table"]) for t in response.json())
    assert found == expected
    assert response.headers["content-total-rows"] == str(len(expected))


def test_search_import_tables_returns_the_list_fields(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    _add_table(
        db_connection,
        import_tool="sqoop",
        etl_engine="spark",
        last_update_from_source="2026-01-15 09:00:00",
        operator_notes="notes",
    )

    response = client.post(
        "/import/search", json={"database": "salesdb"}, headers=auth_headers
    )

    assert response.json() == [
        {
            "database": "salesdb",
            "table": "orders",
            "connection": "src_conn",
            "sourceSchema": "src",
            "sourceTable": "orders",
            "importPhaseType": "full",
            "etlPhaseType": "truncate_insert",
            "importTool": "sqoop",
            "etlEngine": "spark",
            "lastUpdateFromSource": "2026-01-15 09:00:00",
            "includeInAirflow": True,
            "operatorNotes": "notes",
        }
    ]


DETAILS_ROW = {
    "import_phase_type": "incr",
    "etl_phase_type": "merge",
    "import_tool": "sqoop",
    "etl_engine": "spark",
    "last_update_from_source": "2026-01-15 09:00:00",
    "sqoop_sql_where_addition": "a = 1",
    "nomerge_ingestion_sql_addition": "b = 2",
    "include_in_airflow": 0,
    "airflow_priority": 5,
    "validate_import": 0,
    "validationMethod": "customQuery",
    "validate_source": "sqoop",
    "validate_diff_allowed": 7,
    "validationCustomQuerySourceSQL": "select 1",
    "validationCustomQueryHiveSQL": "select 2",
    "validationCustomQueryValidateImportTable": 0,
    "truncate_hive": 0,
    "mappers": 3,
    "soft_delete_during_merge": 1,
    "source_rowcount": 10,
    "source_rowcount_incr": 2,
    "hive_rowcount": 9,
    "validationCustomQuerySourceValue": "[1]",
    "validationCustomQueryHiveValue": "[2]",
    "incr_mode": "append",
    "incr_column": "id",
    "incr_validation_method": "incr",
    "incr_minvalue": "1",
    "incr_maxvalue": "5",
    "incr_minvalue_pending": "5",
    "incr_maxvalue_pending": "9",
    "pk_column_override": "id",
    "pk_column_override_mergeonly": "id2",
    "hive_merge_heap": 2048,
    "hive_split_count": 4,
    "spark_executor_memory": "2g",
    "spark_executors": 3,
    "split_by_column": "id",
    "sqoop_query": "select x",
    "sqoop_options": "--opt",
    "sqoop_last_size": 1000,
    "sqoop_last_rows": 10,
    "sqoop_last_mappers": 2,
    "sqoop_last_execution": 1700000000,
    "sqoop_use_generated_sql": 0,
    "sqoop_allow_text_splitter": 1,
    "force_string": 1,
    "comment": "the orders",
    "generated_hive_column_definition": "(a int)",
    "generated_sqoop_query": "select gen",
    "generated_sqoop_options": "--gen",
    "generated_pk_columns": "id",
    "generated_foreign_keys": "fk",
    "datalake_source": "sales",
    "operator_notes": "notes",
    "copy_finished": "2026-01-16 10:00:00",
    "copy_slave": 1,
    "create_foreign_keys": 0,
    "invalidate_impala": 1,
    "custom_max_query": "select max(x)",
    "mergeCompactionMethod": "minor",
    "sourceTableType": "view",
    "import_database": "imp_db",
    "import_table": "imp_tbl",
    "history_database": "hist_db",
    "history_table": "hist_tbl",
}

DETAILS = {
    "database": "salesdb",
    "table": "orders",
    "connection": "src_conn",
    "sourceSchema": "src",
    "sourceTable": "orders",
    "importPhaseType": "incr",
    "etlPhaseType": "merge",
    "importTool": "sqoop",
    "etlEngine": "spark",
    "lastUpdateFromSource": "2026-01-15 09:00:00",
    "sqlWhereAddition": "a = 1",
    "nomergeIngestionSqlAddition": "b = 2",
    "includeInAirflow": False,
    "airflowPriority": 5,
    "validateImport": False,
    "validationMethod": "customQuery",
    "validateSource": "sqoop",
    "validateDiffAllowed": 7,
    "validationCustomQuerySourceSQL": "select 1",
    "validationCustomQueryHiveSQL": "select 2",
    "validationCustomQueryValidateImportTable": False,
    "sqlSessions": 3,
    "softDeleteDuringMerge": True,
    "sourceRowcount": 10,
    "sourceRowcountIncr": 2,
    "targetRowcount": 9,
    "validationCustomQuerySourceValue": "[1]",
    "validationCustomQueryHiveValue": "[2]",
    "incrMode": "append",
    "incrColumn": "id",
    "incrValidationMethod": "incr",
    "incrMinvalue": "1",
    "incrMaxvalue": "5",
    "incrMinvaluePending": "5",
    "incrMaxvaluePending": "9",
    "pkColumnOverride": "id",
    "pkColumnOverrideMergeonly": "id2",
    "hiveContainerSize": 2048,
    "splitCount": 4,
    "sparkExecutorMemory": "2g",
    "sparkExecutors": 3,
    "splitByColumn": "id",
    "sqoopCustomQuery": "select x",
    "sqoopOptions": "--opt",
    "lastSize": 1000,
    "lastRows": 10,
    "lastSqlSessions": 2,
    "lastExecution": 1700000000,
    "sqoopUseGeneratedSql": False,
    "allowTextSplitter": True,
    "forceString": 1,
    "comment": "the orders",
    "generatedHiveColumnDefinition": "(a int)",
    "generatedSqoopQuery": "select gen",
    "generatedSqoopOptions": "--gen",
    "generatedPkColumns": "id",
    "datalakeSource": "sales",
    "operatorNotes": "notes",
    "copyFinished": "2026-01-16 10:00:00",
    "copySlave": True,
    "createForeignKeys": 0,
    "invalidateImpala": 1,
    "customMaxQuery": "select max(x)",
    "mergeCompactionMethod": "minor",
    "sourceTableType": "view",
    "importDatabase": "imp_db",
    "importTable": "imp_tbl",
    "historyDatabase": "hist_db",
    "historyTable": "hist_tbl",
    "columns": [],
}


def test_get_import_table_details_returns_every_field(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    _add_table(db_connection, **DETAILS_ROW)

    response = client.get(
        "/import/table/salesdb/orders?includeColumns=false", headers=auth_headers
    )

    assert response.status_code == 200
    assert response.json() == DETAILS


def test_get_import_table_details_replaces_values_the_ui_cannot_show(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    _add_table(
        db_connection, incr_mode=None, sqoop_use_generated_sql=-1, force_string=5
    )

    response = client.get(
        "/import/table/salesdb/orders?includeColumns=false", headers=auth_headers
    )

    details = response.json()
    assert (
        details["incrMode"],
        details["sqoopUseGeneratedSql"],
        details["forceString"],
        details["lastUpdateFromSource"],
        details["copyFinished"],
    ) == ("lastmodified", True, -1, None, None)


def _add_column(
    conn: sa.Connection, table_id: int, name: str, order: int, **values: object
) -> None:
    insert_row(
        conn,
        "import_columns",
        **{
            "table_id": table_id,
            "hive_db": "salesdb",
            "hive_table": "orders",
            "column_name": name,
            "column_order": order,
            "source_column_name": name.upper(),
            "column_type": "int",
            "source_column_type": "int(11)",
            "last_update_from_source": "2026-01-15 09:00:00",
            **values,
        },
    )


def test_get_import_table_details_includes_the_columns_in_order(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    table_id = _add_table(db_connection)
    _add_column(
        db_connection,
        table_id,
        "note",
        2,
        column_type="string",
        source_column_type="varchar(50)",
        source_database_type="mysql",
        column_name_override="remark",
        column_type_override="varchar(60)",
        sqoop_column_type="String",
        sqoop_column_type_override="Integer",
        force_string=7,
        include_in_import=0,
        comment="free text",
        operator_notes="notes",
        anonymization_function="Hash",
    )
    _add_column(db_connection, table_id, "id", 1, source_primary_key=1)

    response = client.get("/import/table/salesdb/orders", headers=auth_headers)

    assert response.json()["columns"] == [
        {
            "columnName": "id",
            "columnOrder": 1,
            "sourceColumnName": "ID",
            "columnType": "int",
            "sourceColumnType": "int(11)",
            "sourceDatabaseType": None,
            "columnNameOverride": None,
            "columnTypeOverride": None,
            "sqoopColumnType": None,
            "sqoopColumnTypeOverride": None,
            "forceString": -1,
            "includeInImport": True,
            "sourcePrimaryKey": 1,
            "lastUpdateFromSource": "2026-01-15 09:00:00",
            "comment": None,
            "operatorNotes": None,
            "anonymizationFunction": "None",
        },
        {
            "columnName": "note",
            "columnOrder": 2,
            "sourceColumnName": "NOTE",
            "columnType": "string",
            "sourceColumnType": "varchar(50)",
            "sourceDatabaseType": "mysql",
            "columnNameOverride": "remark",
            "columnTypeOverride": "varchar(60)",
            "sqoopColumnType": "String",
            "sqoopColumnTypeOverride": "Integer",
            "forceString": -1,
            "includeInImport": False,
            "sourcePrimaryKey": None,
            "lastUpdateFromSource": "2026-01-15 09:00:00",
            "comment": "free text",
            "operatorNotes": "notes",
            "anonymizationFunction": "Hash",
        },
    ]


@pytest.mark.parametrize(
    ("table", "status_code", "remaining"),
    [
        ("orders", 200, [("hrdb", "staff"), ("salesdb", "lines")]),
        ("nope", 404, [("hrdb", "staff"), ("salesdb", "lines"), ("salesdb", "orders")]),
    ],
)
def test_delete_import_table_removes_it_or_returns_404_when_unknown(
    client: TestClient,
    auth_headers: dict[str, str],
    db_connection: sa.Connection,
    table: str,
    status_code: int,
    remaining: list[tuple[str, str]],
) -> None:
    _add_search_tables(db_connection)

    response = client.delete(f"/import/table/salesdb/{table}", headers=auth_headers)

    assert response.status_code == status_code
    assert _tables(db_connection) == remaining


def test_bulk_delete_of_import_tables_removes_the_listed_tables(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    _add_search_tables(db_connection)

    response = client.request(
        "DELETE",
        "/import/table",
        json=[
            {"database": "salesdb", "table": "orders"},
            {"database": "hrdb", "table": "staff"},
            {"database": "salesdb", "table": "nope"},
        ],
        headers=auth_headers,
    )

    assert response.status_code == 200
    assert _tables(db_connection) == [("salesdb", "lines")]


@pytest.mark.parametrize(
    ("method", "body"), [("DELETE", []), ("POST", {"importTables": []})]
)
def test_bulk_change_of_import_tables_without_tables_returns_422(
    client: TestClient,
    auth_headers: dict[str, str],
    method: str,
    body: object,
) -> None:
    response = client.request(method, "/import/table", json=body, headers=auth_headers)

    assert response.status_code == 422


def test_bulk_update_of_import_tables_changes_only_the_given_fields(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    _add_search_tables(db_connection)

    response = client.post(
        "/import/table",
        json={
            "importTool": "sqoop",
            "includeInAirflow": False,
            "operatorNotes": "moved",
            "importTables": [
                {"database": "salesdb", "table": "orders"},
                {"database": "hrdb", "table": "staff"},
            ],
        },
        headers=auth_headers,
    )

    assert response.status_code == 200
    rows = db_connection.execute(
        sa.text(
            "SELECT hive_db, hive_table, dbalias, import_tool, include_in_airflow,"
            " operator_notes FROM import_tables ORDER BY hive_db, hive_table"
        )
    )
    assert [tuple(row) for row in rows] == [
        ("hrdb", "staff", "hr_conn", "sqoop", 0, "moved"),
        ("salesdb", "lines", "src_conn", "spark", 1, None),
        ("salesdb", "orders", "src_conn", "sqoop", 0, "moved"),
    ]


def test_bulk_update_of_import_tables_sets_every_field_it_accepts(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    _add_table(db_connection)

    response = client.post(
        "/import/table",
        json={
            "connection": "new_conn",
            "importPhaseType": "incr",
            "etlPhaseType": "merge",
            "importTool": "sqoop",
            "etlEngine": "hive",
            "includeInAirflow": False,
            "operatorNotes": "moved",
            "importTables": [{"database": "salesdb", "table": "orders"}],
        },
        headers=auth_headers,
    )

    assert response.status_code == 200
    row = db_connection.execute(
        sa.text(
            "SELECT dbalias, import_phase_type, etl_phase_type, import_tool,"
            " etl_engine, include_in_airflow, operator_notes FROM import_tables"
        )
    ).one()
    assert tuple(row) == ("new_conn", "incr", "merge", "sqoop", "hive", 0, "moved")


WRITE_MINIMAL = {
    "connection": "src_conn",
    "sourceSchema": "src",
    "sourceTable": "orders",
    "importPhaseType": "full",
    "etlPhaseType": "truncate_insert",
    "importTool": "spark",
    "etlEngine": "spark",
}
DEFAULTED_COLUMNS = (
    "include_in_airflow, validate_import, validationMethod, validate_source,"
    " validate_diff_allowed, validationCustomQueryValidateImportTable,"
    " incr_validation_method, mappers, soft_delete_during_merge,"
    " sqoop_use_generated_sql, sqoop_allow_text_splitter, force_string,"
    " invalidate_impala, mergeCompactionMethod, create_foreign_keys, copy_slave"
)


def test_update_import_table_creates_a_table_with_defaults(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    response = client.post(
        "/import/table/salesdb/orders", json=WRITE_MINIMAL, headers=auth_headers
    )

    assert response.status_code == 200
    row = db_connection.execute(
        sa.text(f"SELECT {DEFAULTED_COLUMNS} FROM import_tables")
    ).one()
    assert tuple(row) == (
        1,
        1,
        "rowCount",
        "query",
        -1,
        1,
        "full",
        -1,
        0,
        -1,
        0,
        -1,
        -1,
        "default",
        -1,
        0,
    )


WRITE_FULL = WRITE_MINIMAL | {
    "importPhaseType": "incr",
    "etlPhaseType": "merge",
    "importTool": "sqoop",
    "lastUpdateFromSource": "2026-01-15 09:00:00",
    "sqlWhereAddition": "a = 1",
    "nomergeIngestionSqlAddition": "b = 2",
    "includeInAirflow": False,
    "airflowPriority": 5,
    "validateImport": False,
    "validationMethod": "customQuery",
    "validateSource": "sqoop",
    "validateDiffAllowed": 7,
    "validationCustomQuerySourceSQL": "select 1",
    "validationCustomQueryHiveSQL": "select 2",
    "validationCustomQueryValidateImportTable": False,
    "sqlSessions": 3,
    "softDeleteDuringMerge": True,
    "incrMode": "append",
    "incrColumn": "id",
    "incrValidationMethod": "incr",
    "pkColumnOverride": "id",
    "pkColumnOverrideMergeonly": "id2",
    "hiveContainerSize": 2048,
    "splitCount": 4,
    "sparkExecutorMemory": "2g",
    "sparkExecutors": 3,
    "splitByColumn": "id",
    "sqoopCustomQuery": "select x",
    "sqoopOptions": "--opt",
    "sqoopUseGeneratedSql": False,
    "allowTextSplitter": True,
    "forceString": 1,
    "comment": "the orders",
    "datalakeSource": "sales",
    "operatorNotes": "notes",
    "copySlave": True,
    "createForeignKeys": 0,
    "invalidateImpala": 1,
    "customMaxQuery": "select max(x)",
    "mergeCompactionMethod": "minor",
    "sourceTableType": "view",
    "importDatabase": "imp_db",
    "importTable": "imp_tbl",
    "historyDatabase": "hist_db",
    "historyTable": "hist_tbl",
}
WRITTEN_COLUMNS = (
    "dbalias, source_schema, source_table, import_phase_type, etl_phase_type,"
    " import_tool, etl_engine, last_update_from_source, sqoop_sql_where_addition,"
    " nomerge_ingestion_sql_addition, include_in_airflow, airflow_priority,"
    " validate_import, validationMethod, validate_source, validate_diff_allowed,"
    " validationCustomQuerySourceSQL, validationCustomQueryHiveSQL,"
    " validationCustomQueryValidateImportTable, mappers, soft_delete_during_merge,"
    " incr_mode, incr_column, incr_validation_method, pk_column_override,"
    " pk_column_override_mergeonly, hive_merge_heap, hive_split_count,"
    " spark_executor_memory, spark_executors, split_by_column, sqoop_query,"
    " sqoop_options, sqoop_use_generated_sql, sqoop_allow_text_splitter,"
    " force_string, comment, datalake_source, operator_notes, copy_slave,"
    " create_foreign_keys, invalidate_impala, custom_max_query,"
    " mergeCompactionMethod, sourceTableType, import_database, import_table,"
    " history_database, history_table"
)


def test_update_import_table_replaces_every_field_of_an_existing_table(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    _add_table(db_connection, operator_notes="old", sqoop_last_rows=10)

    response = client.post(
        "/import/table/salesdb/orders", json=WRITE_FULL, headers=auth_headers
    )

    assert response.status_code == 200
    row = db_connection.execute(
        sa.text(f"SELECT {WRITTEN_COLUMNS}, sqoop_last_rows FROM import_tables")
    ).one()
    assert tuple(row) == (
        "src_conn",
        "src",
        "orders",
        "incr",
        "merge",
        "sqoop",
        "spark",
        datetime(2026, 1, 15, 9, 0),
        "a = 1",
        "b = 2",
        0,
        5,
        0,
        "customQuery",
        "sqoop",
        7,
        "select 1",
        "select 2",
        0,
        3,
        1,
        "append",
        "id",
        "incr",
        "id",
        "id2",
        2048,
        4,
        "2g",
        3,
        "id",
        "select x",
        "--opt",
        0,
        1,
        1,
        "the orders",
        "sales",
        "notes",
        1,
        0,
        1,
        "select max(x)",
        "minor",
        "view",
        "imp_db",
        "imp_tbl",
        "hist_db",
        "hist_tbl",
        10,
    )


def _write_column(name: str, order: int, **values: object) -> dict[str, object]:
    return {
        "columnName": name,
        "columnOrder": order,
        "sourceColumnName": name.upper(),
        "columnType": "int",
        "sourceColumnType": "int(11)",
        "forceString": -1,
        "includeInImport": True,
        "lastUpdateFromSource": "2026-01-15 09:00:00",
        "anonymizationFunction": "None",
        **values,
    }


def test_update_import_table_updates_known_columns_and_adds_new_ones(
    client: TestClient, auth_headers: dict[str, str], db_connection: sa.Connection
) -> None:
    table_id = _add_table(db_connection)
    _add_column(db_connection, table_id, "id", 1)
    body = WRITE_MINIMAL | {
        "columns": [
            _write_column(
                "id",
                1,
                columnNameOverride=" ",
                columnTypeOverride="bigint",
                sourcePrimaryKey=1,
                comment="the key",
            ),
            _write_column(
                "note",
                2,
                columnType="string",
                sourceDatabaseType="mysql",
                columnNameOverride="remark",
                columnTypeOverride="",
                sqoopColumnType="String",
                sqoopColumnTypeOverride="Integer",
                forceString=1,
                includeInImport=False,
                operatorNotes="notes",
                anonymizationFunction="Hash",
            ),
        ]
    }

    response = client.post(
        "/import/table/salesdb/orders", json=body, headers=auth_headers
    )

    assert response.status_code == 200
    rows = db_connection.execute(
        sa.text(
            "SELECT table_id, hive_db, hive_table, column_name, column_order,"
            " source_column_name, column_type, source_column_type,"
            " source_database_type, column_name_override, column_type_override,"
            " sqoop_column_type, sqoop_column_type_override, force_string,"
            " include_in_import, source_primary_key, last_update_from_source,"
            " comment, operator_notes, anonymization_function"
            " FROM import_columns ORDER BY column_order"
        )
    )
    synced = datetime(2026, 1, 15, 9, 0)
    assert [tuple(row) for row in rows] == [
        (
            table_id,
            "salesdb",
            "orders",
            "id",
            1,
            "ID",
            "int",
            "int(11)",
            None,
            None,
            "bigint",
            None,
            None,
            -1,
            1,
            1,
            synced,
            "the key",
            None,
            "None",
        ),
        (
            table_id,
            "salesdb",
            "orders",
            "note",
            2,
            "NOTE",
            "string",
            "int(11)",
            "mysql",
            "remark",
            None,
            "String",
            "Integer",
            1,
            0,
            None,
            synced,
            None,
            "notes",
            "Hash",
        ),
    ]
