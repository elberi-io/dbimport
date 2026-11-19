"""Basic behaviour of the REST API in dbimport.server.rest_server."""

import importlib
import sys
from collections.abc import Iterator
from datetime import timedelta
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
