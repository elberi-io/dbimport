"""Basic behaviour of the REST API in dbimport.server.rest_server."""

import importlib
import sys
from collections.abc import Iterator
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
