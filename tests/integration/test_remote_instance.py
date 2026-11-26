"""The connection to a remote DBImport instance, as copy and deploy open
it. The second database of the compose server, with the schema the ORM
creates, stands in for the configuration database of the remote
instance."""

from collections.abc import Iterator

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import close_all_sessions

from dbimport.operations import copy_operations
from tests.configdb import insert_row
from tests.integration.conftest import ORM_DATABASE

INSTANCE = "remote"


@pytest.fixture(autouse=True)
def _close_sessions() -> Iterator[None]:
    """copy_operations leaves its sessions open, and with them a
    transaction; a command ends them with its process, a test here."""
    yield
    close_all_sessions()


@pytest.fixture
def remote(
    clean_config_db: None,
    engine: sa.Engine,
    db_connection: sa.Connection,
    orm_db_connection: sa.Connection,
) -> Iterator[sa.Connection]:
    """The remote instance, at the schema version of the local one, without
    stored credentials."""
    version = db_connection.execute(
        sa.text("SELECT version_num FROM alembic_version")
    ).scalar_one()
    insert_row(orm_db_connection, "alembic_version", version_num=version)
    insert_row(
        db_connection,
        "dbimport_instances",
        name=INSTANCE,
        db_hostname=engine.url.host,
        db_port=engine.url.port,
        db_database=ORM_DATABASE,
        hdfs_address="hdfs://remote:8020",
        hdfs_basedir="/apps/remote",
    )
    yield orm_db_connection


def _save_credentials(username: str, password: str) -> None:
    copy_operations.Operation().encrypt_user_password(
        instance=INSTANCE, username=username, password=password
    )


def _save_test_user(engine: sa.Engine) -> None:
    assert engine.url.username is not None
    assert engine.url.password is not None
    _save_credentials(engine.url.username, engine.url.password)


def test_connect_remote_dbimport_instance_with_the_stored_user_connects(
    remote: sa.Connection, engine: sa.Engine
) -> None:
    _save_test_user(engine)
    copy = copy_operations.Operation()

    assert copy.connect_remote_dbimport_instance(instance=INSTANCE) is True


@pytest.mark.parametrize(
    "problem",
    ["no-credentials", "not-encrypted", "wrong-password", "unreachable"],
)
def test_connect_remote_dbimport_instance_without_a_working_login_fails(
    remote: sa.Connection,
    engine: sa.Engine,
    db_connection: sa.Connection,
    problem: str,
) -> None:
    assert engine.url.username is not None
    assert engine.url.password is not None
    if problem == "not-encrypted":
        db_connection.execute(
            sa.text("UPDATE dbimport_instances SET db_credentials = 'user pw'")
        )
    elif problem == "wrong-password":
        _save_credentials(engine.url.username, engine.url.password + "x")
    elif problem == "unreachable":
        _save_test_user(engine)
        db_connection.execute(sa.text("UPDATE dbimport_instances SET db_port = 1"))
    copy = copy_operations.Operation()

    assert copy.connect_remote_dbimport_instance(instance=INSTANCE) is False


def test_connect_remote_dbimport_instance_of_an_unknown_instance_raises(
    remote: sa.Connection,
) -> None:
    copy = copy_operations.Operation()

    with pytest.raises(sa.exc.NoResultFound):
        copy.connect_remote_dbimport_instance(instance="nope")


def test_check_remote_instance_schema_at_the_same_version_returns(
    remote: sa.Connection, engine: sa.Engine
) -> None:
    _save_test_user(engine)
    copy = copy_operations.Operation()

    assert copy.check_remote_instance_schema(remote_instance=INSTANCE) is None


@pytest.mark.parametrize(
    "problem", ["other-version", "no-credentials", "unknown-instance"]
)
def test_check_remote_instance_schema_exits_when_it_cannot_confirm_the_version(
    remote: sa.Connection, engine: sa.Engine, problem: str
) -> None:
    if problem != "no-credentials":
        _save_test_user(engine)
    if problem == "other-version":
        remote.execute(sa.text("UPDATE alembic_version SET version_num = 'other'"))
    copy = copy_operations.Operation()

    with pytest.raises(SystemExit) as exit_info:
        copy.check_remote_instance_schema(
            remote_instance="nope" if problem == "unknown-instance" else INSTANCE
        )

    assert exit_info.value.code == 1
