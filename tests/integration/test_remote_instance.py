"""The connection to a remote DBImport instance, as copy, deploy and the
server open it, and the server's record of a finished copy there. The
second database of the compose server, with the schema the ORM creates,
stands in for the configuration database of the remote instance."""

from collections.abc import Iterator
from datetime import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session, close_all_sessions

from dbimport.common.exceptions import InvalidConfigurationError
from dbimport.config_db import engine as config_engine
from dbimport.config_db import instances
from dbimport.config_db.repositories.copies import CopiesRepository, InstanceConnection
from dbimport.operations import copy_operations
from dbimport.server import daemon
from dbimport.settings import get_settings
from tests.configdb import ORM_DATABASE, insert_row

INSTANCE = "remote"
NOW = datetime(2026, 3, 4, 5, 6, 7)


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
    "problem",
    ["other-version", "no-version", "no-credentials", "unknown-instance"],
)
def test_check_remote_instance_schema_exits_when_it_cannot_confirm_the_version(
    remote: sa.Connection, engine: sa.Engine, problem: str
) -> None:
    if problem != "no-credentials":
        _save_test_user(engine)
    if problem == "other-version":
        remote.execute(sa.text("UPDATE alembic_version SET version_num = 'other'"))
    elif problem == "no-version":
        remote.execute(sa.text("DELETE FROM alembic_version"))
    copy = copy_operations.Operation()

    with pytest.raises(SystemExit) as exit_info:
        copy.check_remote_instance_schema(
            remote_instance="nope" if problem == "unknown-instance" else INSTANCE
        )

    assert exit_info.value.code == 1


def test_connect_remote_dbimport_instance_takes_the_password_literally(
    remote: sa.Connection, engine: sa.Engine
) -> None:
    assert engine.url.username is not None
    assert engine.url.password is not None
    real = engine.url.password
    _save_credentials(engine.url.username, f"%{ord(real[0]):02X}{real[1:]}")
    copy = copy_operations.Operation()

    assert copy.connect_remote_dbimport_instance(instance=INSTANCE) is False


@pytest.mark.parametrize("extra", [" more", ""], ids=["with-a-space", "empty"])
def test_connect_remote_dbimport_instance_takes_the_whole_password_after_the_user(
    remote: sa.Connection, engine: sa.Engine, extra: str
) -> None:
    assert engine.url.username is not None
    assert engine.url.password is not None
    password = engine.url.password + extra if extra else ""
    _save_credentials(engine.url.username, password)
    copy = copy_operations.Operation()

    assert copy.connect_remote_dbimport_instance(instance=INSTANCE) is False


def _connection() -> InstanceConnection:
    with Session(config_engine.engine()) as session:
        return CopiesRepository(session).instance_connection(INSTANCE)


def test_instance_engine_opens_the_configuration_database_of_the_instance(
    remote: sa.Connection, engine: sa.Engine
) -> None:
    _save_test_user(engine)
    copy = copy_operations.Operation()

    remote_engine = instances.instance_engine(_connection(), copy.crypto)
    try:
        with remote_engine.connect() as conn:
            database = conn.execute(sa.text("SELECT DATABASE()")).scalar_one()
    finally:
        remote_engine.dispose()

    assert database == ORM_DATABASE


@pytest.mark.parametrize(
    ("problem", "error"),
    [
        ("no-credentials", InvalidConfigurationError),
        ("not-encrypted", InvalidConfigurationError),
        ("public-key-as-private-key", InvalidConfigurationError),
        ("no-password", InvalidConfigurationError),
        ("wrong-password", sa.exc.DBAPIError),
    ],
)
def test_instance_engine_without_a_working_login_raises(
    remote: sa.Connection,
    engine: sa.Engine,
    db_connection: sa.Connection,
    problem: str,
    error: type[Exception],
) -> None:
    assert engine.url.username is not None
    assert engine.url.password is not None
    if problem == "not-encrypted":
        db_connection.execute(
            sa.text("UPDATE dbimport_instances SET db_credentials = 'user pw'")
        )
    elif problem == "no-password":
        _save_credentials(engine.url.username, "")
    elif problem == "wrong-password":
        _save_credentials(engine.url.username, engine.url.password + "x")
    copy = copy_operations.Operation()
    if problem == "public-key-as-private-key":
        _save_test_user(engine)
        copy.crypto.set_private_key_file(get_settings().credentials.public_key)

    with pytest.raises(error):
        instances.instance_engine(_connection(), copy.crypto)


def _add_remote_table(remote: sa.Connection) -> None:
    insert_row(
        remote,
        "import_tables",
        hive_db="db",
        hive_table="orders",
        dbalias="source",
        source_schema="src",
        source_table="orders",
    )


def _copy_finished(remote: sa.Connection) -> list[datetime | None]:
    return list(
        remote.execute(
            sa.text("SELECT copy_finished FROM import_tables ORDER BY table_id")
        ).scalars()
    )


def test_finish_copy_records_the_time_on_the_remote_table(
    remote: sa.Connection,
) -> None:
    _add_remote_table(remote)

    with Session(remote.engine) as remote_session:
        daemon.finish_copy(remote_session, "db", "orders", NOW)

    assert _copy_finished(remote) == [NOW]


def test_finish_copy_of_a_table_missing_on_the_remote_raises(
    remote: sa.Connection,
) -> None:
    _add_remote_table(remote)

    with Session(remote.engine) as remote_session, pytest.raises(sa.exc.NoResultFound):
        daemon.finish_copy(remote_session, "db", "other", NOW)

    assert _copy_finished(remote) == [None]
