"""Connections (jdbc_connections) as the command line programs and the Atlas
discovery read and write them through common_config."""

from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import pendulum
import pytest
import sqlalchemy as sa
from Crypto.PublicKey import RSA

from dbimport.common.exceptions import InvalidConfigurationError
from dbimport.config_db import common_config
from tests.configdb import insert_row, set_driver_path

DBALIAS = "test_mysql"
JDBC_URL = "jdbc:mysql://source.example:3306/sourcedb"


def _add_connection(conn: sa.Connection, **values: object) -> None:
    set_driver_path(conn, "MySQL")
    insert_row(conn, "jdbc_connections", dbalias=DBALIAS, jdbc_url=JDBC_URL, **values)


def _key_pair(directory: Path) -> dict[str, str]:
    key = RSA.generate(2048)
    private_key = directory / "private.pem"
    public_key = directory / "public.pem"
    private_key.write_bytes(key.export_key())
    public_key.write_bytes(key.public_key().export_key())
    return {"private_key_path": str(private_key), "public_key_path": str(public_key)}


def _freeze_clock(monkeypatch: pytest.MonkeyPatch, utc: datetime) -> None:
    now = pendulum.instance(utc, tz="UTC")
    monkeypatch.setattr(pendulum, "now", lambda tz="UTC": now.in_timezone(tz or "UTC"))


@pytest.mark.parametrize(("dbalias", "expected"), [(DBALIAS, True), ("other", False)])
def test_check_connection_alias_returns_whether_the_connection_exists(
    clean_config_db: None, db_connection: sa.Connection, dbalias: str, expected: bool
) -> None:
    _add_connection(db_connection)

    assert common_config.Config().check_connection_alias(dbalias) is expected


def test_encrypt_user_password_for_unknown_connection_raises(
    clean_config_db: None,
) -> None:
    with pytest.raises(InvalidConfigurationError):
        common_config.Config().encrypt_user_password(DBALIAS, "user", "secret")


def test_lookup_connection_alias_with_own_key_pair_decrypts_credentials(
    clean_config_db: None, db_connection: sa.Connection, tmp_path: Path
) -> None:
    _add_connection(db_connection, **_key_pair(tmp_path))
    common_config.Config().encrypt_user_password(DBALIAS, "user", "secret")

    config = common_config.Config()
    config.lookup_connection_alias(DBALIAS)

    assert (config.jdbc_username, config.jdbc_password) == ("user", "secret")


@pytest.mark.parametrize(
    "use_credentials",
    [
        lambda config: config.encrypt_user_password(DBALIAS, "user", "secret"),
        lambda config: config.lookup_connection_alias(DBALIAS),
    ],
    ids=["encrypt", "lookup"],
)
def test_credentials_with_missing_own_key_file_raise(
    clean_config_db: None,
    db_connection: sa.Connection,
    tmp_path: Path,
    use_credentials: Callable[[common_config.Config], None],
) -> None:
    _add_connection(
        db_connection,
        private_key_path=str(tmp_path / "missing_private.pem"),
        public_key_path=str(tmp_path / "missing_public.pem"),
    )

    with pytest.raises(InvalidConfigurationError):
        use_credentials(common_config.Config())


def test_lookup_connection_alias_reads_environment_and_url(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    _add_connection(db_connection, environment="Production")

    config = common_config.Config()
    config.lookup_connection_alias(DBALIAS, decrypt_credentials=False)

    assert (config.jdbc_environment, config.jdbc_hostname, config.jdbc_username) == (
        "Production",
        "source.example",
        None,
    )


def test_lookup_connection_alias_for_unknown_connection_raises(
    clean_config_db: None,
) -> None:
    with pytest.raises(InvalidConfigurationError):
        common_config.Config().lookup_connection_alias(DBALIAS)


def test_lookup_connection_alias_with_given_url_does_not_need_the_row(
    clean_config_db: None,
) -> None:
    config = common_config.Config()
    config.lookup_connection_alias(
        "not_stored", decrypt_credentials=False, jdbc_url=JDBC_URL
    )

    assert config.jdbc_hostname == "source.example"


@pytest.mark.parametrize(
    ("start", "stop", "timezone", "now", "expected"),
    [
        (None, None, None, datetime(2026, 1, 15, 12, 0), True),
        ("08:00:00", "10:00:00", "UTC", datetime(2026, 1, 15, 9, 0), True),
        ("08:00:00", "10:00:00", "UTC", datetime(2026, 1, 15, 11, 0), False),
        ("22:00:00", "02:00:00", "UTC", datetime(2026, 1, 15, 23, 0), True),
        ("22:00:00", "02:00:00", "UTC", datetime(2026, 1, 15, 1, 0), True),
        ("22:00:00", "02:00:00", "UTC", datetime(2026, 1, 15, 12, 0), False),
        ("24:00:00", "02:00:00", "UTC", datetime(2026, 1, 15, 1, 0), True),
        ("08:00:00", None, "UTC", datetime(2026, 1, 15, 9, 0), False),
        ("08:00:00", "08:00:00", "UTC", datetime(2026, 1, 15, 9, 0), False),
        ("08:00:00", "10:00:00", "Asia/Tokyo", datetime(2026, 1, 15, 0, 30), True),
        ("08:00:00", "10:00:00", None, datetime(2026, 1, 15, 0, 30), False),
    ],
)
def test_check_time_window_in_atlas_mode_returns_whether_now_is_inside(
    clean_config_db: None,
    db_connection: sa.Connection,
    monkeypatch: pytest.MonkeyPatch,
    start: str | None,
    stop: str | None,
    timezone: str | None,
    now: datetime,
    expected: bool,
) -> None:
    # Without a time zone on the connection, the configured one (UTC) applies.
    _add_connection(
        db_connection,
        timewindow_start=start,
        timewindow_stop=stop,
        timewindow_timezone=timezone,
    )
    _freeze_clock(monkeypatch, now)

    allowed = common_config.Config().check_time_window(
        DBALIAS, atlas_discovery_mode=True
    )

    assert allowed is expected


def test_get_atlas_jdbc_connection_data_returns_empty_text_for_missing_details(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    _add_connection(
        db_connection,
        atlas_include_filter="sales.*",
        atlas_last_discovery="2026-01-15 09:00:00",
    )

    data = common_config.Config().get_atlas_jdbc_connection_data(DBALIAS)

    assert data == {
        "contact_info": "",
        "description": "",
        "owner": "",
        "atlas_discovery": 1,
        "atlas_include_filter": "sales.*",
        "atlas_exclude_filter": None,
        "atlas_last_discovery": datetime(2026, 1, 15, 9, 0),
    }


def test_get_atlas_jdbc_connection_data_for_unknown_connection_raises(
    clean_config_db: None,
) -> None:
    with pytest.raises(InvalidConfigurationError):
        common_config.Config().get_atlas_jdbc_connection_data(DBALIAS)


@pytest.mark.parametrize(
    ("seed_file_text", "expected"),
    [(None, "test-seed"), ("0123456789abcdefXYZ", "0123456789abcdef")],
)
def test_get_anonymization_seed_uses_seed_file_of_connection_up_to_16_characters(
    clean_config_db: None,
    db_connection: sa.Connection,
    tmp_path: Path,
    seed_file_text: str | None,
    expected: str,
) -> None:
    seed_file = None
    if seed_file_text is not None:
        seed_file = tmp_path / "seed"
        seed_file.write_text(seed_file_text)
    _add_connection(db_connection, seed_file=seed_file and str(seed_file))
    config = common_config.Config()
    config.lookup_connection_alias(DBALIAS, decrypt_credentials=False)

    assert config.get_anonymization_seed() == expected
