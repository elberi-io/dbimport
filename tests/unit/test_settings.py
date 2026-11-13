"""Typed settings from the configuration file (dbimport.settings, ADR-0002)."""

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from dbimport.settings import load_settings

TEMPLATE = Path(__file__).resolve().parents[1] / "fixtures" / "dbimport.test.cfg"


def _config(tmp_path: Path, **changes: str | None) -> Path:
    """The test configuration with `key = value` lines changed (None removes the key)."""
    text = TEMPLATE.read_text()
    for key, value in changes.items():
        pattern = rf"(?mi)^{re.escape(key)}\s*=.*$\n?"
        assert re.search(pattern, text), key
        text = re.sub(pattern, "" if value is None else f"{key} = {value}\n", text)
    path = tmp_path / "dbimport.cfg"
    path.write_text(text)
    return path


def test_load_settings_returns_typed_values(tmp_path: Path) -> None:
    settings = load_settings(_config(tmp_path, mysql_port="3307", use_ssl="true"))

    assert settings.database.mysql_port == 3307
    assert settings.hive.use_ssl is True
    assert settings.database.mysql_password.get_secret_value() == "dbimport"
    assert "dbimport" not in repr(settings.database.mysql_password)


def test_load_settings_environment_overrides_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DBIMPORT_DATABASE__MYSQL_PORT", "4406")

    settings = load_settings(_config(tmp_path))

    assert settings.database.mysql_port == 4406


def test_load_settings_missing_keys_are_reported_together(tmp_path: Path) -> None:
    path = _config(tmp_path, mysql_hostname=None, seed=None)

    with pytest.raises(ValidationError) as error:
        load_settings(path)

    missing = {err["loc"] for err in error.value.errors()}
    assert missing == {("database", "mysql_hostname"), ("anonymization", "seed")}


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("mysql_port", "abc"),
        ("restServer_workers", "four"),
        ("use_ssl", "maybe"),
        ("spark_major_version", "5"),
        ("airflow_mode", "cloud"),
        ("authentication_type", "ldap"),
    ],
)
def test_load_settings_invalid_value_fails(
    tmp_path: Path, key: str, value: str
) -> None:
    with pytest.raises(ValidationError):
        load_settings(_config(tmp_path, **{key: value}))


def test_load_settings_optional_keys_get_defaults(tmp_path: Path) -> None:
    path = _config(
        tmp_path,
        spark_major_version=None,
        packages=None,
        airflow_mode=None,
        region=None,
        aws_secret_name=None,
        driver=None,
    )

    settings = load_settings(path)

    assert settings.spark.spark_major_version == 3
    assert settings.spark.packages is None
    assert settings.spark.s3_access_to_ozone is False
    assert settings.airflow.airflow_mode == "default"
    assert settings.aws.region is None
    assert settings.database.aws_secret_name is None
    assert settings.impala.driver is None


def test_load_settings_ozone_access_needs_both_s3_keys(tmp_path: Path) -> None:
    path = _config(
        tmp_path, hive_library="HiveWarehouseSession\ns3_access_to_ozone = true"
    )

    with pytest.raises(ValidationError, match="s3_access_key_id"):
        load_settings(path)


def test_load_settings_authentication_type_ignores_case(tmp_path: Path) -> None:
    settings = load_settings(_config(tmp_path, authentication_type="Kerberos"))

    assert settings.atlas.authentication_type == "kerberos"


def test_get_settings_invalid_file_exits_without_printing_values(
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, mysql_port="abc", mysql_password="hidden-value", seed=None)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from dbimport.settings import get_settings; get_settings()",
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "DBIMPORT_CONFIGFILE": str(path)},
        timeout=60,
        check=False,
    )

    assert result.returncode == 1
    assert "[Database] mysql_port" in result.stdout
    assert "[Anonymization] seed" in result.stdout
    assert "hidden-value" not in result.stdout + result.stderr
