"""Connection settings of the configuration database (dbimport.config_db.engine)."""

import re
from pathlib import Path

import pytest
from sqlalchemy import make_url

from dbimport.common.exceptions import InvalidConfigurationError
from dbimport.config_db import engine
from dbimport.settings import Settings, load_settings

TEMPLATE = Path(__file__).resolve().parents[1] / "fixtures" / "dbimport.test.cfg"


def _settings(tmp_path: Path, **changes: str) -> Settings:
    """Settings of the test configuration with `key = value` lines changed."""
    text = TEMPLATE.read_text()
    for key, value in changes.items():
        text, count = re.subn(rf"(?m)^{key}\s*=.*$", f"{key} = {value}", text)
        assert count == 1, key
    path = tmp_path / "dbimport.cfg"
    path.write_text(text)
    return load_settings(path)


def test_url_password_with_reserved_characters_survives_the_url_string(
    tmp_path: Path,
) -> None:
    # The configuration file writes a "%" as "%%".
    settings = _settings(tmp_path, mysql_password="p@ss:w/rd%%40?#")

    url = engine.url(engine.credentials(settings))

    assert make_url(url.render_as_string(hide_password=False)).password == (
        "p@ss:w/rd%40?#"
    )


@pytest.mark.parametrize(
    ("username", "password"), [("", "secret"), ("dbimport", ""), ("", "")]
)
def test_credentials_without_user_or_password_and_aws_region_are_rejected(
    tmp_path: Path, username: str, password: str
) -> None:
    settings = _settings(tmp_path, mysql_username=username, mysql_password=password)

    with pytest.raises(InvalidConfigurationError):
        engine.credentials(settings)


def test_credentials_with_aws_region_but_without_secret_name_are_rejected(
    tmp_path: Path,
) -> None:
    settings = _settings(
        tmp_path, mysql_username="", mysql_password="", region="eu-north-1"
    )

    with pytest.raises(InvalidConfigurationError):
        engine.credentials(settings)
