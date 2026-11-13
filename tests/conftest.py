"""Test setup shared by all tests.

DBImport reads its configuration file once, when dbimport.config_reader.configuration
is first imported, and exits if DBIMPORT_HOME or the file is missing. The
environment is therefore prepared in pytest_configure, before any test
module imports DBImport code.
"""

import os
import re
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from Crypto.PublicKey import RSA

from tests.configdb import close_dbimport_connections

REPO_ROOT = Path(__file__).resolve().parent.parent
TEST_CONFIG_TEMPLATE = REPO_ROOT / "tests" / "fixtures" / "dbimport.test.cfg"

_WORKDIR_KEY = pytest.StashKey[Path]()


def _replace_setting(text: str, key: str, value: str) -> str:
    """Return text with the value of every `key = ...` line replaced."""
    new_text, count = re.subn(rf"(?m)^{re.escape(key)} = .*$", f"{key} = {value}", text)
    if count == 0:
        raise ValueError(f"'{key}' not found in {TEST_CONFIG_TEMPLATE}")
    return new_text


def _write_test_config(workdir: Path) -> Path:
    """Write the test configuration with a new RSA key pair into workdir.

    DBIMPORT_TEST_DB_HOST overrides the database host of the template,
    for example when the tests run in a container on the compose network.
    """
    key = RSA.generate(2048)
    private_key = workdir / "dbimport_private_key.pem"
    public_key = workdir / "dbimport_public_key.pem"
    private_key.write_bytes(key.export_key())
    public_key.write_bytes(key.publickey().export_key())

    text = TEST_CONFIG_TEMPLATE.read_text()
    text = _replace_setting(text, "private_key", str(private_key))
    text = _replace_setting(text, "public_key", str(public_key))
    host = os.environ.get("DBIMPORT_TEST_DB_HOST")
    if host:
        text = _replace_setting(text, "mysql_hostname", host)

    config_file = workdir / "dbimport.cfg"
    config_file.write_text(text)
    return config_file


def pytest_configure(config: pytest.Config) -> None:
    workdir = Path(tempfile.mkdtemp(prefix="dbimport-tests-"))
    config.stash[_WORKDIR_KEY] = workdir
    os.environ["DBIMPORT_HOME"] = str(REPO_ROOT)
    os.environ["DBIMPORT_CONFIGFILE"] = str(_write_test_config(workdir))


def pytest_unconfigure(config: pytest.Config) -> None:
    workdir = config.stash.get(_WORKDIR_KEY, None)
    if workdir is not None:
        shutil.rmtree(workdir, ignore_errors=True)
    # common_config creates this directory for every process that uses it.
    shutil.rmtree(f"/tmp/dbimport.{os.getpid()}.tmp", ignore_errors=True)


@pytest.fixture(autouse=True)
def _reset_singletons() -> Iterator[None]:
    """Give every test fresh DBImport singletons (dbimport.common.singleton)."""
    yield
    from dbimport.common.singleton import Singleton

    for instance in Singleton._instances.values():
        close_dbimport_connections(instance)
    Singleton._instances.clear()
