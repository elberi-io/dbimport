import os
import subprocess
import sys
from pathlib import Path

import sqlalchemy as sa

from tests.configdb import insert_row

MANAGE = Path(__file__).resolve().parents[2] / "bin" / "manage"


def test_manage_encrypt_instance_aborted_input_removes_temp_dir(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    insert_row(
        db_connection,
        "dbimport_instances",
        name="remote",
        db_hostname="remote.example",
        db_port=3306,
        db_database="DBImport",
        hdfs_address="hdfs://remote",
        hdfs_basedir="/apps/dbimport",
    )
    process = subprocess.Popen(
        [sys.executable, str(MANAGE), "--encryptInstance"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    )
    temp_dir = Path(f"/tmp/dbimport.{process.pid}.tmp")

    # Input ends after the instance name, so the username prompt raises EOFError.
    _stdout, stderr = process.communicate("remote\n", timeout=120)

    assert process.returncode == 1
    assert "EOFError" in stderr
    assert not temp_dir.exists()
