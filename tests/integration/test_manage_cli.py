import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import sqlalchemy as sa

from tests.configdb import insert_row

MANAGE = Path(__file__).resolve().parents[2] / "bin" / "manage"


def _wait_for(path: Path, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.1)
    return path.exists()


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
    with subprocess.Popen(
        [sys.executable, str(MANAGE), "--encryptInstance"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    ) as process:
        temp_dir = Path(f"/tmp/dbimport.{process.pid}.tmp")
        try:
            assert process.stdin is not None
            process.stdin.write("remote\n")
            process.stdin.flush()
            # The command now waits at the username prompt with its temp dir.
            created = _wait_for(temp_dir, 60)
            # Input ends here, so the username prompt raises EOFError.
            _stdout, stderr = process.communicate(timeout=60)
        finally:
            process.kill()
        left_behind = temp_dir.exists()
        shutil.rmtree(temp_dir, ignore_errors=True)

    assert created
    assert process.returncode == 1
    assert "EOFError" in stderr
    assert not left_behind
