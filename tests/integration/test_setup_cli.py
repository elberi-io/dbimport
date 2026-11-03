import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

SETUP = Path(__file__).resolve().parents[2] / "bin" / "setup"


def test_setup_downgrade_db_fails_because_the_method_is_missing(
    clean_config_db: None,
) -> None:
    # Known bug: bin/setup calls initialize.downgradeDB(), which does not
    # exist. This records the current behaviour. It is not an expected
    # failure on purpose: a fixed command would downgrade the test schema.
    result = subprocess.run(
        [sys.executable, str(SETUP), "--downgradeDB"],
        capture_output=True,
        text=True,
        timeout=120,
        env=os.environ.copy(),
        check=False,
    )

    assert result.returncode == 1
    assert "AttributeError" in result.stderr
    assert "downgradeDB" in result.stderr
