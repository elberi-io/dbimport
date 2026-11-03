import os
import subprocess
import sys
from pathlib import Path

import pytest

BIN = Path(__file__).resolve().parents[2] / "bin"


def _run(launcher: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(BIN / launcher), *args],
        capture_output=True,
        text=True,
        timeout=60,
        env=os.environ.copy(),
        check=False,
    )


# restServer and mainServer are left out: they connect to the
# configuration database when they import their server modules.
@pytest.mark.parametrize(
    "launcher", ["copy", "deploy", "export", "import", "manage", "setup"]
)
def test_launcher_help_prints_options_and_exits_with_1(launcher: str) -> None:
    result = _run(launcher, "--help")

    assert result.returncode == 1
    assert "Options:" in result.stdout
