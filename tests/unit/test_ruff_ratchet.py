"""The per-file-ignores list in pyproject.toml names only codes that still occur.

A module's entry must shrink when its findings are fixed: a code that no
longer has a finding in its module would otherwise keep allowing new
violations there. ruff itself does not report unused per-file ignores.
"""

import json
import subprocess
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
IGNORES: dict[str, list[str]] = tomllib.loads(
    (REPO_ROOT / "pyproject.toml").read_text()
)["tool"]["ruff"]["lint"]["per-file-ignores"]


def _findings_without_per_file_ignores() -> set[tuple[str, str]]:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--no-cache",
            "--exit-zero",
            "--output-format",
            "json",
            "--config",
            "lint.per-file-ignores = {}",
            *IGNORES,
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )
    return {
        (Path(f["filename"]).relative_to(REPO_ROOT).as_posix(), f["code"])
        for f in json.loads(result.stdout)
    }


def test_per_file_ignores_lists_only_codes_with_findings() -> None:
    findings = _findings_without_per_file_ignores()

    stale = sorted(
        (path, code)
        for path, codes in IGNORES.items()
        for code in codes
        if (path, code) not in findings
    )

    assert stale == [], f"remove from per-file-ignores in pyproject.toml: {stale}"
