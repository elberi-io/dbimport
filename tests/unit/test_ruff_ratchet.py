"""The ruff findings in modules listed under per-file-ignores can only go down.

`per-file-ignores` in pyproject.toml lets existing modules keep the
findings they had when the rule set was introduced (#159). ruff then
reports nothing for those codes in those modules, so these tests check
them without the ignores:

- every listed code still has a finding in its module, so a module's
  entry shrinks as it is fixed;
- the number of findings per module and code equals the baseline in
  tests/fixtures/ruff_baseline.json, so new findings fail and fixed ones
  must be locked in by updating the baseline.

The baseline is generated. After fixing findings, regenerate it with
DBIMPORT_UPDATE_RUFF_BASELINE=1 and review the diff.
"""

import json
import os
import subprocess
import sys
import tomllib
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE_FILE = REPO_ROOT / "tests" / "fixtures" / "ruff_baseline.json"
UPDATE_BASELINE = os.environ.get("DBIMPORT_UPDATE_RUFF_BASELINE") == "1"

with (REPO_ROOT / "pyproject.toml").open("rb") as pyproject:
    IGNORES: dict[str, list[str]] = tomllib.load(pyproject)["tool"]["ruff"]["lint"][
        "per-file-ignores"
    ]

Findings = Counter[tuple[str, str]]


def _findings_without_per_file_ignores() -> Findings:
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
    return Counter(
        (Path(f["filename"]).relative_to(REPO_ROOT).as_posix(), f["code"])
        for f in json.loads(result.stdout)
    )


def _write_baseline(findings: Findings) -> None:
    nested: dict[str, dict[str, int]] = {}
    for (path, code), count in sorted(findings.items()):
        nested.setdefault(path, {})[code] = count
    BASELINE_FILE.write_text(json.dumps(nested, indent=2) + "\n", encoding="utf-8")


def _read_baseline() -> Findings:
    nested = json.loads(BASELINE_FILE.read_text(encoding="utf-8"))
    return Counter(
        {
            (path, code): count
            for path, codes in nested.items()
            for code, count in codes.items()
        }
    )


def test_per_file_ignores_lists_only_codes_with_findings() -> None:
    findings = _findings_without_per_file_ignores()

    stale = sorted(
        (path, code)
        for path, codes in IGNORES.items()
        for code in codes
        if findings[(path, code)] == 0
    )

    assert stale == [], f"remove from per-file-ignores in pyproject.toml: {stale}"


def test_per_file_ignores_finding_counts_match_baseline() -> None:
    current = _findings_without_per_file_ignores()
    if UPDATE_BASELINE:
        _write_baseline(current)

    baseline = _read_baseline()

    changed = {
        key: (baseline[key], current[key])
        for key in sorted(baseline.keys() | current.keys())
        if baseline[key] != current[key]
    }
    assert changed == {}, (
        "findings per module and code changed, as (baseline, now). Fix new "
        "findings; after fixing old ones, regenerate the baseline with "
        "DBIMPORT_UPDATE_RUFF_BASELINE=1: "
        f"{changed}"
    )
