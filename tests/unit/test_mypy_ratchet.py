"""The mypy errors in bin/ can only go down.

The existing code in bin/ (packages and launchers) has almost no type
annotations. mypy checks it with the settings of its override in
pyproject.toml (#202), and the errors per module and error code must equal
tests/fixtures/mypy_baseline.json: a module that is not listed there must
have none, a count that grows fails, and a count that drops must be
locked in by updating the baseline.

The baseline is generated. After fixing errors, regenerate it with
DBIMPORT_UPDATE_MYPY_BASELINE=1 and review the diff.
"""

import json
import os
import subprocess
import sys
import tomllib
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE_FILE = REPO_ROOT / "tests" / "fixtures" / "mypy_baseline.json"
UPDATE_BASELINE = os.environ.get("DBIMPORT_UPDATE_MYPY_BASELINE") == "1"

with (REPO_ROOT / "pyproject.toml").open("rb") as pyproject:
    # The launchers are the files ruff includes without a .py extension.
    LAUNCHERS: list[str] = tomllib.load(pyproject)["tool"]["ruff"]["extend-include"]

Findings = Counter[tuple[str, str]]


def _run_mypy(*args: str) -> Findings:
    result = subprocess.run(
        [sys.executable, "-m", "mypy", "--output", "json", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    # 0: no errors, 1: errors found; anything else means mypy itself failed.
    assert result.returncode in (0, 1), result.stderr
    errors: Findings = Counter()
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        error = json.loads(line)
        if error["severity"] == "error":
            errors[(Path(error["file"]).as_posix(), str(error["code"]))] += 1
    return errors


def _mypy_errors() -> Findings:
    # A launcher named like a standard library module (bin/copy) would shadow
    # it as a module, so it is checked on its own, as __main__.
    shadowing = [
        path for path in LAUNCHERS if Path(path).name in sys.stdlib_module_names
    ]
    others = [path for path in LAUNCHERS if path not in shadowing]
    errors = _run_mypy("--scripts-are-modules", "bin", *others)
    for path in shadowing:
        errors += _run_mypy(path)
    return errors


def _write_baseline(errors: Findings) -> None:
    nested: dict[str, dict[str, int]] = {}
    for (path, code), count in sorted(errors.items()):
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


def test_mypy_errors_in_bin_match_baseline() -> None:
    current = _mypy_errors()
    if UPDATE_BASELINE:
        _write_baseline(current)

    baseline = _read_baseline()

    changed = {
        key: (baseline[key], current[key])
        for key in sorted(baseline.keys() | current.keys())
        if baseline[key] != current[key]
    }
    assert changed == {}, (
        "mypy errors per module and code changed, as (baseline, now). Fix new "
        "errors; after fixing old ones, regenerate the baseline with "
        "DBIMPORT_UPDATE_MYPY_BASELINE=1: "
        f"{changed}"
    )
