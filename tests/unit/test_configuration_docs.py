"""The key table in docs/configuration.rst lists config_keys.KEYS (ADR-0003).

The table is generated. After changing config_keys, regenerate it with
DBIMPORT_UPDATE_CONFIG_DOCS=1 and review the diff.
"""

import os
from pathlib import Path

from dbimport.config_db.config_keys import KEYS

DOCS_FILE = Path(__file__).resolve().parents[2] / "docs" / "configuration.rst"
UPDATE_DOCS = os.environ.get("DBIMPORT_UPDATE_CONFIG_DOCS") == "1"
START = (
    ".. The table below is generated from dbimport.config_db.config_keys by\n"
    ".. tests/unit/test_configuration_docs.py; do not edit it here.\n"
)
END = ".. End of the generated table.\n"


def _table() -> str:
    rows = [(key.name, key.column, key.description) for key in KEYS]
    rows.sort()
    widths = [max(len(row[i]) for row in rows) for i in range(3)]
    rule = " ".join("=" * width for width in widths)
    lines = [
        " ".join(
            cell.ljust(width) for cell, width in zip(row, widths, strict=True)
        ).rstrip()
        for row in rows
    ]
    return "\n".join([rule, *lines, rule]) + "\n"


def test_configuration_docs_key_table_matches_config_keys() -> None:
    text = DOCS_FILE.read_text(encoding="utf-8")
    head, start, rest = text.partition(START)
    table, end, tail = rest.partition(END)
    assert start and end, f"generated table markers not found in {DOCS_FILE}"
    expected = "\n" + _table() + "\n"

    if UPDATE_DOCS:
        DOCS_FILE.write_text(head + START + expected + END + tail, encoding="utf-8")

    assert table == expected, (
        f"{DOCS_FILE} is stale; regenerate it with DBIMPORT_UPDATE_CONFIG_DOCS=1"
    )
