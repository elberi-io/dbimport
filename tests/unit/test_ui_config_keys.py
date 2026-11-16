"""The UI knows the configuration keys the REST API shows (config_keys, ADR-0003).

The UI keeps its own copies of the keys until its types come from the
OpenAPI schema; a key added to config_keys fails here until the UI has it.
"""

import json
import re
from pathlib import Path

import pytest

from dbimport.config_db.config_keys import KEYS

UI_SRC = Path(__file__).resolve().parents[2] / "ui" / "src"
REST_KEYS = {key.name for key in KEYS if key.rest}


def _interface_fields(name: str) -> set[str]:
    text = (UI_SRC / "utils" / "interfaces.tsx").read_text(encoding="utf-8")
    body = re.search(rf"^export interface {name} \{{\n(.*?)^\}}", text, re.M | re.S)
    assert body, f"interface {name} not found"
    return set(re.findall(r"^  (\w+)\??:", body.group(1), re.M))


@pytest.mark.parametrize("interface", ["ConfigGlobal", "ConfigGlobalWithIndex"])
def test_ui_config_interface_has_a_field_for_every_rest_key(interface: str) -> None:
    assert _interface_fields(interface) == REST_KEYS


def test_ui_info_texts_have_a_text_for_every_rest_key() -> None:
    texts = json.loads((UI_SRC / "infoTexts.json").read_text(encoding="utf-8"))

    assert set(texts["configuration"]["global"]) == REST_KEYS
