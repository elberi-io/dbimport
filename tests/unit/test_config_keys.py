"""The configuration table keys (dbimport.config_db.config_keys, ADR-0003)."""

from dbimport.config_db.config_keys import BY_NAME, KEYS


def test_initial_value_secret_key_is_new_for_every_installation() -> None:
    key = BY_NAME["restserver_secret_key"]

    first, second = key.initial_value(), key.initial_value()

    assert first != second
    assert isinstance(first, str)
    assert len(first) == 64


def test_keys_names_are_unique() -> None:
    names = [key.name for key in KEYS]

    assert len(set(names)) == len(names)
