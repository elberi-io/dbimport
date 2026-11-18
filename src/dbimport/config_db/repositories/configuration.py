# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""The configuration table: the values of the keys in config_keys.KEYS."""

from collections.abc import Mapping

from sqlalchemy import insert, select, update
from sqlalchemy.orm import InstrumentedAttribute, Session

from dbimport.config_db import config_keys
from dbimport.config_db.config_schema import Configuration

Value = bool | int | str


class ConfigurationError(Exception):
    """A key that is not in config_keys.KEYS, or a stored value that cannot
    be used."""


class UnknownKeyError(ConfigurationError):
    """A key that is not in config_keys.KEYS."""


def _config_key(key: str) -> config_keys.ConfigKey:
    config_key = config_keys.BY_NAME.get(key)
    if config_key is None:
        raise UnknownKeyError(f"There is no configuration with the name '{key}'")
    return config_key


def _value_column(
    config_key: config_keys.ConfigKey,
) -> InstrumentedAttribute[int | None] | InstrumentedAttribute[str | None]:
    if config_key.kind == "str":
        return Configuration.value_str
    return Configuration.value_int


class ConfigurationRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def value(self, key: str) -> Value:
        """The value of key, of the type its ConfigKey names.

        Raises UnknownKeyError when key is not in config_keys.KEYS, and
        ConfigurationError when it has no row, when a str key has an empty
        value, or when an int or bool key has none or a bool key has a value
        other than 0 or 1.
        """
        config_key = _config_key(key)
        row = (
            self._session.execute(
                select(Configuration.value_int, Configuration.value_str).where(
                    Configuration.config_key == key
                )
            )
            .tuples()
            .one_or_none()
        )
        if row is None:
            raise ConfigurationError(
                f"Configuration key '{key}' cant be found. Have you upgraded the "
                "database schema to the latest version?"
            )
        value_int, value_str = row
        if config_key.kind == "str":
            if value_str is None or value_str.strip() == "":
                raise ConfigurationError(
                    f"Configuration Key '{key}' must have a value in 'valueStr'"
                )
            return value_str
        if value_int is None:
            raise ConfigurationError(
                f"Configuration Key '{key}' must have a value in 'valueInt'"
            )
        if config_key.kind == "bool":
            if value_int not in (0, 1):
                raise ConfigurationError(
                    f"Configuration Key '{key}' can only have 0 or 1 in column "
                    "'valueInt'"
                )
            return value_int == 1
        return value_int

    def stored_values(self) -> dict[str, int | str | None]:
        """The stored value of every key of config_keys.KEYS that has a row:
        the int column of a bool or int key (a bool as 0 or 1), the str
        column of a str key."""
        rows = self._session.execute(
            select(
                Configuration.config_key,
                Configuration.value_int,
                Configuration.value_str,
            ).order_by(Configuration.config_key)
        )
        values: dict[str, int | str | None] = {}
        for key, value_int, value_str in rows.tuples():
            config_key = config_keys.BY_NAME.get(key)
            if config_key is not None:
                values[key] = value_str if config_key.kind == "str" else value_int
        return values

    def set_values(self, values: Mapping[str, Value]) -> None:
        """Stores the values of existing rows, a bool as 0 or 1.

        Raises UnknownKeyError for a key that is not in config_keys.KEYS.
        """
        for key, value in values.items():
            config_key = _config_key(key)
            stored = int(value) if config_key.kind == "bool" else value
            self._session.execute(
                update(Configuration)
                .where(Configuration.config_key == key)
                .values({_value_column(config_key): stored})
            )

    def seed(self) -> None:
        """Adds the keys of config_keys.KEYS that have no row, with their
        initial value, and gives every key the description of KEYS. Values
        and rows of other keys stay as they are."""
        descriptions = dict(
            self._session.execute(
                select(Configuration.config_key, Configuration.description)
            )
            .tuples()
            .all()
        )
        for key in config_keys.KEYS:
            if key.name not in descriptions:
                self._session.execute(
                    insert(Configuration).values(
                        {
                            Configuration.config_key: key.name,
                            Configuration.description: key.description,
                            _value_column(key): key.initial_value(),
                        }
                    )
                )
            elif descriptions[key.name] != key.description:
                self._session.execute(
                    update(Configuration)
                    .where(Configuration.config_key == key.name)
                    .values(description=key.description)
                )
