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
"""The copy of configuration rows to the configuration database of
another DBImport instance."""

from collections.abc import Collection, Mapping, Sequence
from typing import Any

from sqlalchemy import Table, func, select, update
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import Base


def _table(model: type[Base]) -> Table:
    table = model.__table__
    assert isinstance(table, Table)
    return table


class InstanceCopy:
    """Reads rows here and writes them on another instance. The caller
    commits the session of the other instance."""

    def __init__(self, local: Session, remote: Session) -> None:
        self._local = local
        self._remote = remote

    def rows(
        self,
        model: type[Base],
        where: Mapping[str, object],
        order_by: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        """The rows of the model's table here whose columns have the values
        of where, by column name."""
        table = _table(model)
        result = self._local.execute(
            select(table)
            .where(*(table.c[name] == value for name, value in where.items()))
            .order_by(*(table.c[name] for name in order_by))
        )
        return [dict(row) for row in result.mappings()]

    def copy(
        self,
        model: type[Base],
        row: Mapping[str, object],
        match: Mapping[str, object],
        leave_out: Collection[str] = (),
    ) -> None:
        """Writes row to the row of the model's table on the other instance
        whose columns have the values of match, and adds that row when there
        is none. Columns in match and in leave_out keep their value there; a
        new row gets the values of match and the defaults for leave_out.
        Raises MultipleResultsFound when more than one row matches."""
        table = _table(model)
        values = {
            name: value
            for name, value in row.items()
            if name not in match and name not in leave_out
        }
        condition = [table.c[name] == value for name, value in match.items()]
        found = self._remote.scalar(
            select(func.count()).select_from(table).where(*condition)
        )
        if found == 1:
            self._remote.execute(update(table).where(*condition).values(values))
        elif found == 0:
            # Another copy may add the row before this transaction ends.
            add = insert(table).values({**match, **values})
            if values:
                add = add.on_duplicate_key_update(values)
            self._remote.execute(add)
        else:
            raise MultipleResultsFound(
                f"{found} rows of {table.name} on the other instance match {match}"
            )

    def remote_value(
        self, model: type[Base], column: str, match: Mapping[str, object]
    ) -> Any:
        """The value of column in the row of the model's table on the other
        instance whose columns have the values of match. Raises
        NoResultFound when there is none."""
        table = _table(model)
        return self._remote.scalars(
            select(table.c[column]).where(
                *(table.c[name] == value for name, value in match.items())
            )
        ).one()
