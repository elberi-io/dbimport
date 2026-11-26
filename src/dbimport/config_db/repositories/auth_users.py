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
"""The users of the REST server in auth_users."""

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import delete, insert, select, update
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import AuthUsers


class StoredUser(BaseModel):
    """An auth_users row. password is a bcrypt hash, the ARN of an AWS
    Secrets Manager secret, or None."""

    model_config = ConfigDict(frozen=True, from_attributes=True)

    username: str
    password: str | None = Field(repr=False)
    disabled: bool
    fullname: str
    department: str
    email: str


class AuthUsersRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def user(self, username: str) -> StoredUser | None:
        row = self._session.scalars(
            select(AuthUsers).where(AuthUsers.username == username)
        ).one_or_none()
        return None if row is None else StoredUser.model_validate(row)

    def add(
        self,
        username: str,
        password: str,
        *,
        disabled: bool = False,
        fullname: str | None = None,
        department: str | None = None,
        email: str | None = None,
    ) -> None:
        """Adds a user. A detail that is None is left to the column
        default (an empty text)."""
        details = {
            name: value
            for name, value in (
                ("fullname", fullname),
                ("department", department),
                ("email", email),
            )
            if value is not None
        }
        self._session.execute(
            insert(AuthUsers).values(
                username=username, password=password, disabled=disabled, **details
            )
        )

    def update(self, user: StoredUser) -> None:
        """Replaces the password, the disabled flag and the details of the
        user with user.username."""
        self._session.execute(
            update(AuthUsers)
            .where(AuthUsers.username == user.username)
            .values(user.model_dump(exclude={"username"}))
        )

    def delete(self, username: str) -> None:
        self._session.execute(delete(AuthUsers).where(AuthUsers.username == username))
