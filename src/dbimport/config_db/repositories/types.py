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
"""Field types shared by the repository models."""

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator


def _zero_date_as_none(value: object) -> object:
    # Outside strict mode MariaDB stores an invalid DATETIME as zero, and
    # PyMySQL returns it as this text instead of a datetime.
    if isinstance(value, str) and value.startswith("0000-00-00"):
        return None
    return value


StoredDateTime = Annotated[datetime | None, BeforeValidator(_zero_date_as_none)]
"""A DATETIME column; a zero date reads as None."""
