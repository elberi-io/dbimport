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
"""The host's local time without a zone, the form DBImport's outputs
(statistics documents, Atlas lineage, the REST API) carry while the
configuration database holds UTC (ADR-0007)."""

from datetime import UTC, datetime


def to_host_time(value: datetime) -> datetime:
    """An aware time as the host's local time, without a zone."""
    return value.astimezone().replace(tzinfo=None)


def from_host_time(text: str) -> datetime:
    """A time written as the host's local time ('2026-06-01 12:00:00'),
    as an aware time in UTC. Raises ValueError for any other text."""
    return datetime.fromisoformat(text).astimezone(UTC)


def host_time_text(value: datetime | None) -> str:
    """str() of the host's local time; 'None' for None, as str(None)."""
    return str(None if value is None else to_host_time(value))
