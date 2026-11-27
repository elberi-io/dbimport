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
"""Times as DBImport writes them into its outputs (statistics documents,
the Airflow document, the REST API): ISO 8601 in UTC with the offset
(ADR-0007)."""

from datetime import UTC, datetime


def iso_time(value: datetime) -> str:
    """An aware time as '2026-06-01T10:00:00+00:00': UTC, to the second.
    Raises ValueError for a time without a zone."""
    if value.tzinfo is None:
        raise ValueError(f"The time has no zone: {value}")
    return value.astimezone(UTC).isoformat(timespec="seconds")


def iso_time_text(value: datetime | None) -> str:
    """iso_time(); 'None' for None, as the statistics documents wrote it."""
    return "None" if value is None else iso_time(value)
