# ADR-0007: Store the times of the configuration database in UTC

- **Status:** Accepted
- **Date:** 2026-11-27

## Context

The configuration database has 186 DATETIME columns (run statistics,
stage times, logs, copy status, last update times) and no TIMESTAMP
columns. DBImport wrote them with `datetime.now()`, the local time of
the host it runs on, and the database stores no time zone. A few values
were UTC (unix times converted with `utcfromtimestamp`), and four
columns take the database server's `CURRENT_TIMESTAMP` in its session
time zone. Hosts in different zones, or a zone change, make the stored
times ambiguous; daylight saving makes one hour a year ambiguous. The
tests of #238 P7 (#305) record this with a host zone other than UTC.

## Decision

We will store every DATETIME of the configuration database in UTC and
work with aware datetimes in Python.

- The ORM type `UTCDateTime` (`config_db/config_schema.py`) keeps the
  DATETIME column, stores an aware datetime as UTC, returns aware UTC
  datetimes and refuses a naive one.
- Every connection of `config_db/engine.py` and of the migrations sets
  the session time zone to UTC, so `CURRENT_TIMESTAMP` defaults are UTC.
- `setup --upgradeDB --localTimeZone=<zone>` converts the existing rows
  once from the zone they were written in; the operator names it.
- Times of day with their own zone (schedules, time windows) and
  `atlas_column_cache.table_create_time` (the source's own time) stay
  as they are.
- ruff `DTZ` rejects new naive datetimes.

## Alternatives considered

- **TIMESTAMP columns** - MariaDB converts them by the session time zone
  and, in 11.4, cannot hold times after 2038.
- **Local time with a stored zone** - every reader must convert, and
  hosts in different zones still mix.
- **No conversion of existing rows** - old and new rows would mean
  different things in one column.

## Consequences

- Positive: one meaning per column; comparisons with Airflow (UTC) and
  between hosts hold; daylight saving no longer repeats an hour.
- Negative: the upgrade needs the zone the rows were written in and
  rewrites every time column (long on large statistics tables); rows
  written in the repeated hour at the end of daylight saving take the
  earlier time; the three columns the database server's clock filled
  are converted with the hosts' zone; a downgrade stops when two times
  of a key would become the same local time.
- Follow-up: the outputs (statistics documents, Atlas lineage, REST
  API, Airflow JSON) still carry the host's local time; #238 P7 7c
  writes them as ISO 8601 with offset.
