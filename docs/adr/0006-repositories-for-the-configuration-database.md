# ADR-0006: Access the configuration database through repositories and one engine

- **Status:** Accepted
- **Date:** 2026-11-17

## Context

The configuration database is accessed in two styles: raw
`mysql.connector` cursors in the `config_db` modules (128 statements,
22 of them with values formatted into the SQL text) and the SQLAlchemy
ORM in REST, copy, the daemon, DAG generation, Atlas and setup (195
statements). Seven modules created their own engine, each with its
own URL and pool. Most statements are followed by a commit, so
multi-step changes are not atomic, and a raw cursor keeps a transaction
open after a SELECT. Tests cover little of this code (the `config_db`
modules between 7% and 29% of their lines, copy and the daemon none).
The ORM describes the schema with typed attributes since ADR-0004.

## Decision

We will access the configuration database only through repositories
built on the typed ORM, with engines from one module:

- `dbimport.config_db.engine` resolves the credentials once per process
  (configuration file or AWS Secrets Manager) and builds the engines: one
  that the `config_db` classes share, and one for each component that
  disposes of its engine itself; a remote DBImport instance gets its
  engine from the same module;
- one repository class per group of tables in
  `dbimport.config_db.repositories` (configuration, connections, import
  tables, export tables, stages and statistics, Airflow, copy); it takes
  a `Session`, uses statements of the typed ORM with parameters, returns
  typed values or pydantic models, and does not commit;
- the caller opens a session per logical operation and commits at the
  points where the code committed before, so a stage change is still
  stored when it is made; changes that belong together are committed
  together;
- the public methods of the `config_db` classes stay the interface for
  operations and the command line programs; only their bodies change.
  Modules that used the ORM directly use the repositories directly;
- each group is switched in its own change, after tests that record the
  current behaviour against the test database; the new modules pass the
  strict mypy settings; `mysql-connector-python` goes with the last raw
  cursor.

## Alternatives considered

- **Operations call the repositories directly** - rewrites the
  operations modules, whose own refactoring is a later phase; the
  `config_db` methods would change twice.
- **Return ORM objects from the repositories** - ties callers to the
  session that loaded them (lazy loads, expired attributes after a
  commit) and to the camelCase column names.
- **SQLAlchemy Core with hand-written table objects** - a second
  description of the schema next to the ORM (ADR-0004).
- **Keep `mysql.connector` for the `config_db` modules** - two drivers
  and two connections per process, and the statements stay
  hand-written SQL.

## Consequences

- Positive: one way to reach the configuration database; parameters
  everywhere; atomic multi-step changes; one place that builds
  connections; typed results; the second MySQL driver goes.
- Negative: several changes over many modules, each needing new tests
  first; the facade methods keep their untyped callers until the
  operations modules are rewritten; per-operation sessions add a
  connection checkout per call where the raw cursor reused one
  connection; until the last raw cursor goes, a command line process
  holds a raw connection and a pooled one.
- Follow-up: the groups after configuration, in the order of the #238
  plan; the Hive metastore and Airflow database engines stay outside
  this decision.
