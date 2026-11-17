# ADR-0004: Describe the configuration database schema with the typed ORM

- **Status:** Accepted
- **Date:** 2026-11-17

## Context

The schema of the configuration database is built by a linear chain of
hand-written Alembic migrations. The ORM in
`dbimport.config_db.config_schema` was a second, untyped description of
it (`declarative_base()`, `Column`) that had drifted: Alembic's
`compare_metadata()` reported 43 differences, a comparison through
`information_schema` 81 plus another column order in 11 tables (#260).
The next step of #238 replaces the migration chain with a new baseline
and needs one description of the schema to build it from. The data
layer step after it puts typed repositories on top of that description.

## Decision

We will treat the ORM in `config_schema.py` as the description of the
configuration database schema:

- it creates exactly the schema the migrations create; an integration
  test compares both through `information_schema` (tables, columns with
  order, types, defaults and comments, indexes, foreign keys, CHECK
  constraints, triggers) and with `compare_metadata()`;
- the models use the typed SQLAlchemy 2 form (`DeclarativeBase`,
  `Mapped[...]`, `mapped_column(...)`); the database type is written
  out where the annotation alone would not give the same DDL, and the
  annotation follows the column's nullability;
- attribute names are snake_case; the database keeps its column names
  (`mapped_column("configKey", ...)`);
- a table without a primary key is a Core `Table`, not a mapped class
  with an invented identity.

pydantic stays out of the database layer: it is used where data enters
or leaves (settings, REST, the repositories' results).

## Alternatives considered

- **SQLModel** (one class as table and pydantic model) - an extra
  dependency that follows SQLAlchemy releases with a delay; most columns
  would still need `sa_column=Column(...)` for the exact DDL, table
  models skip pydantic validation, and the REST field names differ from
  the column names anyway.
- **Migrations as the only description, models reflected at run time
  (automap)** - no typed attributes for mypy, and the next step could
  not generate a baseline from code.
- **Keep the old style and a list of known differences** - the list hid
  real differences and does not give mypy the attribute types.

## Consequences

- Positive: one checked description of the schema; mypy knows the type
  of every ORM attribute, and the ORM modules have no mypy errors left;
  the baseline step can generate from the models.
- Negative: until that step, every migration needs the matching model
  change, or the schema test fails; the camelCase column names stay in
  the database, so two names exist for 20 columns: raw SQL, rows read
  through `Model.__table__` and keys of Core statements (`values()`,
  `on_duplicate_key_update()`, `update(Model.__table__)`) use the column
  name, while ORM attributes, rows of ORM queries and ORM updates
  (`Query.update()`, bulk updates) use the attribute name. A key of the
  wrong kind fails, or in `on_duplicate_key_update()` is skipped with
  only a warning.
- Follow-up: schema changes the ORM text suggested (comments, the
  `auth_users` columns, primary keys for the three tables without one)
  are decided in the baseline step; the Hive metastore and Airflow
  models follow the same style but describe schemas DBImport does not
  create, so no test checks them against a database.
