# ADR-0005: Start the migrations with one baseline revision generated from the ORM

- **Status:** Accepted
- **Date:** 2026-11-17

## Context

The configuration database schema was built by 48 hand-written Alembic
revisions, from the base layout of 2019 to 0.80.009. Several of their
downgrades fail, and `setup --downgradeDB` called a method that does not
exist (#153). New revisions were written by hand, because the ORM
differed from the migrated schema. Since #260 the ORM creates exactly
that schema, checked by a test (ADR-0004). Releases 1.0.0 to 1.00.006
ship the same 48 revisions, so their databases are all at the last one,
`a5899e8b06a9`.

## Decision

We will replace the 48 revisions with one baseline revision generated
from the ORM, and generate later revisions from ORM changes:

- the baseline keeps the id `a5899e8b06a9`, so a database of DBImport
  1.0.x is at the baseline without any stamp or upgrade step;
- `setup --upgradeDB` stops with an error on a revision it does not
  have; a database of a version before 1.0.0 is first upgraded with
  DBImport 1.00.006;
- a schema change starts in the ORM; the revision comes from `alembic
  revision --autogenerate` (`alembic.ini`, the database of the
  configuration file) and is reviewed before it is committed; the
  schema test compares the migrated schema with the ORM;
- `setup --downgradeDB` undoes one revision and stops at the baseline,
  whose downgrade raises instead of dropping every table.

## Alternatives considered

- **Keep the chain and fix its downgrades** - 48 revisions to keep
  working and typed, a new installation runs all of them, and new
  revisions stay hand-written.
- **A new baseline id, stamped over the old head by `upgradeDB`** - a
  code path that every installation runs once and that has to stay
  tested; reusing the id needs none.
- **Keep the old chain beside the baseline for databases before
  1.0.0** - two histories to maintain for releases from before
  2025-04-29.

## Consequences

- Positive: a new installation creates the schema in one revision;
  schema changes are generated from the ORM, so ORM and migrations
  cannot drift unnoticed; downgrades work one revision at a time; the
  migrations package passes the strict mypy settings.
- Negative: a database older than 1.0.0 needs an intermediate upgrade
  with DBImport 1.00.006; the baseline is about 3,500 generated lines;
  autogenerate output needs a careful review: it writes a renamed column
  as a dropped and an added one, does not show AUTO_INCREMENT outside a
  primary key, and changes a column with MODIFY, which restates the
  whole column definition.
- Follow-up: the data layer step of #238 decides on primary keys for
  the three tables without one, if a repository needs them; the REST
  contract step decides whether the `auth_users` details may be NULL.
