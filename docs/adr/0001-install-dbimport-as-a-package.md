# ADR-0001: Install DBImport as a Python package with a src layout

- **Status:** Accepted
- **Date:** 2026-11-13

## Context

DBImport ran from the extracted tarball: the packages lived in `bin/`
with mixedCase names (`DBImportConfig`, `Server`, ...) and were imported
by bare name, because Python puts the launcher's directory on
`sys.path`. pyproject.toml had no build backend (`[tool.uv] package =
false`), so ruff, mypy, pytest and import-linter each carried a setting
that put `bin/` on the path, and code built paths from `DBIMPORT_HOME`
to find its own files (Spark UDF modules, Alembic migrations, UI).
Generated Airflow DAGs and the REST server call `bin/import` and
`bin/manage` by path. Phase 1 (#238) moves code between modules; doing
that in a standard layout first avoids moving it twice.

## Decision

We will install DBImport as the package `dbimport` from `src/dbimport`,
built with `uv_build` (`uv_build>=0.13.7,<0.14`), with snake_case
package and module names. The launcher code lives in `dbimport.cli`;
console scripts `dbimport-import`, `dbimport-export`, ... call it, and
the files in `bin/` keep their names as wrappers that call the same
functions. The package finds its own files through the package
(`__file__`), not through `DBIMPORT_HOME`, which stays for `conf/`, the
UI in `ui/` and other files outside the package.

## Alternatives considered

- **Source tree without an install step** (code in `src/dbimport`,
  launchers put `src/` on `sys.path`) - keeps the path settings in four
  tools and the path handling in code; not the standard layout.
- **hatchling or setuptools as backend** - both work; hatchling brings
  four dependencies, setuptools is larger and more verbose; `uv_build`
  has no dependencies and comes with the tool that manages the lock.
- **Drop the bin/ launchers** - would break deployed DAGs, the REST
  server's `bin/manage` calls and operator scripts until all are changed.

## Consequences

- Positive: imports are `dbimport.*` everywhere; `uv sync` installs the
  project, so the tools need no path settings; ruff checks module names
  (N999); git keeps the history of moved files.
- Negative: installing from the tarball needs a second step (`pip
  install --no-deps .`), which downloads `uv_build`; hosts without
  internet access need its wheel or a built DBImport wheel. Import paths
  changed for anyone importing DBImport modules directly.
- Follow-up: the `bin/` wrappers stay until the CLI is rebuilt (later
  phase) and the DAGs are regenerated with new commands; regrouping the
  packages into the target layers is phase 2 work.
