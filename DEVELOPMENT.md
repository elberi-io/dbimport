# Development

This page describes how to set up a local development environment for
DBImport. It is meant for people who work on DBImport itself. To install
DBImport on a cluster, see the
[installation guide](https://dbimport.readthedocs.io/en/latest/install.html).

DBImport only runs on Linux. On Windows, use WSL 2 with an Ubuntu
distribution and keep the repository in the Linux file system (for example
under `~/work`), not under `/mnt/c`.

## Requirements

- Linux, or WSL 2 with Ubuntu 24.04 on Windows
- Docker, with Docker Compose. On WSL 2, Docker Desktop with the WSL
  integration enabled for the Ubuntu distribution
- [uv](https://docs.astral.sh/uv/)
- A C compiler and the Kerberos development headers, needed to build some
  of the Python packages. On Debian and Ubuntu:

  ```bash
  sudo apt-get install -y build-essential libkrb5-dev git curl ca-certificates
  ```

## Python environment

uv installs Python 3.11 if it is not already available and creates the
environment from `uv.lock`:

```bash
uv sync --locked
uv run python --version
```

## Configuration database

`compose.yaml` starts a MariaDB 11.4 server for development and tests. It
listens on 127.0.0.1:3306 and has a database `DBImport` with the user
`dbimport` and password `dbimport`. These are throwaway values for local
use only.

The server runs with `latin1` as the default character set. With `utf8mb3`
or `utf8mb4`, the schema migrations fail because several primary keys
exceed the InnoDB key length limit of 3072 bytes.

Start the server and wait until it is healthy:

```bash
docker compose up -d --wait
```

`tests/fixtures/dbimport.test.cfg` is a DBImport configuration file that
points at this server. It expects an RSA key pair in
`tests/fixtures/keys/`, which is not part of the repository. Create it once
with:

```bash
mkdir -p tests/fixtures/keys
openssl genrsa -out tests/fixtures/keys/dbimport_private_key.pem 8192
openssl rsa -in tests/fixtures/keys/dbimport_private_key.pem -out tests/fixtures/keys/dbimport_public_key.pem -outform PEM -pubout
```

Point DBImport at the repository and the test configuration, then create
the configuration database schema:

```bash
export DBIMPORT_HOME=$PWD
export DBIMPORT_CONFIGFILE=$PWD/tests/fixtures/dbimport.test.cfg
uv run python bin/setup --createDB
```

Stop the server and delete its data with:

```bash
docker compose down
```

## Running the tests

Unit tests (`tests/unit/`) need no database:

```bash
uv run pytest -m "not integration"
```

Integration tests (`tests/integration/`) run against the MariaDB server
from `compose.yaml`. They delete and recreate all tables in the `DBImport`
database, and refuse to run against any host other than 127.0.0.1,
localhost or the host named in the `DBIMPORT_TEST_DB_HOST` environment
variable (used when the tests run in a container on the compose
network). The tests generate their own RSA key pair, so the key pair from
the section above is not needed for them.

```bash
docker compose up -d --wait
uv run pytest
```

Known bugs are recorded as expected failures (`xfail`, strict), so a fix
shows up as an unexpected pass and the marker must be removed with it.

## Checks

CI (`.github/workflows/ci.yaml`) runs these on every pull request. Run them
before you open one:

```bash
uv run ruff format --check
uv run ruff check
uv run mypy
PYTHONPATH=bin uv run lint-imports
uv run pytest
```

For the web UI, in `bin/Server/html`:

```bash
npm ci
npm run lint
npm run build
```

All Python code is formatted with `ruff format` and checked with one
rule set, `select` in `pyproject.toml` (pycodestyle E4, E7 and E9,
pyflakes, bugbear, pyupgrade, import sorting with the packages in `bin/`
as first party, PEP 8 naming, invalid escape sequences, unused `noqa`).
Names that are contracts are not checked yet: class names, class-level
fields (they include the JSON field names of the REST API) and module
names. Existing modules that still break some of these rules are listed under
`per-file-ignores`, each with exactly the rule codes it breaks.
`tests/unit/test_ruff_ratchet.py` checks these modules without the
ignores: it fails when a listed code no longer occurs in its module, and
when the number of findings per module and code differs from
`tests/fixtures/ruff_baseline.json`. So a new finding in a listed module
fails too; after you fix old findings, regenerate the baseline (see
below) and remove codes that no longer occur. New modules must pass the
full set, and new names in listed modules follow PEP 8 too, even where
the code around them does not. A known bug
that a rule reports is marked on its line with `noqa` and a `TODO` that
names the issue. import-linter checks the package layering defined in
`pyproject.toml`.

`uv run mypy` checks `tests/` and the typed packages `bin/common` and
`bin/ConfigReader` in strict mode (`files` in `pyproject.toml`). The
other code in `bin/` has almost no type annotations;
`tests/unit/test_mypy_ratchet.py` checks it
(packages and launchers, all function bodies, with the settings of its
override in `pyproject.toml`) and fails when the number of errors per
module and error code differs from `tests/fixtures/mypy_baseline.json`.
New modules must have no errors, and in listed modules the count per
error code must not grow (it compares counts, so a change that fixes one
error and adds another of the same code passes); after fixing old
errors, regenerate the baseline (see below). A change that adds type
hints may raise counts where the new types expose existing type mixes;
its commit message lists each one. A `# type: ignore` must
name the error code. Libraries without type information are listed
under `ignore_missing_imports`.

The commits that reformatted `bin/` are listed in `.git-blame-ignore-revs`.
GitHub skips them in the blame view; to skip them in `git blame` locally,
run once:

```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

## Generated files

Do not edit these by hand; regenerate them and review the diff:

| File | Command |
|---|---|
| `uv.lock` | `uv lock` (after changing dependencies in `pyproject.toml`) |
| `bin/requirements.txt` | `uv export --locked --no-dev --format requirements-txt -o bin/requirements.txt` |
| `tests/fixtures/dags/*.py.txt` | `DBIMPORT_UPDATE_EXPECTED_DAGS=1 uv run pytest tests/integration/test_airflow_dag_generation.py` |
| `tests/fixtures/ruff_baseline.json` | `DBIMPORT_UPDATE_RUFF_BASELINE=1 uv run pytest tests/unit/test_ruff_ratchet.py` |
| `tests/fixtures/mypy_baseline.json` | `DBIMPORT_UPDATE_MYPY_BASELINE=1 uv run pytest tests/unit/test_mypy_ratchet.py` |

`tests/fixtures/orm_schema_drift.txt` lists the known differences between
the ORM and the migrated schema. When the ORM or the migrations change,
update it from the difference that
`test_orm_differs_from_migrated_schema_only_by_known_drift` reports.
