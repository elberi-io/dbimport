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
