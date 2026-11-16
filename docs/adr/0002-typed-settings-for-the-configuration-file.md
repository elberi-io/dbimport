# ADR-0002: Read the configuration file into typed settings

- **Status:** Accepted
- **Date:** 2026-11-13

## Context

DBImport read its INI configuration file with `configparser` when the
module `config_reader.configuration` was first imported, and 97 places
in the code read single keys as strings (57 keys in 13 sections). A
missing key ended the process only when the code that needed it ran;
10 keys were optional through `try`/`except` around the read. Values
were converted where they were read (`int()`, `.lower() == "true"`),
sometimes differently for the same key, and passwords were plain
strings. #238 types the configuration and data layer ("full pydantic":
pydantic at every boundary and for configuration).

## Decision

We will read the configuration file into pydantic models with
pydantic-settings, one model per section, through a small custom INI
source (pydantic-settings has none) in `dbimport.settings`. The file is
found and parsed as before (configparser with its `%` interpolation, so
`%%` stays a literal `%`); an environment variable
`DBIMPORT_<SECTION>__<KEY>` overrides a value from the file. The file is
read and validated once, on the first `get_settings()` call: every
missing required key and every value of the wrong type is reported
together and the program exits. The keys of the template are required,
except the keys the code read only when a feature is used (the Atlas
keys, the Airflow database connection) and the keys it already treated
as optional; these get the defaults the code used before, and a blank
value means "not set". Values are typed (integers, booleans, fixed
values); passwords, the anonymization seed and connection strings that
contain a password are `SecretStr`.

## Alternatives considered

- **Plain pydantic models fed by configparser** - no new dependency and
  no pydantic upgrade, but environment overrides and the source order
  would be hand-written.
- **Keep validating each key when it is read** - a command would still
  run with an incomplete file, but errors would keep coming one at a
  time and the conversions would stay spread over the code.

## Consequences

- Positive: one typed object for the configuration, checked once;
  conversions in one place; secrets hidden in logs and tracebacks;
  overrides from the environment (services, containers).
- Negative: new dependencies pydantic-settings (MIT) and
  typing-inspection, and pydantic moved from 2.6.2 to 2.12.5; a file
  that misses a required key a command does not use now stops that
  command; a true/false key accepts what pydantic accepts (`yes`, `1`,
  `off`, ...) and rejects other values, where before every value but
  `true` (or `false` for Atlas `ssl_verify`) counted as the other one, so
  `ssl_verify = no` now turns TLS verification off; a blank AWS region
  means no region, so empty database credentials no longer try AWS
  Secrets Manager with an empty region but report the configuration
  error; section names are case-insensitive.
- Follow-up: the REST server still reads the settings and the
  configuration database while it is imported, until its start-up is
  reworked (#238, REST runtime step); file paths stay strings until the
  modules that use them are rewritten.
