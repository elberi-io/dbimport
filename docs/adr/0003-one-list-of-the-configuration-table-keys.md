# ADR-0003: Keep the configuration table keys in one typed list

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

The `configuration` table holds 74 global settings, one row per key with
the value in `valueInt` (also 0/1 flags) or `valueStr`. Each key was
written out in nine places: the seed in `setup` (default, description,
plus three later description rewrites), the read whitelist
`get_config_value_column` (column, bool flag), the REST model
`Configuration` (type), a separate REST key list, the key table in
`docs/configuration.rst`, and four UI files (`infoTexts.json`,
`cardRenderFormatting.ts`, `interfaces.tsx`, `nameMappings.ts`). The
code copies agreed, but the docs table listed 8 keys the code no longer
had and lacked 31, and a UI type lacked 2. #238 types the configuration
and data layer.

## Decision

We will keep every key in `dbimport.config_db.config_keys.KEYS`, a tuple
of frozen dataclasses with name, kind (`bool`, `int`, `str`), default,
description and whether the REST API shows it. A default can be a
function, called once per installation (the REST token key). From the
list we derive:

- the read whitelist (`get_config_value` accepts only listed keys);
- the seed: `setup` inserts missing keys with their default and sets the
  description of existing rows to the listed text, never their value;
- the REST model, with `pydantic.create_model`;
- the key table in the docs, checked by a test that rewrites it on
  request.

The UI keeps its copies until its types are generated from the OpenAPI
schema (#238, REST contract step); a test checks that its key sets equal
the REST keys. `get_config_value` keeps its string argument and its
`Any` result until the data layer is rewritten (#238).

## Alternatives considered

- **Keep the hand-written REST model, test it against the list** - mypy
  would see the fields, but a key would still be added in two places.
- **A data file (JSON, YAML) instead of Python** - the list would need
  its own loader and schema check, which a Python list gets from mypy
  and ruff with the code.
- **Typed accessors per key now** - would change all 105
  `get_config_value` calls in this step instead of with the data layer.

## Consequences

- Positive: a new key is one entry plus its UI copies; the seed, the
  whitelist, the REST model and the docs cannot drift apart; existing
  installations get current descriptions on `setup --upgradeDB`.
- Negative: mypy sees the REST model only as `BaseModel`, so its fields
  are not type-checked where the model is used; descriptions an operator
  edited in the table are overwritten by `setup --upgradeDB`.
- Follow-up: UI copies and descriptions in the UI (REST contract step),
  typed reads (data layer step), both in #238.
