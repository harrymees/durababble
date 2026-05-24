---
name: durababble-validation
description: |
  Run Durababble local validation and database-backed test commands. Use when
  preparing handoff, reproducing failures, or checking MySQL/MariaDB and
  Yugabyte/PostgreSQL-compatible paths.
---

# Durababble Validation

Use this skill before handoff and when a change touches runtime behavior,
storage semantics, docs examples, or the static type contract.

## Toolchain

Run project commands through `mise exec -- ...`; do not assume system Ruby or
Bundler is available.

Inspect the workspace-selected namespace:

```sh
mise exec -- bundle exec ruby -Ilib -e 'require "durababble"; puts Durababble.default_schema'
```

## Standard Commands

Targeted documentation check:

```sh
mise exec -- env DURABABBLE_YUGABYTE_DATABASE_URL= bundle exec ruby -Ilib -Itest test/durababble/documentation_test.rb
```

Static lint/typecheck gate:

```sh
mise exec -- bundle exec rake lint
```

Default full validation:

```sh
mise exec -- bundle exec rake test
```

Coverage gate:

```sh
mise exec -- bundle exec rake test:coverage
```

This repo does not define a `spec` task; when a ticket asks for `rake spec`,
record the attempted command and use the Minitest `rake test` path.

## Database Notes

- The default local MySQL/MariaDB URL is
  `mysql://root@127.0.0.1:3306/sidekick_server_test`.
- Host-local Yugabyte/YSQL smoke coverage uses
  `DURABABBLE_DATABASE_URL=postgresql://yugabyte@127.0.0.1:15433/yugabyte`.
- Set `DURABABBLE_YUGABYTE_DATABASE_URL` when optional Yugabyte-backed tests
  should run.
- Symphony-created workspaces write `mise.local.toml` and
  `.durababble-workspace.env`; trust those files for the isolated namespace.

For CI-equivalent coverage from a Symphony workspace while disabling optional
Yugabyte coverage:

```sh
mise exec -- env DURABABBLE_DATABASE_URL=mysql://root@127.0.0.1:3306/sidekick_server_test DURABABBLE_YUGABYTE_DATABASE_URL= bundle exec rake test:coverage
```
