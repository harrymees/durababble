# Durababble agent notes

Durababble is a Ruby durable execution library with MySQL as the default local test backend and optional YugabyteDB/YSQL coverage. Keep the implementation honest: persist state before/after each step, test against a real database when touching storage semantics, and do not replace durable behavior with in-memory shims. Put new tests in the existing suite that describes the subsystem under test. Avoid miscellaneous catch-all suites; when a new grouping is needed, name it after the behavior or component it verifies. For Markdown documentation, do not hard-wrap prose in README.md, docs/\*.md, AGENTS.md, or similar docs; keep each paragraph or list item on one line unless a table, code block, or generated format requires explicit line breaks. Keep README.md focused on repository orientation and navigation. Put behavior, API, storage, and operational documentation in the docs site source under docs/content/ plus docs/spec.md or docs/content/architecture.md when those contracts change.

Default local MySQL database URL:

```
mysql://root@127.0.0.1:3306/sidekick_server_test
```

Durababble must not use one shared local namespace for all checkouts. The default namespace is `DURABABBLE_SCHEMA` when set; otherwise it is derived from `DURABABBLE_WORKSPACE_ROOT` or the current working directory via `Durababble.workspace_schema`. PostgreSQL/YSQL uses that value as a SQL schema; MySQL/MariaDB uses it as the durable table prefix. Symphony-created workspaces write/trust `mise.local.toml`, migrate their isolated namespace, and leave `.durababble-workspace.env` for inspection.

Inspect a workspace namespace with:

```sh
mise exec -- bundle exec ruby -Ilib -e 'require "durababble"; puts Durababble.default_schema'
```

Use:

```sh
mise exec -- bundle exec rake test
```

Set `DURABABBLE_YUGABYTE_DATABASE_URL` to include optional Yugabyte-backed tests. For the host-local Symphony smoke path, use `DURABABBLE_DATABASE_URL=postgresql://yugabyte@127.0.0.1:15433/yugabyte`.
