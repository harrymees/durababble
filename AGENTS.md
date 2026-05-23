# Durababble agent notes

Durababble is a Ruby durable execution library with MySQL as the default local test backend and optional YugabyteDB/YSQL coverage.
Keep the implementation honest: persist state before/after each step, test against a real database when touching storage semantics, and do not replace durable behavior with in-memory shims.

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
