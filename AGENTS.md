# Durababble agent notes

Durababble is a Ruby durable execution library with MySQL as the default local test backend and optional YugabyteDB/YSQL coverage.
Keep the implementation honest: persist state before/after each step, test against a real database when touching storage semantics, and do not replace durable behavior with in-memory shims.

Default local MySQL database URL:

```
mysql://root@127.0.0.1:3306/sidekick_server_test
```

Use:

```sh
bundle exec rake test
```

Set `DURABABBLE_YUGABYTE_DATABASE_URL` to include optional Yugabyte-backed tests.
