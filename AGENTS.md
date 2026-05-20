# Durababble agent notes

Durababble is a Ruby 4 prototype for durable execution backed by YugabyteDB/YSQL. Keep the implementation honest: persist state before/after each step, test against real Yugabyte when touching storage semantics, and do not replace durable behavior with in-memory shims.

Default local database URL:

```
postgresql://yugabyte@127.0.0.1:15433/yugabyte
```

Use:

```sh
mise exec -- bundle exec rake spec
```
