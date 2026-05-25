---
title: "Storage"
weight: 40
---

# Storage

Durababble adds durability by storing workflow and object state in your existing database. The store records workflow rows, step rows, attempts, waits, retries, cancellation metadata, leases, fences, outbox rows, durable-object rows, and durable-object-command rows. Completed step results replay from storage instead of rerunning side effects, and workers claim work with SQL leases so stale ownership can be fenced.

The default local backend is MySQL or MariaDB, and PostgreSQL-compatible YugabyteDB coverage is available through the optional YSQL path. Configure the database URL explicitly for the environment that will run workflows:

```shell
export DURABABBLE_DATABASE_URL="mysql://root@127.0.0.1:3306/sidekick_server_test"
```

Then create a store and migrate the durable tables:

```ruby
require "durababble"

store = Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
```

The default namespace is `DURABABBLE_SCHEMA` when set; otherwise Durababble derives one from `DURABABBLE_WORKSPACE_ROOT` or the current working directory. MySQL and MariaDB use that value as a durable table prefix, while PostgreSQL and YSQL use it as a SQL schema.

Timer waits, side-effect fences, and durable outbox primitives are database state, not in-memory coordination. Retry due-time claims distinguish retryable failures from terminal failed workflows, and heartbeats plus stale lease recovery let replacement workers resume work after process exits.

Durable history is intentionally explicit. That makes replay honest, but it also means workflows should usually be finite processes rather than permanent entities. Prefer durable objects for long-lived identities, and prefer splitting very large jobs into a workflow per bounded batch or phase.
