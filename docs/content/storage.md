---
title: "Storage"
weight: 40
---

# Storage

Durababble adds durability by storing workflow and object state in your existing database. The store records workflow rows, workflow history, step rows, attempts, retries, cancellation metadata, child-workflow links, leases, fences, outbox rows, durable-object rows, object wake rows, and durable-object-command rows. Completed step and child-observe results replay from storage instead of rerunning side effects, and workers claim work with SQL leases so stale ownership can be fenced.

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

Runtime payloads are Paquito-serialized and stored in binary columns (`LONGBLOB` on MySQL/MariaDB and `bytea` on PostgreSQL/YSQL). Durababble enforces serialized-byte limits before durable writes or RPC sends: workflow input, workflow result, step output, durable object state, inbox payload/result, and RPC argument limits all default to 4 MiB and can be overridden with `Durababble.payload_limits = { workflow_input: bytes, workflow_result: bytes, step_output: bytes, object_state: bytes, inbox_payload: bytes, rpc_argument: bytes }` or the matching `DURABABBLE_MAX_*_BYTES` environment variables. The `workflow_args` hash key and `DURABABBLE_MAX_WORKFLOW_ARGS_BYTES` remain aliases for workflow input size. These Durababble limits are intentionally lower than backend binary column capacities so payload mistakes fail with `Durababble::PayloadTooLarge` before they create large replay, storage, or RPC pressure.

Timer waits, side-effect fences, child-workflow starts/observes, and durable outbox primitives are database state, not in-memory coordination. Workflow timer due times live on `workflows.next_run_at`, retry due-time claims distinguish retryable failures from terminal failed workflows, child links let parents or durable objects reattach after crashes/retries, and heartbeats plus stale lease recovery let replacement workers resume work after process exits.

Durable history is intentionally explicit. That makes replay honest, but it also means workflows should usually be finite processes rather than permanent entities. Prefer durable objects for long-lived identities, and prefer splitting very large jobs into a workflow per bounded batch or phase.
