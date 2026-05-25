---
title: "Installation"
weight: 12
---

# Installation

Add Durababble to a Ruby application and connect it to a database. Durababble stores all its durable state in your application's database, so the install step is mostly about pointing the store at the right ActiveRecord connection.

```ruby
gem "durababble"
```

## Supported Databases

Durababble runs on top of ActiveRecord and uses adapter-specific SQL for leases, fences, and outbox claims. The following adapters are supported and exercised in CI:

- **MySQL** and **MariaDB** via the `trilogy` adapter. This is the default local backend and the most heavily tested path.
- **PostgreSQL** via the `postgresql` adapter.
- **YugabyteDB** through the PostgreSQL-compatible `yugabyte` adapter. Tested through the optional YSQL path; CI exercises it when `DURABABBLE_YUGABYTE_DATABASE_URL` is set.

Other ActiveRecord adapters are rejected at connect time with an explicit error. SQLite is not supported because the store relies on row-level locking and `SELECT … FOR UPDATE SKIP LOCKED` semantics that SQLite cannot provide.

## Connecting To A New Connection Pool

The simplest path is to let Durababble establish its own ActiveRecord connection pool from a database URL. This is what scripts, examples, and small services usually want:

```shell
export DURABABBLE_DATABASE_URL="mysql://root@127.0.0.1:3306/my_app_development"
```

```ruby
require "durababble"

store = Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
```

`Store.connect` creates an anonymous `ActiveRecord::Base` subclass, establishes a connection pool against the database URL, and returns the right adapter-specific store. `store.migrate!` creates the durable tables.

## Connecting To An Existing ActiveRecord Pool

If you already have an ActiveRecord setup (Rails app, multi-database configuration, custom abstract class) you can hand Durababble that pool directly instead of giving it a database URL. This shares connections with the rest of your application and avoids a second pool against the same database:

```ruby
# Reuse the primary application connection pool:
store = Durababble::Store.from_active_record(
  connection_pool: ApplicationRecord.connection_pool,
)
store.migrate!
```

```ruby
# Or target a dedicated abstract class for a separate durababble database:
class DurababbleRecord < ActiveRecord::Base
  self.abstract_class = true
end
DurababbleRecord.establish_connection(:durababble)

store = Durababble::Store.from_active_record(
  connection_pool: DurababbleRecord.connection_pool,
)
store.migrate!
```

`from_active_record` also accepts a bare `connection:` for cases where you already hold a checked-out ActiveRecord connection. The adapter is detected from `connection.adapter_name`, so the same call works for MySQL, PostgreSQL, and YugabyteDB.

## Workers And Cluster Addresses

A `Durababble::Worker` is enough for a single process: it polls the store, claims leases, and drains workflows and object inboxes. In production you usually run many workers across many pods, and they need to reach each other over the network so that simple RPCs (`expose`) and live command delivery (`expose_command`) can land on the worker that currently owns the workflow or durable object.

`Durababble::WorkerRuntime` wraps a `Worker` with a gRPC server and is the production-shaped entry point. Each runtime advertises an address that becomes its `worker_id`, and the lease row written when the runtime claims a workflow records that address as the owner. Any other worker that wants to RPC into the lease holder reads the same row and dials the address directly:

```ruby
runtime = Durababble::WorkerRuntime.start(
  store:,
  workflows: [FulfillOrder],
  objects: [Account],
  worker_pool: "orders",
  rpc_host: ENV.fetch("POD_IP"),  # the address other pods can reach this one at
  rpc_port: 50_051,
)
```

The address is the worker's public identity, so `rpc_host` must be a value other pods can actually connect to. In Kubernetes this is typically the pod IP exposed through the downward API; on bare metal it is the host's routable address. The defaults (`rpc_host: "127.0.0.1"`, `rpc_port: 0`) are fine for single-process examples and tests but leave the worker unreachable from anywhere else. `rpc_credentials:` defaults to insecure local-only credentials; production deployments should pass mTLS credentials through this argument.

For more on how peer-to-peer routing works once workers are wired up — and why Durababble routes simple RPC and live command delivery through the cluster mesh rather than the database — see [Cluster RPC](cluster-rpc.md).

## Schemas And Table Prefixes

The store namespaces its tables so multiple applications can share one database. Pass `schema:` explicitly when you want a stable name:

```ruby
Durababble::Store.from_active_record(
  connection_pool: ApplicationRecord.connection_pool,
  schema: "my_app_durababble",
)
```

If `schema:` is omitted, Durababble reads `DURABABBLE_SCHEMA` from the environment; if that is unset, it derives one from `DURABABBLE_WORKSPACE_ROOT` or the current working directory. MySQL and MariaDB use the schema value as a durable table prefix, while PostgreSQL and YugabyteDB use it as a SQL schema.
