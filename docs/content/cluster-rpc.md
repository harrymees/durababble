---
title: "Cluster RPC"
weight: 45
---

# Cluster RPC

Durababble workers form a peer-to-peer HTTP/2 RPC mesh. When code somewhere in the cluster calls `handle.label` on a workflow handle or reads `account.balance` on a durable object reference, the call is delivered straight to the pod that currently holds the lease for that entity — no database round-trip in the request path. This page explains why that mesh exists, what it is and isn't, and how it stays cheap.

## Two Kinds Of Calls

Durababble has two distinct kinds of cross-process call, and they take very different paths.

- **Durable commands** — `expose_command` mutations and durable workflow signals — are inbox messages. The sender writes a row, the owner's worker consumes that row exactly once, and the result is recorded. They survive crashes, get retried, are guaranteed to be delivered eventually, and are ordered per recipient. They are also intentionally expensive: every command is at least one insert plus one update plus a fence.

- **Simple RPC** — `expose` reads and the live wake-up that gets a freshly-inserted command picked up promptly — does not need any of that. A `balance` query just wants the current state, right now, from whichever worker has it warm. Sending those through the database would mean every status check, every "is this thing still running?", every read against a hot durable object hits SQL and waits behind whatever else is in flight.

The mesh exists so the cheap calls stay cheap. Durable commands still go through the inbox; this page is about everything else.

```mermaid
flowchart TB
    Caller["Caller<br/>(web app, background job,<br/>or another worker)"]

    subgraph cluster["Worker cluster — peer-to-peer HTTP/2 mesh, any worker can dial any other"]
        direction LR
        WA["Worker A<br/>10.0.0.41:50051"]
        WB["Worker B<br/>10.0.0.42:50051<br/>⭐ holds lease for acct_123"]
        WC["Worker C<br/>10.0.0.43:50051"]
    end

    DB[("SQL store<br/><b>lease table = phone book</b><br/>(address only, no payload)")]

    Caller ==>|"handle.balance — any worker can route"| WB

    WA -.->|"lookup"| DB
    WB -.->|"hold + heartbeat"| DB
    WC -.->|"lookup"| DB

    classDef caller stroke:#d97706,stroke-width:3px
    classDef owner stroke:#16a34a,stroke-width:3px
    class Caller caller
    class WB owner
```

Every worker runs an HTTP/2 RPC server (via `async-http`) and can dial every other worker. The SQL store is shared, but only the **lease table** is consulted on the request path — to look up which worker currently owns the target. The payload itself never touches the database.

## Why Not Run Everything Through The Database

Temporal-shaped durable execution platforms tend to route all cross-node traffic through their own control plane, which in turn writes to a backing store. That model is uniform and tidy, but it has costs that show up at scale:

- Every RPC is a write. A status check or a polling read becomes a row in the history table.
- The database becomes the hot path for traffic that does not need to be durable. Read-heavy workloads against durable objects (live counters, "what is the current cursor?") amplify into write-heavy SQL.
- Latency floors are set by the database. A read that could be a 1 ms RPC call becomes a 10–50 ms round-trip through the orchestration service and storage layer.
- Backpressure and rate limits in the durable layer apply to traffic that should never have touched it.

```mermaid
flowchart TB
    subgraph durable["Durable command path — expose_command, signals (survives crashes)"]
        direction LR
        DC1["Caller"] -->|"1. INSERT inbox row"| DCDB[("inbox<br/>(SQL)")]
        DCDB -->|"2. owner's mailbox pulls"| DCO["Owner worker"]
        DCO -->|"3. apply, UPDATE result"| DCDB
        DCDB -.->|"4. caller polls result"| DC1
    end

    durable ~~~ simple

    subgraph simple["Simple RPC path — expose reads, wakeups (ephemeral)"]
        direction LR
        SC1["Caller"] -->|"1. SELECT lease"| SLDB[("lease table<br/>(SQL)")]
        SLDB -.->|"address"| SC1
        SC1 ==>|"2. HTTP/2 RPC call"| SO["Owner worker"]
        SO -.->|"3. result"| SC1
    end

    classDef sqlNode stroke:#d97706,stroke-width:3px
    class DCDB,SLDB sqlNode
```

Both paths end up at the same owning worker, but the durable path persists every step so the message survives crashes, while the simple-RPC path uses SQL only as a lookup and runs the call in-memory.

Durababble's stance is that durable execution is great for things that must survive crashes, and a bad fit for things that are fundamentally ephemeral. The mesh handles the ephemeral half.

## How The Mesh Works

The mesh piggybacks on infrastructure Durababble already needs for correct durable execution: leases.

To execute a workflow or process a durable object's inbox, a worker first claims a lease in the database. The lease row records `worker_id` and `locked_until` so that exactly one worker is doing the work at a time and stale ownership can be fenced. For production runtimes, `worker_id` is a compact identity such as `7f3a9c21d0ab@10.0.0.42:50051`: the prefix is a per-process random worker id, and the suffix is the reachable RPC address. That row is the address book and the incarnation fence, so any caller that can read the lease can dial the owner and tell the receiver which exact worker identity it expected.

A simple call flows like this:

1. Caller has a handle: `ReviewWorkflow.handle(run_id)`.
2. Caller invokes a method: `handle.label`.
3. The router reads `current_workflow_lease(workflow_id)` and gets back the owning worker's full identity.
4. The router opens (or reuses) an HTTP/2 connection to the address suffix and sends the call with the full identity as `expected_worker_id`.
5. The owning worker validates that its local identity matches `expected_worker_id`, validates that it still holds the lease, runs the handler, and returns the result.

```mermaid
sequenceDiagram
    autonumber
    participant C as Caller
    participant DB as Lease table
    participant O as Owning worker

    C->>C: handle = ReviewWorkflow.handle(run_id) then handle.label
    C->>DB: SELECT worker_id FROM lease WHERE workflow_id = run_id
    DB-->>C: 7f3a9c21d0ab@10.0.0.42:50051
    C->>O: HTTP/2 CallTransient(label, expected_worker_id)
    O->>O: validate - this message is for my worker id and I still hold this lease
    O-->>C: "ready for review"

    Note over C,O: If the lease moved, the caller gets StaleLease,<br/>re-reads the lease, and dials the new owner.
```

If the lease has moved (the owner crashed, the workflow was rescheduled), or if Kubernetes has recycled the old owner's address for a new worker with a different identity, the caller gets `StaleLease` or `NodeUnavailable` and can retry, which re-reads the lease and dials the new owner. The DB is consulted to find the owner, not to carry the payload.

For durable objects, the same flow applies. `Account.at("acct_123").balance` reads the object's lease, dials the owner, and returns the current state. `Account.at("acct_123").credit(1_000)` writes a row to the object's inbox and waits for the owning worker's mailbox loop to apply it.

## Retry Semantics

Routing layers (`WorkflowRpc::Router`, durable object handles) will retry an RPC **only** on typed routing failures — things the protocol knows are recoverable and that look benign to retry:

- `NodeUnavailable` — the peer is unreachable (connection refused, TCP timeout, HTTP/2 stream reset, returned status 503). The lease is re-read and the new owner is dialed.
- `StaleLease` — the lease moved while the call was in flight (server replied with a `moved` envelope, or the caller's `expected_worker_id` no longer matches). Same recovery path.
- `NoActiveLease` — the workflow or object is not currently leased. The router waits and retries.

**Unexpected raises on the peer are not retried.** When a handler raises something the protocol does not recognise, the server returns HTTP 500 and the client surfaces it as `Rpc::Error` (which is deliberately _not_ a subclass of `Rpc::Unavailable`). The router lets it propagate. This is on purpose: a bug that takes one peer's handler down should not be amplified into a stampede that takes the rest of the cluster with it. If the failure is genuinely transient, the caller (or the durable command machinery, for `expose_command` calls) gets to decide whether to retry; the RPC layer does not retry blindly.

The status-code mapping the transport uses:

| Status | Client raises                          | Retried by router?   |
| ------ | -------------------------------------- | -------------------- |
| 200    | (success — payload returned)           | —                    |
| 401    | `Rpc::Unauthenticated`                 | no                   |
| 500    | `Rpc::Error`                           | **no** (handler bug) |
| 503    | `Rpc::Unavailable` → `NodeUnavailable` | **yes** (peer down)  |
| other  | `Rpc::Error`                           | no                   |

503 is reserved for transport-level unavailability and is currently produced by the client's `with_rpc_errors` translation of socket / HTTP/2 errors, not by the server's request handler. A peer that reaches the handler and raises always gets 500.

## What This Buys

- **No write amplification.** Status reads, live counters, and warm-path RPCs do not write to SQL.
- **Latency is HTTP/2 latency.** Calls land on the lease holder directly; there is no orchestration hop.
- **The DB sees what it has to see.** Workflow history, step results, inbox messages, fences, outbox rows — the things that genuinely need to be durable. Not "what's your status?".
- **No new components to operate.** The mesh is HTTP/2 servers spun up inside the workers you were already running. There is no routing tier, no broker, no service registry. The address book is the lease table.
- **Free fencing.** Because the mesh reads lease rows, a request to a worker that has lost its lease fails fast with `StaleLease` instead of being silently mishandled by a zombie process.

## Wiring It Up

Production workers need a reachable address. The [install instructions](install.md#workers-and-cluster-addresses) cover the `rpc_host` / `rpc_port` arguments to `Durababble::WorkerRuntime`. The short version:

```ruby
Durababble::WorkerRuntime.start(
  store:,
  workflows: [FulfillOrder],
  objects: [Account],
  worker_pool: "orders",
  rpc_host: ENV.fetch("POD_IP"),
  rpc_port: 50_051,
)
```

Single-process scripts and tests can stick with `Durababble::Worker.new(...)` and skip the mesh entirely; in that mode RPCs are routed in-process. The mesh only matters when more than one worker is running and they need to address each other.

## What Is Still Aspirational

### Transport Security

The current transport is **cleartext HTTP/2 (h2c) carrying Paquito/Marshal payloads, with no peer authentication**. This is acceptable on a closed pod network where the cluster operator already trusts every other peer at the network layer — and that's how Durababble is expected to be deployed today — but it is _not_ a hardened wire protocol.

Two limitations to understand before exposing the RPC port to anything untrusted:

- **Marshal is the wire format.** A peer that can write to a worker's RPC socket can also feed it arbitrary Ruby objects, which `Marshal.load` will instantiate. Anything that reaches the transport must be on the same trust boundary as the worker process itself.
- **There is no built-in authentication.** The `authorize:` hook on `Rpc::Server` is the only opt-in check, and it runs over a cleartext channel — useful for cheap "is this peer in my pool?" tags but not a substitute for transport-level identity.

The intended hardening path is to layer real authenticated transport — mTLS at minimum, ideally SPIFFE-style workload identity — under the existing HTTP/2 transport. In Shopify-style deployments this is typically provided by the infrastructure (sidecar mTLS, service-mesh identity) rather than by Durababble itself, so the gem's job is to be _compatible_ with that, not to ship its own crypto. A future version will document the expected sidecar / mesh setup and may grow a built-in TLS configuration for deployments that don't have one of those.

Until then: keep the RPC port off the internet, keep it off shared networks, and rely on infrastructure-level isolation between worker pools.

### Other In-Progress Surfaces

The HTTP/2 transport, peer routing, lease-keyed addressing, stale-lease retries, and the no-retry-on-unexpected-raise policy described above are implemented and exercised by the test matrix. Admin surfaces, richer health and routing observability for the mesh, and the transport-security hardening above are target work — see [the reference](reference.md) for the current prototype boundary.
