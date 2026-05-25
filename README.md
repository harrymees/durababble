# Durababble

Durababble is a Ruby durable execution library for workflows and durable objects that persist progress in an application database.

The docs site source starts at [docs/content/README.md](docs/content/README.md). From there, use the docs navigation for workflows, durable objects, storage, observability, testing, and reference material.

## Documentation

- [Docs site introduction](docs/content/README.md)
- [Quickstart](docs/content/quickstart.md)
- [Installation](docs/content/install.md)
- [Workflows](docs/content/workflows.md)
- [Durable Objects](docs/content/durable-objects.md)
- [Object Patterns](docs/content/object-patterns.md)
- [Storage](docs/content/storage.md)
- [Observability](docs/content/observability.md)
- [Testing](docs/content/testing.md)
- [Reference](docs/content/reference.md)
- [Spec](docs/spec.md)
- [Architecture](docs/content/architecture.md)
- [Comparisons](docs/content/comparisons.md)
- [Deterministic Testing](docs/deterministic-testing.md)
- [Benchmarks](bench/README.md)

The Zeitung documentation source lives in `docs/content/`; see [docs/README.md](docs/README.md) for local preview and build commands.

## Workflow Replay Bounds

Durababble bounds workflow replay by counting persisted `workflow_history` rows before loading replay payloads. The limit defaults to `10_000` events and can be tuned with `DURABABBLE_MAX_WORKFLOW_HISTORY_EVENTS` or `Durababble.max_workflow_history_events = 20_000`. When an open workflow exceeds the limit, resume fails durably with `Durababble::WorkflowHistoryLimitExceeded` and the workflow becomes terminal `failed`; completed/canceled/failed workflows are returned as-is. Operationally, treat this as a workflow design or retention signal: split very long workflows into child runs or smaller durable objects, compact completed history through a deliberate retention tool, or raise the limit only after benchmarking replay latency with `mise exec -- ruby bench/run.rb --profile history-smoke`.
