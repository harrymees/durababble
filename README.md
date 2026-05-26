# Durababble

Durababble is a Ruby durable execution library for workflows and durable objects that persist progress in an application database. Workflow handles support cooperative cancellation and operator hard termination; the docs site covers the API and storage semantics.

Workflow starts may use generated ids or caller-provided ids; see [Workflows](docs/content/workflows.md) and [Spec](docs/spec.md) for the enqueue contract.

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
