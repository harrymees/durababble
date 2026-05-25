# Durababble

Durababble is a Ruby durable execution library for workflows and durable objects that persist progress in an application database. Configure a default store once with `Durababble.configure`, then enqueue workflows with `SomeWorkflow.enqueue(...)` / `SomeWorkflow.start(...)` and address durable objects with `SomeObject.at("id")`.

The docs site source starts at [docs/content/README.md](docs/content/README.md). From there, use the docs navigation for workflows, durable objects, storage, observability, testing, and reference material.

## Documentation

- [Docs site introduction](docs/content/README.md)
- [Quickstart](docs/content/quickstart.md)
- [Install Instructions](docs/content/install.md)
- [Workflows](docs/content/workflows.md)
- [Durable Objects](docs/content/durable-objects.md)
- [Storage](docs/content/storage.md)
- [Observability](docs/content/observability.md)
- [Testing](docs/content/testing.md)
- [Reference](docs/content/reference.md)
- [Spec](docs/spec.md)
- [Architecture](docs/content/architecture.md)
- [Comparisons](docs/content/why-not-background-jobs.md)
- [Deterministic Testing](docs/deterministic-testing.md)
- [Benchmarks](bench/README.md)

The Zeitung documentation source lives in `docs/content/`; see [docs/README.md](docs/README.md) for local preview and build commands.
