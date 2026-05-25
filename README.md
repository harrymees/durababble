# Durababble

Durababble is a Ruby durable execution library for workflows and durable objects that persist progress in an application database.

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

## Development Notes

Query-plan and benchmark coverage are part of the storage contract. When adding production SQL for hot queue, lease, wait, outbox, durable-object, or inbox-shaped paths, define it in `Durababble::StoreQueries`, add PostgreSQL/YSQL and MySQL/MariaDB plan coverage where practical, and extend `bench/run.rb` for benchmark coverage. If a registered query is intentionally not covered by the large-fixture EXPLAIN suite, add it to the explicit uncovered-query list in the query-plan test so the exemption is visible in review.

The Zeitung documentation source lives in `docs/content/`; see [docs/README.md](docs/README.md) for local preview and build commands.
