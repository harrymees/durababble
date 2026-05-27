---
title: "Testing"
weight: 50
---

# Testing

Durability-sensitive changes should be tested against a real database. The local suite defaults to MySQL and derives an isolated namespace for each workspace unless `DURABABBLE_SCHEMA` is set.

```shell
mise exec -- bundle exec rake test
```

Set `DURABABBLE_TEST_BACKENDS=postgres` with `DURABABBLE_POSTGRES_DATABASE_URL` to run the standard PostgreSQL backend locally or in CI. CI runs the coverage-gated suite once with `DURABABBLE_TEST_BACKENDS=mysql` and once with `DURABABBLE_TEST_BACKENDS=postgres`; the host-local Thompson smoke path is `DURABABBLE_DATABASE_URL=postgresql://postgres@127.0.0.1:5432/postgres DURABABBLE_POSTGRES_DATABASE_URL=postgresql://postgres@127.0.0.1:5432/postgres`.

Use `DURABABBLE_TEST_BACKENDS` to narrow backend coverage when needed. Supported values are `mysql`, `postgres`, and `yugabyte`, either as one name or as a comma-separated list.

Set `DURABABBLE_YUGABYTE_DATABASE_URL` when a change needs the optional PostgreSQL-compatible YugabyteDB path. Yugabyte remains available for targeted YSQL coverage, but it is not the required CI PostgreSQL leg.

Documentation examples are executable where they describe public API behavior, and the focused documentation check runs them against the implemented API:

```shell
mise exec -- env DURABABBLE_YUGABYTE_DATABASE_URL= bundle exec ruby -Ilib -Itest test/durababble/documentation_test.rb
```

Storage and recovery changes should keep backend conformance coverage honest. Prefer tests that exercise persisted workflow rows, step history, waits, leases, object command rows, and replay behavior over tests that only assert metadata.
