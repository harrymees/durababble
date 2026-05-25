---
title: "Testing"
weight: 50
---

# Testing

Durability-sensitive changes should be tested against a real database. The local suite defaults to MySQL and derives an isolated namespace for each workspace unless `DURABABBLE_SCHEMA` is set.

```shell
mise exec -- bundle exec rake test
```

Set `DURABABBLE_YUGABYTE_DATABASE_URL` when a change needs the optional PostgreSQL-compatible YugabyteDB path. For the host-local Symphony smoke path, use `DURABABBLE_DATABASE_URL=postgresql://yugabyte@127.0.0.1:15433/yugabyte`.

Documentation examples are executable where they describe public API behavior, and the focused documentation check runs them against the implemented API:

```shell
mise exec -- env DURABABBLE_YUGABYTE_DATABASE_URL= bundle exec ruby -Ilib -Itest test/durababble/documentation_test.rb
```

Storage and recovery changes should keep backend conformance coverage honest. Prefer tests that exercise persisted workflow rows, step history, waits, leases, object command rows, and replay behavior over tests that only assert metadata.
