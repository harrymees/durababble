---
name: durababble-typing
description: |
  Tighten or validate Durababble's public RBS/static type contract. Use when
  editing sig/durababble.rbs, public API types, docs with typed snippets, or the
  strict no-untyped validation path.
---

# Durababble Typing

Use this skill when work touches public RBS signatures, typed documentation
examples, or the static contract validation path.

## Public Contract Rules

- Runtime code must not load or validate user RBS. The RBS files are for static
  tooling and public API documentation only.
- Keep payload serialization Paquito-backed unless the implementation and spec
  deliberately change together.
- Do not add `untyped` to committed public signatures or documented code
  examples. Prefer generics, type aliases, unions, interfaces, or explicit
  serialized-payload aliases.
- Keep `sig/durababble.rbs` aligned with public workflow, durable object,
  engine/store, wait, retry policy, worker runtime, RPC, and CLI-facing APIs.
- Keep `docs/spec.md`, `docs/architecture.md`, and `README.md` aligned when an
  API, storage guarantee, behavior, or operational expectation changes.

## Validation

Run the static gate through `mise`:

```sh
mise exec -- bundle exec rake typecheck
```

That task runs:

- `rbs validate` for signature syntax and name resolution.
- `scripts/validate_rbs_strict.rb`, which rejects `untyped` in public RBS/RBI
  files and fenced README/docs/skill examples.
- Sorbet against inline implementation signatures.

For a broader local gate, run:

```sh
mise exec -- bundle exec rake lint
```

## Tooling Decision

The current prototype uses `rbs validate`, the repo-local strict gate, and
Sorbet inline checking. Do not add Steep or TypeProf casually: Steep needs a
larger public/private signature split before it provides useful implementation
checking here, and TypeProf inference is too broad for the metaprogrammed
`step`, `expose`, and proxy surfaces.
