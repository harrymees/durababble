# Paquito storage correction

Review date: 2026-05-21

## Finding

The earlier storage implementation drifted from the intended design: runtime values were stored in Yugabyte as `jsonb` and encoded/decoded with Ruby's `JSON` library. That contradicted the expected Paquito-based serialization path.

This was an implementation/spec-documentation divergence, not a reason to change the intended design. The implementation has been updated to use Paquito.

## Correction

`Durababble::Store` now serializes runtime values with:

```ruby
Paquito::SingleBytePrefixVersion.new(1, 1 => Marshal)
```

The serialized bytes are stored in YSQL `bytea` columns for:

- `workflows.input`
- `workflows.result`
- `steps.result`
- `step_attempts.result`
- `waits.context`
- `waits.payload`
- `fences.result`
- `outbox.payload`

Text columns such as workflow names, step names, topics, keys, and errors remain text and are never deserialized as runtime payloads.

## Migration behavior

`Store#migrate!` creates new schemas with Paquito `bytea` columns. If it sees the earlier prototype's `jsonb` runtime columns, it converts existing values into Paquito bytea columns in-place.

The only remaining JSON use in `Store` is the legacy JSONB migration reader for old prototype schemas. New writes are Paquito bytea writes.

## Test coverage

The store specs now explicitly verify:

- runtime value columns are `bytea`, not `jsonb`;
- raw stored bytes have the Paquito version prefix and decode through the Paquito serializer;
- a legacy JSONB workflow row migrates to Paquito bytea while preserving input/result values.
