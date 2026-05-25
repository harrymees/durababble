---
title: "Observability"
weight: 55
---

# Observability

Durababble observability is optional and disabled by default. Disabled instrumentation only executes cheap no-op checks. Durababble depends on the official OpenTelemetry API gems for tracing and metrics, but it does not choose or configure an SDK, collector, or exporter.

Applications that already configure the OpenTelemetry SDK can enable Durababble instrumentation against the global OpenTelemetry providers:

```ruby
require "opentelemetry/sdk"
require "opentelemetry/exporter/otlp"

OpenTelemetry::SDK.configure do |c|
  c.service_name = "my-app"
  c.use_all
end

Durababble.configure_observability(
  enabled: true,
  attributes: { "deployment.environment" => ENV.fetch("RACK_ENV", "development") },
)
```

If `enabled: true` is used before an SDK is configured, OpenTelemetry's API-level no-op providers are used. That lets tests and local runs exercise instrumentation without a collector while production apps can add `opentelemetry-sdk`, `opentelemetry-metrics-sdk`, and exporters in their own boot code.

Local OTLP smoke example:

```sh
docker run --rm -p 4317:4317 -p 4318:4318 otel/opentelemetry-collector:latest
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 mise exec -- bundle exec ruby examples/your_app.rb
```

Stable spans include `durababble.workflow.start`, `durababble.workflow.resume`, `durababble.workflow.execute`, `durababble.workflow.step`, `durababble.object.query`, `durababble.object.command.enqueue`, `durababble.object.command`, `durababble.workflow_rpc.route`, `durababble.workflow_rpc.handle`, `durababble.rpc.client.*`, and `durababble.rpc.server.*`. Durababble does not wrap ActiveRecord SQL execution in its own spans; applications should use standard ActiveRecord/database OpenTelemetry instrumentation for SQL visibility.

Stable metrics include workflow start/completion/failure counters, step attempt/success/failure/retry counters, wait start/completion counters and latency histograms, queue claim latency, lease heartbeat/conflict/recovery counters, outbox pending/processed counters, worker tick duration/counts, and workflow replay/history size measurements. Applications should use ActiveRecord/database OpenTelemetry instrumentation for SQL operation latency/error metrics.
