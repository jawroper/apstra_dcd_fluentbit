# promexport

Native Prometheus exporter for DCD telemetry — a standalone HTTP `/metrics`
endpoint, built into the plugin itself, completely independent of Fluent
Bit's `[OUTPUT]` mechanism.

## Why this isn't a Fluent Bit `[OUTPUT]` block

Fluent Bit has two separate, non-interoperable internal pipelines: **logs**
and **metrics**. This plugin (`apstra_dcd_fluentbit`) is a classic log-type input
— it uses the `FLBPluginInputCallback`/`FLBPluginInputCallbackCtx` msgpack
API, the same one `tail`, `tcp`, `syslog`, etc. use. Every record it
produces lives in the logs pipeline.

Fluent Bit's `prometheus_exporter` and `prometheus_remote_write` **output**
plugins only read from the separate metrics pipeline, populated by
metrics-type *input* plugins like `node_exporter_metrics`. They cannot
consume arbitrary log records — this is gated at the plugin architecture
level (an internal "supported event types" tag), not by inspecting record
shape at runtime. No `[OUTPUT]` configuration, record format, or filter
changes that.

So: **there is no `[OUTPUT]` block for Prometheus.** This package runs its
own embedded HTTP server instead, and an external Prometheus server scrapes
it directly.

```
DCD server ──(binary protobuf)──▶ port 7777  (inbound — DCD → plugin)
                                       │
                                  decode + build record
                                       │
                          ┌────────────┴────────────┐
                          ▼                          ▼
              Fluent Bit pipeline          promexport HTTP server
           (file / influxdb / stdout /      port 9112, path /metrics
            elasticsearch / ... via          (outbound — Prometheus
            your [OUTPUT] blocks)             pulls FROM the plugin)
```

Port 7777 and the Prometheus port serve opposite directions of opposite
protocols — one is DCD pushing data in, the other is Prometheus pulling data
out. They're unrelated except for both living inside the same plugin process.

## Every record goes to both places, always

When `prometheus_enabled on` is set, every record passes through
`Decoder.MakeRecord`, which unconditionally forwards to both the embedded
exporter *and* the normal Fluent Bit pipeline. Your existing `[OUTPUT]`
blocks (InfluxDB, file, stdout, Elasticsearch, whatever) keep working
exactly as before, completely unaffected:

```go
func (d *Decoder) MakeRecord(series string, tags, fields, ts) Record {
    if d.metrics != nil {
        d.metrics.Observe(series, tags, fields)   // → Prometheus exporter
    }
    ...
    return makeFlatRecord(...)   // → Fluent Bit pipeline → your [OUTPUT]s
}
```

## Enabling it — one `[INPUT]` block is all you need

```ini
[INPUT]
    Name                     apstra_dcd
    dcd_release              6.0.0
    dcd_server               192.168.57.250
    local_address            192.168.57.128
    port                     7777
    streaming_types          perfmon, alerts, events
    output_format            json
    tag                      apstra.dcd
    prometheus_enabled       on
    prometheus_port          9112
    # prometheus_streaming_types  perfmon        ← optional, see below

[OUTPUT]
    Name        influxdb
    Match       apstra.dcd
    Auto_Tags   On
    ...
```

This single block gives you InfluxDB *and* Prometheus at the same time — no
second instance, no second port.

Verify it's serving (on the Fluent Bit host, once some data has flowed):

```bash
curl http://localhost:9112/metrics
```

Then point a Prometheus server at it:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: dcd
    static_configs:
      - targets: ['<fluent-bit-host>:9112']
```

## Controlling what Prometheus sees: `prometheus_streaming_types`

By default, Prometheus exports whatever `streaming_types` DCD is subscribed
to send — if you subscribe to `perfmon, alerts, events`, Prometheus gets all
three. If you want Prometheus to see only a subset (or a different set),
use `prometheus_streaming_types`:

```ini
[INPUT]
    Name                        apstra_dcd
    streaming_types             perfmon, alerts, events   ← what DCD sends
    prometheus_enabled          on
    prometheus_streaming_types  perfmon                   ← what Prometheus sees
    ...
```

| Value | Effect |
|-------|--------|
| not set (default) | Prometheus sees whatever `streaming_types` is set to |
| `perfmon` | Only `interface_counters`, `system_info`, `process_info`, `file_info`, `probe_message`, etc. |
| `alerts` | Only `alert_bgp_neighbor_mismatch`, `alert_liveness`, `alert_lag`, etc. |
| `events` | Only `event_link_status`, `event_bgp_neighbor`, `event_device_state`, etc. |
| `perfmon, alerts` | Any combination works — comma-separated, same syntax as `streaming_types` |

**Corner case — requesting a type not in `streaming_types`**: if
`prometheus_streaming_types` includes a type that `streaming_types` doesn't
subscribe to (e.g. `streaming_types = alerts, events` but
`prometheus_streaming_types = perfmon`), the plugin **automatically adds the
missing type to its DCD subscription** at startup and logs it clearly:

```
I! [dcd] prometheus_streaming_types includes "perfmon" which is not in
   streaming_types — automatically adding it so DCD will send this data
   to the Prometheus exporter
```

The extra type flows in on the same TCP listener (port 7777). Whether it
also reaches your Fluent Bit `[OUTPUT]` blocks depends on your `tag`/`Match`
routing as usual — it's available in the pipeline, it just won't match a
`[OUTPUT]` that isn't looking for it.

### Why there's no two-`[INPUT]`-blocks approach

The conceptually obvious way to give Prometheus a different type subset
would be to run a second `[INPUT]` block with its own `port`, its own
`streaming_types`, and `prometheus_enabled on`. This doesn't work on current
Fluent Bit versions: `FLBPluginConfigKey` on a second instance of the same
Go input plugin returns empty or wrong values for config keys — so the
second instance can't correctly read its own `port` (falls back to the
default `7777`, which the first instance already owns) or its own
`streaming_types`. `prometheus_streaming_types` on the *same* `[INPUT]`
block is the correct, working solution to this.

## Scope: PerfMon, alerts, and events — all of it

Every series this plugin decodes can be exported: PerfMon
(`interface_counters`, `system_info`, `process_info`, `file_info`,
`probe_message`, ...), alerts (`alert_bgp_neighbor_mismatch`,
`alert_liveness`, ...), and events (`event_link_status`,
`event_bgp_neighbor`, ...). Use `prometheus_streaming_types` to narrow this
if needed.

This matches how Telegraf-based DCD Prometheus exporters (e.g. the
[dcdom](https://github.com/Apstra/dcdom) project) work in production —
including alerts as Prometheus gauges — via the same architecture: a
standalone server that never goes through Fluent Bit's logs/metrics
pipeline distinction at all.

## Metric naming

Every numeric field becomes its own gauge automatically, named
`dcd_<series>_<field>`:

```
dcd_interface_counters_tx_bytes
dcd_interface_counters_rx_bytes
dcd_system_info_cpu_user
dcd_process_info_<whatever ProcessInfo reports>
dcd_probe_message_<whatever a given probe reports>
dcd_alert_bgp_neighbor_mismatch_status
dcd_alert_liveness_status
dcd_event_link_status_event
```

This is fully automatic — adding a new DCD release or a new field never
requires touching this package. A genuinely *new* top-level PerfMon oneof
case (e.g. a hypothetical future `flow_telemetry`) needs one new case in
the relevant `pkg/decoder/vX_Y_Z/decoder.go` — once that's emitting records
normally, this package picks it up automatically.

## Labels

**PerfMon metrics** carry the fixed label set:

```
device, device_key, interface, role, blueprint, device_name,
process_name, file_name, probe_id, stage_name, item_id, probe_label
```

Any label that doesn't apply defaults to `""` (e.g. `system_info` has no
`interface`).

**Alert and event metrics** each get their own label set, derived
automatically from that specific type's own tag keys the first time it's
seen — `alert_bgp_neighbor_mismatch` gets `actual_state`, `expected_state`,
`lcl_asn`, `rmt_asn`, etc.; a different alert type gets a completely
different, independent set. Two alert/event types never share or leak labels
into each other.

**Known limitation**: truly dynamic tag *keys* — e.g. `probe_message`'s
`prop_<name>` tags, where the key name itself varies per probe — can't become
Prometheus labels. Prometheus requires a label's name to be fixed in the
metric's descriptor. That data remains available through the normal Fluent
Bit log pipeline (InfluxDB, Elasticsearch, etc.).

## Troubleshooting

- **`curl localhost:9112/metrics` connection refused** — check
  `prometheus_enabled` is `on` and look for
  `I! [dcd] Prometheus exporter listening on :9112/metrics` at startup. If
  that line is missing or shows `E! [dcd] Could not start Prometheus
  exporter`, something else is already bound to that port.
- **Endpoint responds but a metric you expect is missing** — check
  `prometheus_streaming_types` isn't excluding it. Also confirm data is
  flowing at all; this exporter only has something to show once at least one
  matching record has been observed.
- **A series you expect isn't there** — DCD may be sending a PerfMon
  sub-type the decoder doesn't yet have an explicit case for. Check the
  Fluent Bit log for `W! [dcd] DecodeMessage: ... oneof ... has field #N set
  on the wire` — these are always logged regardless of the `debug` setting.
