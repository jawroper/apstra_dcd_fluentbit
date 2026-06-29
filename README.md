# Apstra Data Center Director (DCD) Telemetry Streaming — Fluent Bit Input Plugin

A Go port of the Apstra AOSOM github repo (https://github.com/Apstra/aosom-streaming), implemented as a native Fluent Bit input plugin using the [fluent-bit-go](https://github.com/fluent/fluent-bit-go) SDK.

---

## Architecture

```
DCD Server
   │  GPB protobuf frames over TCP
   │  [2-byte big-endian length][N-byte DcdSequencedMessage, wrapping DcdMessage]
   ▼
┌─────────────────────────────────────────────────────┐
│  apstra_dcd_fluentbit.so  (this plugin)             │
│                                                     │
│  FLBPluginInit                                      │
│    ├── restapi.Client.Login()                       │
│    ├── restapi.Client.GetBlueprints/GetSystems()    │
│    ├── listener.Listener.Start()  ← TCP port 7777   │
│    ├── restapi.Client.StartStreaming() × N types    │
│    └── goroutine: metadata refresh every 30s        │
│                                                     │
│  perfmon / alerts / events ALL flow through the     │
│  SAME decode → record → channel → callback path —   │
│  there is no internal type-based routing. Every     │
│  record (regardless of series, e.g.                 │
│  "interface_counters", "alert_liveness",            │
│  "event_link_status") goes to every configured      │
│  [OUTPUT] that matches the [INPUT]'s Tag.           │
│                                                     │
│  FLBPluginInputCallbackCtx (called by Fluent Bit)   │
│    └── drain listener.Records channel               │
│         └── encoder.AppendObject(ts, record)        │
└──────────────────────┬──────────────────────────────┘
                       │  Fluent Bit MsgPack records,
                       │  one Fluent Bit tag for all of them
                       ▼
            Whatever [OUTPUT] plugin(s) you configure
         (InfluxDB, Elasticsearch, stdout, S3, Loki, ...) —
       this is a normal downstream Fluent Bit routing choice,
            not something this plugin decides internally.
```

**Sending everything to InfluxDB**: perfmon, alerts, and events are just
records like any other to Fluent Bit, and one `[OUTPUT] influxdb` block with
`Match *` (or matching this `[INPUT]`'s `Tag`) picks up all of them.
See **[Output formats & InfluxDB](#output-formats--influxdb)** below for the
config details (record format choice + `Auto_Tags`), since getting this right
requires a couple of non-obvious settings.

### Package layout

```
apstra_dcd_fluentbit/
├── cmd/plugin/
│   └── main.go                 # FLBPluginRegister/Init/InputCallback/Exit
│                                # releaseHandlers map dispatches by dcd_release
│                                # uses pkg/cbuf for the C-engine data handoff
├── pkg/
│   ├── cbuf/
│   │   ├── cbuf.go             # safe Go→C buffer handoff (cgo memory-lifetime
│   │   │                       # fix — see its package doc comment)
│   │   └── cbuf_test.go
│   ├── config/
│   │   └── config.go           # reads [INPUT] params from fluent-bit.conf
│   ├── restapi/
│   │   ├── restapi.go          # DCD REST API client (login, blueprints, systems, streaming)
│   │   └── test_helpers.go     # InjectSystem/InjectBlueprint for unit tests
│   ├── decoder/
│   │   ├── decoder.go          # version-independent: Decoder, GetTags, MakeRecord,
│   │   │                       # ProtoToFields, IsNilProto, CopyTags
│   │   ├── json_helpers.go     # proto→JSON marshaling shim
│   │   ├── decoder_test.go     # GetTags tests (version-independent)
│   │   ├── v6_0_0/              # DCD 6.0.0 message dispatch (DecodeMessage, Extract*)
│   │   └── v6_1_2/              # DCD 6.1.2 message dispatch (DecodeMessage, Extract*)
│   ├── listener/
│   │   └── listener.go         # TCP server, 2-byte length-prefix frame reader
│   │                            # (version-agnostic — takes a MessageHandler)
│   └── promexport/
│       ├── promexport.go       # native Prometheus /metrics exporter (perfMon + alerts + events)
│       ├── promexport_test.go
|       └── README.md
├── proto/
│   ├── README.md                # naming convention + how to add a new DCD release
│   ├── v6_0_0/                  # DCD 6.0.0 .proto + generated .pb.go
│   └── v6_1_2/                  # DCD 6.1.2 .proto + generated .pb.go
├── deployments/
│   ├── Dockerfile              # three-stage: fluent-extract + builder + debian:12-slim runtime
│   ├── fluent-bit.conf         # example configuration with all output options
│   └── plugins.conf            # tells Fluent Bit where to load the .so
├── Makefile
└── go.mod
```

DCD renumbers (and sometimes restructures) its streaming telemetry schema
between releases, so a single static schema can't correctly decode every
version's wire bytes. Each `[INPUT]` block declares which DCD release its
server runs via `dcd_release` (e.g. `6.0.0`), and the plugin picks the
matching decoder package at startup. See **[proto/README.md](proto/README.md)**
for the full naming convention and how to add support for a new release.

---

## Record format

Each record emitted to Fluent Bit is a flat map. The `series` key carries the
measurement name, mirroring the series name from the original plugin. If a
key:value pair has an empty value, it will **not** be emitted as part of the
record.

```json
{
  "series":     "interface_counters",
  "device_key": "50:01:00:01:00:00",
  "device":     "spine1",
  "role":       "spine",
  "blueprint":  "prod-dc",
  "interface":  "et-0/0/0",
  "in_octets":  123456789,
  "out_octets": 987654321
}
```

### Series names

| Series                      | Source                          | Key fields                    |
|-----------------------------|---------------------------------|-------------------------------|
| `interface_counters`        | PerfMon – interface counters    | in/out octets, pkts, errors   |
| `system_info`               | PerfMon – system resources      | cpu_percent, memory_percent   |
| `process_info`              | PerfMon – per-process           | process_name, cpu/mem percent |
| `file_info`                 | PerfMon – file sizes            | file_name, size               |
| `probe_message`             | PerfMon – DCD analytics probes  | value, name, metadata         |
| `<data_type>`               | PerfMon – generic               | dynamic tag/field names       |
| `event_<type>`              | Events                          | event=1, all event fields→tags|
| `alert_<type>`              | Alerts                          | status=1/0, severity          |

Alert `status`: `1` = raised, `0` = cleared.

### Timestamp sanity checking

Every record's timestamp comes from DCD's embedded `DcdMessage.timestamp`
field — except when that value is implausible, in which case the plugin
silently falls back to its own receipt time (`time.Now()`) instead, and logs
a warning. This isn't theoretical: in production, release 6.0.0 produced
some records from a stale or different clock source used specifically by its
streaming telemetry subsystem, separate from the system clock.

"Implausible" means more than 24 hours off from this host's wall-clock time
in either direction (`decoder.MaxClockSkew`, in `pkg/decoder/decoder.go`) —
generous enough to never trigger on legitimate NTP-managed clock drift
(normally sub-second to a few seconds), strict enough to catch exactly the
kind of multi-year clock-source bug seen above. When it triggers, you'll see:

```
W! [dcd] DCD-provided timestamp 2007-03-03T22:17:01Z is implausible (19y... from wall-clock time) — using plugin receipt time instead (origin="WS3717090026")
```

If you see this warning, it's worth checking DCD's own clock health, but
it's not something this plugin can fix on DCD's behalf — the fallback to
receipt time is the safety approach. It is **not** a corrective measure.

### Output formats

`output_format` (an `[INPUT]` config key) controls the *internal* shape of
each record — it doesn't change which series types get emitted, just how
they're structured:

- `json` (recommended, shown above) — flat map, every field at the top level.
- `msgpack` — nested `{"series", "_tags": {...}, "_fields": {...}}`. Only
  useful for a custom downstream consumer that specifically understands this
  convention (e.g. a Lua filter you write yourself). **Fluent Bit's stock
  InfluxDB output plugin does not read this nested format** — use `json` for
  InfluxDB.

### Sending everything to InfluxDB

perfmon, alerts, and events are not routed differently by this plugin —
they're all just records to Fluent Bit, distinguished only by their `series`
value. One `[OUTPUT] influxdb` block picks up all of them:

```ini
[INPUT]
    Name          apstra_dcd
    dcd_release   6.1.2
    dcd_server    192.168.57.250
    local_address 192.168.57.128
    port          7777
    output_format json
    tag           apstra.dcd

[OUTPUT]
    Name        influxdb
    Match       apstra.dcd
    Host        127.0.0.1
    Port        8086
    Database    fluentbit
    Auto_Tags   On
```

`Auto_Tags On` works cleanly here because every tag-like value this plugin
produces (`device`, `role`, `blueprint`, `interface`, `series`, `severity`,
and so on) is always a string, and every actual measurement value (byte
counters, `status`, `event`/`probe` markers, CPU/memory percentages) is
always numeric — so InfluxDB's "tag every string field" rule lines up exactly
with this plugin's own tag/field split, with nothing to hand-maintain as new
event or alert sub-types show up.

One real limitation to know about: InfluxDB's *measurement* name comes from
the Fluent Bit **tag** set on `[INPUT]` (a single static string — `apstra.dcd`
above), not from anything inside the record. So by default, every series type
lands in **one** InfluxDB measurement (`apstra.dcd`), distinguished by the
`series` tag — e.g. `SELECT * FROM "apstra.dcd" WHERE series='alert_liveness'`.

If you'd rather have separate InfluxDB measurements per series
(`interface_counters`, `alert_liveness`, etc.), add a `rewrite_tag` filter
that sets the Fluent Bit tag from each record's `series` field before the
output runs:

```ini
[FILTER]
    Name          rewrite_tag
    Match         apstra.dcd
    Rule          $series ^(.*)$ apstra.$1 false
    Emitter_Name  apstra_retag

[OUTPUT]
    Name        influxdb
    Match       apstra.*
    Host        127.0.0.1
    Port        8086
    Database    fluentbit
    Auto_Tags   On
```

### Sending DCD telemetry to Prometheus

Prometheus is **not** reachable through either output format above — Fluent
Bit's `prometheus_exporter` and `prometheus_remote_write` outputs only read
from Fluent Bit's internal *metrics* pipeline, which is completely separate
from the *logs* pipeline this plugin (a log-type input) populates.

Instead, this plugin runs its own embedded Prometheus exporter — a
standalone HTTP server, independent of Fluent Bit's pipeline entirely, that
an external Prometheus server scrapes directly. **One `[INPUT]` block is all
you need** — records flow to Fluent Bit's normal pipeline *and* to the
Prometheus exporter simultaneously:

```ini
[INPUT]
    Name                        apstra_dcd
    dcd_release                 6.0.0
    dcd_server                  192.168.57.250
    local_address               192.168.57.128
    port                        7777
    streaming_types             perfmon, alerts, events
    output_format               json
    tag                         apstra.dcd
    prometheus_enabled          on
    prometheus_port             9112
    # prometheus_streaming_types  perfmon   ← optional: subset for Prometheus
```

`prometheus_streaming_types` lets you control what Prometheus sees
independently of what `streaming_types` subscribes from DCD — e.g. subscribe
to all three from DCD but export only `perfmon` to Prometheus. When not set,
defaults to whatever `streaming_types` is configured to. Accepts the same
comma-separated values: `perfmon`, `alerts`, `events`, or any combination.

> **Note:** Separating prometheus into its own `[INPUT]` block for this purpose
> don't work on current Fluent Bit versions. Use only a single `[INPUT]` block.


To test if the internal prometheus HTTP server is seeing data:

```bash
curl http://127.0.0.1:9112/metrics
```

```
# HELP dcd_interface_counters_tx_bytes DCD interface_counters.tx_bytes (auto-generated)
# TYPE dcd_interface_counters_tx_bytes gauge
dcd_interface_counters_tx_bytes{device="spine1",interface="et-0/0/1",role="spine",...} 987654321

# HELP dcd_alert_bgp_neighbor_mismatch_status DCD alert_bgp_neighbor_mismatch.status (auto-generated)
# TYPE dcd_alert_bgp_neighbor_mismatch_status gauge
dcd_alert_bgp_neighbor_mismatch_status{device="leaf2",actual_state="BGP_SESSION_DOWN",...} 1
```

Full details (architecture, label design, filtering, troubleshooting) are in
[pkg/promexport/README.md](pkg/promexport/README.md).

### Quieter logging

`debug` (default `off`) gates a handful of high-frequency diagnostic log
lines (periodic heartbeat/queue-depth confirmations, raw message byte
previews) that are useful while troubleshooting but unnecessary noise once
things are working. Genuine errors and warnings are always logged regardless
of this setting.

```ini
[INPUT]
    Name  apstra_dcd
    debug off
```

---

## Installation

### Prerequisites

**To build the plugin**, only Go and a C compiler are required — `fluent-bit-go`'s
cgo bridge bundles its own headers, so no Fluent Bit packages are needed just to
compile `apstra_dcd_fluentbit.so`:

```bash
sudo apt update
sudo apt install -y build-essential golang-go   # Debian/Ubuntu
# or: brew install go                            # macOS
```

**To run/test the plugin**, you need the actual `fluent-bit` binary to load it.
Fluent Bit isn't in the default Debian/Ubuntu repos, so add their APT source first
(swap `ubuntu` for `debian` in the URL below if applicable):

```bash
sudo apt install -y curl gpg
curl https://packages.fluentbit.io/fluentbit.key | gpg --dearmor | \
  sudo tee /usr/share/keyrings/fluentbit-keyring.gpg > /dev/null

codename=$(grep -oP '(?<=VERSION_CODENAME=).*' /etc/os-release)
echo "deb [signed-by=/usr/share/keyrings/fluentbit-keyring.gpg] https://packages.fluentbit.io/ubuntu/$codename $codename main" | \
  sudo tee /etc/apt/sources.list.d/fluent-bit.list

sudo apt update
sudo apt install -y fluent-bit
```

**Only needed if you modify a `.proto` schema or add support for a new DCD
release** — every currently-supported release's `.pb.go` is already generated
and committed to the repo, so this is *not* required for a normal build:

```bash
sudo apt install -y protobuf-compiler
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
export PATH="$PATH:$(go env GOPATH)/bin"
```

### 1. (Optional) Regenerate the proto bindings

Each DCD release has its own `.proto` file and generated Go package under
`proto/vX_Y_Z/` — see **[proto/README.md](proto/README.md)** for the full
naming convention. Skip this step unless you've changed a schema or are
adding a new release:

```bash
make proto VERSION=v6_0_0   # regenerate one release
make proto-all              # regenerate every release
```

Each release's decoder package (`pkg/decoder/vX_Y_Z/`) provides a
`NewHandler` that wires up unmarshal + decode for that schema, registered in
`cmd/plugin/main.go`'s `releaseHandlers` map:

```go
var releaseHandlers = map[string]func(*decoder.Decoder) func([]byte) ([]decoder.Record, error){
    v6_0_0.Release: v6_0_0.NewHandler,
    v6_1_2.Release: v6_1_2.NewHandler,
}
```

### 2. Build the shared library

**First time only**: this repo ships without `go.sum` (it was regenerated
when the Prometheus exporter dependency was added, and a checksum file
computed in a network-restricted build sandbox isn't safe to ship — it needs
generating fresh against the real module proxy). Run this once, on a machine
with normal internet access:

```bash
go mod tidy
```

This downloads `github.com/prometheus/client_golang` and its few transitive
dependencies and writes a correct `go.sum`. After that, `make build` works
normally and doesn't need network access again unless dependencies change.

`make build` supports an optional VERSION argument which embeds a version
string in the plugin, visible in the journal on startup. If not used, the
plugin logs:

```
I! [dcd] apstra_dcd_fluentbit plugin version dev
```

Recommendation is to set VERSION to the DCD release you compiled against.

```bash
make build VERSION=6.1.2
# produces: apstra_dcd_fluentbit.so
```

### 3. Install

`make install` must be run as root but does **not** need Go — build first
as your normal user, then install with sudo:

```bash
sudo mkdir -p /usr/local/lib/fluent-bit
make build VERSION=6.1.2
sudo make install
# copies apstra_dcd_fluentbit.so → /usr/local/lib/fluent-bit/
```

Edit your .bashrc to update the PATH env variable

```ini
export PATH=$PATH:/opt/fluent-bit/bin
```

```bash
source ~/.bashrc
```

### 4. Configure Fluent Bit

Edit `deployments/fluent-bit.conf` with your DCD server details:

```ini
[INPUT]
    Name          apstra_dcd
    dcd_release   6.0.0
    dcd_server    192.168.57.250
    local_address 192.168.57.128
    port          7777
```

For production move the statements above into /etc/fluent-bit/fluent-bit.conf

You also need to let Fluent-bit know about the plugin

Edit `/etc/fluent-bit/plugins.conf`

```ini
[PLUGINS]
    Path /usr/local/lib/fluent-bit/apstra_dcd_fluentbit.so
```

### 5. Run

manually run Fluent Bit

```bash
fluent-bit -c deployments/fluent-bit.conf
```

Or in production

```bash
sudo systemctl start fluent-bit.service
```

Or with Docker. The image is built locally from source — there is no pre-built
image to pull. From the repository root:

```bash
# Build the image (optionally embed a version string)
docker build \
  --build-arg VERSION=6.1.2 \
  -f deployments/Dockerfile \
  -t apstra_dcd_fluentbit:latest .

# Run with host networking (recommended)
sudo docker run -d \
  --name apstra_dcd \
  --network host \
  --restart unless-stopped \
  apstra_dcd_fluentbit:latest
```

Before building, edit `deployments/fluent-bit.conf` with your DCD server
details and ensure the `[SERVICE]` block uses container paths
(`/fluent-bit/etc/`) rather than native paths (`/etc/fluent-bit/`).

See **[DOCKER.md](DOCKER.md)** for the full guide covering deployment
topologies, why three build stages are needed, Compose, volume mounts,
configuration overrides, and troubleshooting.

---

## Testing

```bash
make test
```

27 tests total, run with real generated protobuf types — no CGO, no live DCD
server needed:

- `pkg/decoder` — 3 version-independent `GetTags` tests
- `pkg/decoder/v6_0_0` — 12 tests against the DCD 6.0.0 schema, including a
  regression test for the uint64-microseconds timestamp conversion
- `pkg/decoder/v6_1_2` — 12 tests against the DCD 6.1.2 schema (same coverage,
  including its `google.protobuf.Timestamp`-based timestamp handling)

Adding a new DCD release should mean adding a parallel `pkg/decoder/vX_Y_Z`
test file, not modifying these — see [proto/README.md](proto/README.md).
