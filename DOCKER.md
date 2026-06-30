# Docker Deployment Guide

## Deployment topologies

Before choosing a run option, identify which host receives the TCP frames
from your DCD server:

**Topology A — container host receives the DCD stream:**
```
DCD Server
      │  GPB protobuf over TCP port 7777
      ▼
 Container host
 ┌─────────────────────────────────────────┐
 │  Docker container                       │
 │  Fluent Bit + apstra_dcd_fluentbit.so   │──→ InfluxDB / Prometheus / File
 └─────────────────────────────────────────┘
```
Use `--network host` or `-p 7777:7777/tcp` as shown below. The plugin
runs inside the container and receives the DCD stream directly.

**Topology B — a separate VM receives the DCD stream:**
```
DCD Server
      │  GPB protobuf over TCP port 7777
      ▼
 Fluent Bit VM  (apstra_dcd_fluentbit plugin installed natively)
      │  outputs
      ▼
 Container host (InfluxDB / Grafana / dashboards)
```
The container host does **not** run the plugin. Fluent Bit and the `.so`
are installed natively on the Fluent Bit VM (see the Quick start section
of the README). The Docker setup below does not apply to this topology.

**Topology C — both in the same environment:**
```
DCD Server
      │  GPB protobuf over TCP port 7777
      ▼
 Fluent Bit VM  (apstra_dcd_fluentbit, native install)
      │  outputs to
      ├──→ InfluxDB container
      ├──→ Grafana container
      └──→ Prometheus container
```
Run the plugin natively on the Fluent Bit VM. Run your storage and
visualisation stack in containers on a separate host. No plugin `.so`
is needed inside those containers.

The Dockerfile and Compose file below apply only to **Topology A** — where
you want Fluent Bit and the plugin running inside a container that directly
receives the DCD telemetry stream.

---

## Why three build stages

The official `fluent/fluent-bit` image is **distroless** — it contains only
the Fluent Bit binary and its direct runtime dependencies, with no shell, no
package manager, and no standard OS tools. This makes it secure and small but
means you cannot install additional packages into it directly.

The Dockerfile uses three stages to work around this:

| Stage | Base | Purpose |
|-------|------|---------|
| `fluent-extract` | `fluent/fluent-bit:3.1` | Extract the Fluent Bit binary and bundled config |
| `builder` | `golang:1.22-bookworm` | Compile the Go plugin as a C-shared library |
| runtime | `debian:12-slim` | Install runtime libraries, combine Fluent Bit + plugin |

`debian:12-slim` is used as the runtime base because it has a real package
manager (`apt-get`) so we can install the libraries that both Fluent Bit and
the plugin need, and it is ABI-compatible with the official Fluent Bit build.

---

## Dockerfile

The Dockerfile is at `deployments/Dockerfile`:

```dockerfile
# ── Stage 1: extract Fluent Bit binary from the official distroless image ───
FROM fluent/fluent-bit:3.1 AS fluent-extract

# ── Stage 2: build the Go shared library ────────────────────────────────────
FROM golang:1.22-bookworm AS builder

WORKDIR /build

COPY go.mod go.sum* ./
RUN go mod download 2>/dev/null || true

COPY . .

ARG VERSION=dev
RUN go mod tidy && \
    CGO_ENABLED=1 go build \
        -buildmode=c-shared \
        -trimpath \
        -ldflags="-s -w -X main.Version=${VERSION}" \
        -o apstra_dcd_fluentbit.so \
        ./cmd/plugin/

# ── Stage 3: runtime ─────────────────────────────────────────────────────────
FROM debian:12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    libsasl2-2 \
    libpq5 \
    libsystemd0 \
    libyaml-0-2 \
    libcurl4 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=fluent-extract /fluent-bit/bin/fluent-bit /fluent-bit/bin/fluent-bit
COPY --from=fluent-extract /fluent-bit/etc/           /fluent-bit/etc/

RUN mkdir -p /usr/local/lib/fluent-bit
COPY --from=builder /build/apstra_dcd_fluentbit.so /usr/local/lib/fluent-bit/

COPY deployments/fluent-bit.conf /fluent-bit/etc/fluent-bit.conf
COPY deployments/plugins.conf    /fluent-bit/etc/plugins.conf

RUN mkdir -p /var/log/apstra

EXPOSE 7777
EXPOSE 2020
EXPOSE 9112

ENTRYPOINT ["/fluent-bit/bin/fluent-bit"]
CMD ["-c", "/fluent-bit/etc/fluent-bit.conf"]
```

> **Note:** If the build fails with missing shared libraries at runtime,
> run `docker exec apstra_dcd ldd /usr/local/lib/fluent-bit/apstra_dcd_fluentbit.so`
> to identify any missing packages and add them to the `apt-get` install line in Stage 3.

---

## Configuring fluent-bit.conf for the container

The `deployments/fluent-bit.conf` is baked into the image at build time.
Inside the container, Fluent Bit's config files live under `/fluent-bit/etc/`
rather than `/etc/fluent-bit/`. The `[SERVICE]` section must use container
paths:

```ini
[SERVICE]
    flush           1
    daemon          Off
    log_level       info
    parsers_file    /fluent-bit/etc/parsers.conf   # ← container path
    plugins_file    /fluent-bit/etc/plugins.conf   # ← container path
    http_server     Off
    http_listen     0.0.0.0
    http_port       2020
    storage.metrics on
```

If you leave native paths (`/etc/fluent-bit/...`) in place the container
will fail to start with:
```
[error] could not open parser configuration file, aborting.
[error] plugins_file not found, aborting.
```

Update your `[INPUT]` block with your actual DCD server details before
building the image:

```ini
[INPUT]
    Name          apstra_dcd
    dcd_release   6.1.2
    dcd_server    192.168.57.250
    local_address 192.168.57.128
    dcd_port      443
    dcd_login     admin
    dcd_password  yourpassword
    port          7777
    streaming_types  perfmon, alerts, events
    refresh_interval 30
    output_format json
    tag           apstra.dcd
```

---

## Build the image

Run from the repository root:

```bash
docker build -f deployments/Dockerfile -t apstra_dcd_fluentbit:latest .
```

To embed a version string in the plugin (visible in the journal on startup):

```bash
docker build \
  --build-arg VERSION=6.1.2 \
  -f deployments/Dockerfile \
  -t apstra_dcd_fluentbit:latest .
```

The first build takes 2–3 minutes as Go downloads dependencies and compiles
the plugin. Subsequent builds are faster due to Docker layer caching.

---

## Run the container

The container must receive TCP on the DCD port (default 7777). Use
`--network host` (strongly recommended on Linux) or publish the port
explicitly:

```bash
# Option A — host networking (recommended, Linux only)
sudo docker run -d \
    --name apstra_dcd \
    --network host \
    --restart unless-stopped \
    apstra_dcd_fluentbit:latest

# Option B — bridge networking with port mapping
sudo docker run -d \
    --name apstra_dcd \
    -p 7777:7777/tcp \
    -p 9112:9112/tcp \
    -p 2020:2020/tcp \
    --restart unless-stopped \
    apstra_dcd_fluentbit:latest
```

> **Why host networking?** With bridge networking, the DCD server must be
> able to reach the container's port 7777 to establish the TCP stream. Host
> networking avoids any NAT/bridge translation and is simpler to configure
> for the DCD `local_address` setting, which must match the IP the DCD server
> sees as the connection source.

---

## Log directory and volume mounts

The Dockerfile creates `/var/log/apstra` inside the container, so the file
output plugin can write there without any host setup. Logs written inside
the container persist as long as the container exists.

If you want logs accessible on the host VM, add a bind mount:

```bash
-v /var/log/apstra:/var/log/apstra
```

The directory must exist on the host first:
```bash
sudo mkdir -p /var/log/apstra
```

To read logs from inside a running container without a host mount:
```bash
sudo docker exec apstra_dcd tail -f /var/log/apstra/telemetry.log
```

---

## Override configuration at runtime

**This is the recommended way to run the container.** Mounting your own
`fluent-bit.conf` lets you set the correct `local_address` and other
site-specific settings without rebuilding the image every time.

The `local_address` in the `[INPUT]` block must match the IP address of
the VM running the container (its `eth0` address) — this is the address
the DCD server uses to push telemetry back to the plugin. If it's wrong,
the DCD server will connect and immediately reset.

Check your VM's IP first:
```bash
ip addr show eth0 | grep "inet "
```

Then edit `deployments/fluent-bit.conf` with your actual DCD server details
and correct `local_address`, and mount it at runtime:

```bash
sudo docker run -d \
    --name apstra_dcd \
    --network host \
    --restart unless-stopped \
    -v /home/labuser/apstra-dcd-fluentbit/deployments/fluent-bit.conf:/fluent-bit/etc/fluent-bit.conf:ro \
    apstra_dcd_fluentbit:latest
```

The `/var/log/apstra` directory is created inside the container by the
Dockerfile — no host mount is needed for file output unless you want the
logs accessible on the host VM, in which case add:
```bash
    -v /var/log/apstra:/var/log/apstra \
```

To pick up config changes, just stop, remove, and rerun — no rebuild needed:
```bash
sudo docker stop apstra_dcd && sudo docker rm apstra_dcd
# edit deployments/fluent-bit.conf, then rerun the docker run command above
```

---

## Docker Compose

`deployments/docker-compose.yml`:

```yaml
services:
  apstra_dcd:
    build:
      context: ..
      dockerfile: deployments/Dockerfile
    image: apstra_dcd_fluentbit:latest
    container_name: apstra_dcd
    network_mode: host
    restart: unless-stopped
    volumes:
      - ./fluent-bit.conf:/fluent-bit/etc/fluent-bit.conf:ro
      - apstra_logs:/var/log/apstra

volumes:
  apstra_logs:
```

Run from the `deployments/` directory:

```bash
docker compose -f deployments/docker-compose.yml up -d
docker compose -f deployments/docker-compose.yml logs -f
```

---

## Viewing logs from the container

```bash
# Fluent Bit output (startup, errors, plugin version, debug messages)
sudo docker logs -f apstra_dcd

# Decoded telemetry records (if file output is configured)
sudo docker exec apstra_dcd tail -f /var/log/apstra/telemetry.log

# Prometheus metrics (if prometheus_enabled is on)
curl http://localhost:9112/metrics
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `error opening plugin: undefined symbol` | Missing runtime library | Run `docker exec apstra_dcd ldd /usr/local/lib/fluent-bit/apstra_dcd_fluentbit.so` to identify missing libs |
| `could not open parser configuration file` | Wrong paths in `[SERVICE]` | Use `/fluent-bit/etc/` paths, not `/etc/fluent-bit/` |
| DCD stream not connecting | `local_address` mismatch | With bridge networking the `local_address` must be the container's IP, not the host's — use `--network host` instead |
| `permission denied` writing log file | Log directory owned by root | Pre-create the directory on the host before starting the container |
| `permission denied` connecting to Docker | User not in docker group | Use `sudo` or `sudo usermod -aG docker $USER` then re-login |
| Config changes not taking effect | Old image cached | `docker stop apstra_dcd && docker rm apstra_dcd` then rebuild and rerun |
| No data in InfluxDB | Container can't reach InfluxDB host | With `--network host` use the host's actual IP in the `[OUTPUT]` block, not `127.0.0.1` |
