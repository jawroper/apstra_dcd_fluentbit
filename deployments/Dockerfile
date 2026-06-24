# =============================================================================
# Multi-stage Dockerfile
#
# Stage 1 (builder): compiles the Go shared library
# Stage 2 (runtime): minimal Fluent Bit image with the plugin installed
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1 — build the Go shared library
# ---------------------------------------------------------------------------
FROM golang:1.22-bookworm AS builder

WORKDIR /build

# Copy module files first so Docker caches the dependency layer
COPY go.mod go.sum* ./
RUN go mod download 2>/dev/null || true

# Copy source
COPY . .

# Compile the Fluent Bit input plugin as a C-shared library.
# CGO_ENABLED=1 is required; the fluent-bit-go library uses cgo.
RUN CGO_ENABLED=1 go build \
        -buildmode=c-shared \
        -trimpath \
        -ldflags="-s -w" \
        -o apstra_dcd_fluentbit.so \
        ./cmd/plugin/

# ---------------------------------------------------------------------------
# Stage 2 — runtime image
# ---------------------------------------------------------------------------
FROM fluent/fluent-bit:3.1

# Install the compiled plugin
COPY --from=builder /build/apstra_dcd_fluentbit.so /usr/local/lib/fluent-bit/

# Copy configuration
COPY deployments/fluent-bit.conf /fluent-bit/etc/fluent-bit.conf
COPY deployments/plugins.conf    /fluent-bit/etc/plugins.conf

# DCD streaming TCP port
EXPOSE 7777

# Fluent Bit's own internal monitoring HTTP server (http_port in fluent-bit.conf's
# [SERVICE] block) — unrelated to DCD telemetry or Prometheus metrics.
EXPOSE 2020

# This plugin's native Prometheus exporter (prometheus_port in fluent-bit.conf's
# [INPUT] block, default 9112) — only listens if prometheus_enabled is on.
EXPOSE 9112

CMD ["/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf"]
