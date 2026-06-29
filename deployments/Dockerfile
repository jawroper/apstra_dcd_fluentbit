# =============================================================================
# Multi-stage Dockerfile
#
# Stage 1 (fluent-extract): extract the Fluent Bit binary from the official
#           image, which is distroless and has no shell or package manager
# Stage 2 (builder):        compile the Go shared library
# Stage 3 (runtime):        debian:12-slim with runtime libs, Fluent Bit
#           binary, and plugin combined
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1 — extract Fluent Bit binary from the official distroless image
# ---------------------------------------------------------------------------
# The official fluent/fluent-bit image is distroless — it contains only
# /fluent-bit/bin and /fluent-bit/etc. There is no shell, package manager,
# or /fluent-bit/lib directory. We extract the binary here and combine it
# with a real base image in Stage 3 so we can install runtime libraries.
FROM fluent/fluent-bit:3.1 AS fluent-extract

# ---------------------------------------------------------------------------
# Stage 2 — build the Go shared library
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
ARG VERSION=dev
RUN go mod tidy && \
    CGO_ENABLED=1 go build \
        -buildmode=c-shared \
        -trimpath \
        -ldflags="-s -w -X main.Version=${VERSION}" \
        -o apstra_dcd_fluentbit.so \
        ./cmd/plugin/

# ---------------------------------------------------------------------------
# Stage 3 — runtime
# ---------------------------------------------------------------------------
# debian:12-slim is ABI-compatible with the official Fluent Bit build and
# has a real package manager so we can install the runtime libraries that
# both Fluent Bit and the plugin need.
FROM debian:12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    libsasl2-2 \
    libpq5 \
    libsystemd0 \
    libyaml-0-2 \
    libcurl4 \
    && rm -rf /var/lib/apt/lists/*

# Copy Fluent Bit binary and default config from the official image.
# Only /fluent-bit/bin and /fluent-bit/etc exist in the distroless image.
COPY --from=fluent-extract /fluent-bit/bin/fluent-bit /fluent-bit/bin/fluent-bit
COPY --from=fluent-extract /fluent-bit/etc/           /fluent-bit/etc/

# Copy the compiled plugin
RUN mkdir -p /usr/local/lib/fluent-bit
COPY --from=builder /build/apstra_dcd_fluentbit.so /usr/local/lib/fluent-bit/
RUN chmod 755 /usr/local/lib/fluent-bit/apstra_dcd_fluentbit.so

# Copy configuration
COPY deployments/fluent-bit.conf /fluent-bit/etc/fluent-bit.conf
COPY deployments/plugins.conf    /fluent-bit/etc/plugins.conf

# Create log directory for the file output plugin
RUN mkdir -p /var/log/apstra

# DCD streaming TCP port
EXPOSE 7777

# Fluent Bit's own internal monitoring HTTP server (http_port in fluent-bit.conf's
# [SERVICE] block) — unrelated to DCD telemetry or Prometheus metrics.
EXPOSE 2020

# This plugin's native Prometheus exporter (prometheus_port in fluent-bit.conf's
# [INPUT] block, default 9112) — only listens if prometheus_enabled is on.
EXPOSE 9112

ENTRYPOINT ["/fluent-bit/bin/fluent-bit"]
CMD ["-c", "/fluent-bit/etc/fluent-bit.conf"]
