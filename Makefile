BINARY      := apstra_dcd_fluentbit.so
MODULE      := github.com/jawroper/apstra-dcd-fluentbit
GOFLAGS     := CGO_ENABLED=1
BUILD_FLAGS := -buildmode=c-shared -trimpath
VERSION     ?= dev
LDFLAGS     := -ldflags="-s -w -X main.Version=$(VERSION)"

.PHONY: all build test clean docker proto proto-all install help

all: build

## build: compile the Fluent Bit shared library (.so)
##        Usage: make build VERSION=1.2.3   (defaults to "dev" if omitted)
build:
	$(GOFLAGS) go build $(BUILD_FLAGS) $(LDFLAGS) -o $(BINARY) ./cmd/plugin/
	@echo "Built $(BINARY) version=$(VERSION)"

## test: run unit tests (pure-Go packages, no CGO required)
test:
	go test ./pkg/... ./proto/... -v -count=1

## test-race: run tests with race detector
test-race:
	go test -race ./pkg/... ./proto/... -v -count=1

## proto: generate Go code from a versioned .proto file
##        Requires: protoc + protoc-gen-go
##        Usage: make proto VERSION=v6_0_0
##        See proto/README.md for the full naming convention and how to add
##        a new DCD release.
proto:
	@test -n "$(VERSION)" || \
		(echo "ERROR: specify VERSION, e.g. make proto VERSION=v6_0_0" && exit 1)
	@test -d proto/$(VERSION) || \
		(echo "ERROR: proto/$(VERSION) not found." && exit 1)
	protoc \
		--go_out=proto/$(VERSION) \
		--go_opt=paths=source_relative \
		--go_opt=Mstreaming-telemetry-schema-$(VERSION).proto=$(MODULE)/proto/$(VERSION)\;$(VERSION) \
		-I proto/$(VERSION) \
		proto/$(VERSION)/streaming-telemetry-schema-$(VERSION).proto
	@echo "Generated proto/$(VERSION)/streaming-telemetry-schema-$(VERSION).pb.go"

## proto-all: regenerate Go code for every supported DCD release
proto-all:
	@for d in proto/v*/; do \
		v=$$(basename $$d); \
		$(MAKE) proto VERSION=$$v; \
	done

## docker: build a Docker image containing Fluent Bit + the plugin
docker: build
	docker build -t jawroper/apstra-dcd-fluentbit:latest -f deployments/Dockerfile .

## install: copy the .so to the Fluent Bit plugins directory
##          Run 'make build VERSION=x.y.z' first, then 'sudo make install'
install:
	install -m 755 $(BINARY) /usr/local/lib/fluent-bit/

## clean: remove build artifacts
clean:
	rm -f $(BINARY) $(BINARY:.so=.h)

## help: list available targets
help:
	@grep -E '^## ' Makefile | sed 's/## //'
