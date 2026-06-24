// Package config holds all configuration for the DCD Fluent Bit input plugin.
// Values are read from the Fluent Bit plugin parameter map, which is populated
// from the [INPUT] block in fluent-bit.conf.
package config

import (
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/fluent/fluent-bit-go/input"
)

// OutputFormat controls how records are structured before being MsgPack-encoded
// into the Fluent Bit data bus.
type OutputFormat string

const (
	// OutputFormatMsgpack produces structured records with separate "_tags" and
	// "_fields" maps.
	//
	//   { "series": "interface_counters",
	//     "_tags":   { "device": "spine1", "interface": "et-0/0/1" },
	//     "_fields": { "tx_bytes": 1234567, "rx_bytes": 987654 } }
	//
	// Only useful for a custom downstream consumer that specifically
	// understands this nested convention (e.g. a Lua filter you write
	// yourself). Fluent Bit's stock InfluxDB output plugin does NOT read this
	// format — use OutputFormatJSON for InfluxDB. Prometheus is not reachable
	// through either output format at all; see the native Prometheus exporter
	// (PrometheusEnabled/PrometheusPort below) instead — Fluent Bit's
	// prometheus_exporter/prometheus_remote_write outputs only consume its
	// separate internal metrics pipeline, which this plugin (a log-type
	// input) never populates.
	OutputFormatMsgpack OutputFormat = "msgpack"

	// OutputFormatJSON produces a flat record with all tags and fields merged
	// into a single map. This is the correct format for log-oriented outputs
	// such as Elasticsearch, OpenSearch, Loki, plain stdout JSON debugging,
	// and Fluent Bit's InfluxDB output plugin (with Auto_Tags On — see
	// README.md's "Output formats" section).
	//
	//   { "series": "interface_counters",
	//     "device": "spine1", "interface": "et-0/0/1",
	//     "tx_bytes": 1234567, "rx_bytes": 987654 }
	OutputFormatJSON OutputFormat = "json"
)

// Config holds every parameter the plugin understands.
type Config struct {
	// TCP port this plugin listens on for incoming DCD telemetry streams.
	Port int

	// LocalAddress is the IP of this host, advertised to DCD so it knows
	// where to stream data. Must be reachable from the DCD server.
	LocalAddress string

	// StreamingTypes controls what DCD sends: "perfmon", "alerts", "events".
	// Comma-separated list.
	StreamingTypes []string

	// DcdRelease selects which DCD schema version this [INPUT] instance
	// decodes against, e.g. "6.0.0" or "6.1.2". DCD renumbers and sometimes
	// restructures its streaming telemetry protobuf schema between releases,
	// so this must match the actual version of the DCD server this instance
	// connects to. See proto/README.md for the list of supported releases
	// and how to add a new one.
	DcdRelease string

	// DCD REST API connection details.
	DcdServer   string
	DcdPort     int
	DcdLogin    string
	DcdPassword string
	DcdProtocol string // "https" or "http"

	// RefreshInterval is how often (in seconds) to re-poll DCD REST API for
	// updated blueprint and system metadata used to enrich records.
	RefreshInterval int

	// Tag mirrors the [INPUT] block's Tag directive, which Fluent Bit's own
	// engine reads and applies natively — every record this plugin emits
	// gets that exact same static tag, uniformly, with NO per-series
	// suffixing or other modification from this plugin's own Go code (a
	// classic input plugin has no API to influence per-record tagging; only
	// Fluent Bit's native config parser controls it). This field exists for
	// this plugin's own informational/logging use only; changing it has no
	// effect by itself unless your [INPUT] block's Tag is also set to match
	// — see README.md's "Sending everything to InfluxDB" section for how
	// [OUTPUT] Match needs to agree with whatever Tag is actually configured.
	Tag string

	// OutputFormat controls the record structure:
	//   "msgpack" (default) — structured _tags/_fields maps, for custom consumers
	//   "json"              — flat merged map, for Elasticsearch / Loki / InfluxDB / stdout
	OutputFormat OutputFormat

	// PrometheusEnabled starts a native embedded Prometheus exporter
	// (independent of Fluent Bit's own output pipeline, which has no path to
	// Prometheus for a log-type input like this one). When enabled, records
	// whose series prefix matches PrometheusStreamingTypes are exposed as
	// Prometheus gauges on PrometheusPort's /metrics endpoint, for an external
	// Prometheus server to scrape directly.
	PrometheusEnabled bool

	// PrometheusPort is the TCP port the embedded Prometheus /metrics HTTP
	// server listens on, when PrometheusEnabled is true.
	PrometheusPort int

	// PrometheusStreamingTypes controls which DCD streaming types the embedded
	// Prometheus exporter exports. Accepts the same values as streaming_types:
	// "perfmon", "alerts", "events", or any combination. When not specified,
	// defaults to whatever streaming_types is set to — i.e. all subscribed
	// types are exported. This exists because the alternative of running two
	// [INPUT] blocks (one per streaming-type subset) does not work on current
	// Fluent Bit versions: FLBPluginConfigKey on a second instance of the same
	// Go input plugin returns empty/wrong values for config keys, so the second
	// instance can't read its own port or streaming_types from the config file.
	// prometheus_streaming_types solves that problem without a second [INPUT].
	PrometheusStreamingTypes []string

	// Debug enables verbose, high-frequency diagnostic logging intended for
	// troubleshooting (periodic heartbeat/queue-depth logging, raw message
	// byte previews, and similar). Leave this off in normal production use —
	// genuine errors and warnings are always logged regardless of this
	// setting; this only controls the noisy, rate-limited confirmatory logs.
	Debug bool
}

// Defaults applied when a parameter is not specified in fluent-bit.conf.
var Defaults = Config{
	Port:                     7777,
	LocalAddress:             "",
	StreamingTypes:           []string{"perfmon", "alerts", "events"},
	DcdPort:                  443,
	DcdLogin:                 "admin",
	DcdPassword:              "admin",
	DcdProtocol:              "https",
	RefreshInterval:          30,
	Tag:                      "apstra.dcd",
	OutputFormat:             OutputFormatMsgpack,
	PrometheusEnabled:        false,
	PrometheusPort:           9112,
	PrometheusStreamingTypes: nil, // nil = inherit from StreamingTypes at parse time
	Debug:                    false,
}

// FromPlugin reads configuration from the Fluent Bit plugin parameter block.
// Call this inside FLBPluginInit.
func FromPlugin(plugin unsafe.Pointer) (*Config, error) {
	cfg := &Config{
		Port:                     Defaults.Port,
		LocalAddress:             Defaults.LocalAddress,
		StreamingTypes:           Defaults.StreamingTypes,
		DcdPort:                  Defaults.DcdPort,
		DcdLogin:                 Defaults.DcdLogin,
		DcdPassword:              Defaults.DcdPassword,
		DcdProtocol:              Defaults.DcdProtocol,
		RefreshInterval:          Defaults.RefreshInterval,
		Tag:                      Defaults.Tag,
		OutputFormat:             Defaults.OutputFormat,
		PrometheusEnabled:        Defaults.PrometheusEnabled,
		PrometheusPort:           Defaults.PrometheusPort,
		PrometheusStreamingTypes: Defaults.PrometheusStreamingTypes,
		Debug:                    Defaults.Debug,
	}

	if v := input.FLBPluginConfigKey(plugin, "port"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid port %q: %w", v, err)
		}
		cfg.Port = p
	}

	if v := input.FLBPluginConfigKey(plugin, "local_address"); v != "" {
		cfg.LocalAddress = v
	}

	if v := input.FLBPluginConfigKey(plugin, "streaming_types"); v != "" {
		types := strings.Split(v, ",")
		for i := range types {
			types[i] = strings.TrimSpace(types[i])
		}
		cfg.StreamingTypes = types
	}

	if v := input.FLBPluginConfigKey(plugin, "dcd_release"); v != "" {
		cfg.DcdRelease = v
	}

	if v := input.FLBPluginConfigKey(plugin, "dcd_server"); v != "" {
		cfg.DcdServer = v
	}

	if v := input.FLBPluginConfigKey(plugin, "dcd_port"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid dcd_port %q: %w", v, err)
		}
		cfg.DcdPort = p
	}

	if v := input.FLBPluginConfigKey(plugin, "dcd_login"); v != "" {
		cfg.DcdLogin = v
	}

	if v := input.FLBPluginConfigKey(plugin, "dcd_password"); v != "" {
		cfg.DcdPassword = v
	}

	if v := input.FLBPluginConfigKey(plugin, "dcd_protocol"); v != "" {
		cfg.DcdProtocol = v
	}

	if v := input.FLBPluginConfigKey(plugin, "refresh_interval"); v != "" {
		ri, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid refresh_interval %q: %w", v, err)
		}
		cfg.RefreshInterval = ri
	}

	if v := input.FLBPluginConfigKey(plugin, "tag"); v != "" {
		cfg.Tag = v
	}

	if v := strings.ToLower(input.FLBPluginConfigKey(plugin, "output_format")); v != "" {
		switch v {
		case "msgpack", "influxdb":
			cfg.OutputFormat = OutputFormatMsgpack
		case "json", "flat":
			cfg.OutputFormat = OutputFormatJSON
		default:
			return nil, fmt.Errorf("invalid output_format %q: must be 'msgpack' or 'json'", v)
		}
	}

	if v := strings.ToLower(input.FLBPluginConfigKey(plugin, "prometheus_enabled")); v == "true" || v == "on" || v == "yes" {
		cfg.PrometheusEnabled = true
	}

	if v := input.FLBPluginConfigKey(plugin, "prometheus_port"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid prometheus_port %q: %w", v, err)
		}
		cfg.PrometheusPort = p
	}

	if v := input.FLBPluginConfigKey(plugin, "prometheus_streaming_types"); v != "" {
		types := strings.Split(v, ",")
		for i := range types {
			types[i] = strings.TrimSpace(types[i])
		}
		cfg.PrometheusStreamingTypes = types
	}

	// If prometheus_streaming_types was not specified, inherit from
	// streaming_types — Prometheus sees whatever DCD is subscribed to send.
	if cfg.PrometheusStreamingTypes == nil {
		cfg.PrometheusStreamingTypes = cfg.StreamingTypes
	}

	if v := strings.ToLower(input.FLBPluginConfigKey(plugin, "debug")); v == "true" || v == "on" || v == "yes" {
		cfg.Debug = true
	}

	if cfg.DcdServer == "" {
		return nil, fmt.Errorf("dcd_server is required")
	}
	if cfg.LocalAddress == "" {
		return nil, fmt.Errorf("local_address is required (the IP of this host, reachable from DCD)")
	}
	if cfg.DcdRelease == "" {
		return nil, fmt.Errorf("dcd_release is required (e.g. \"6.0.0\" or \"6.1.2\") — must match the DCD server's actual version, see proto/README.md for supported releases")
	}

	return cfg, nil
}
