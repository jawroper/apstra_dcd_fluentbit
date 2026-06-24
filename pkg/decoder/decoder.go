// Package decoder holds the DCD-release-independent building blocks used to
// turn protobuf telemetry messages into Fluent Bit records.
//
// DCD renumbers and occasionally restructures fields between releases, so the
// actual per-message decoding logic (which knows the concrete generated proto
// types for one specific release) lives in versioned sub-packages:
//
//	pkg/decoder/v6_0_0   — decodes proto/v6_0_0  (DCD 6.0.0 wire format)
//	pkg/decoder/v6_1_2   — decodes proto/v6_1_2  (DCD 6.1.2 wire format)
//
// Everything in *this* file is intentionally version-agnostic: it only deals
// in map[string]interface{} / reflection, never in a specific generated proto
// type. Adding support for a new DCD release should never require touching
// this file — see proto/README.md and pkg/decoder/v6_0_0 for the pattern to
// copy.
//
// # Output formats
//
// The Decoder supports two record formats, selected at construction time via
// config.OutputFormat. Both are encoded as MessagePack by main.go — the format
// controls how the data is *structured inside* the MsgPack map, not the wire
// encoding itself (which is always MsgPack).
//
// OutputFormatMsgpack (default) — structured _tags/_fields:
//
//	{ "series":  "interface_counters",
//	  "_tags":   { "device": "spine1", "interface": "et-0/0/1", "role": "spine" },
//	  "_fields": { "tx_bytes": 1234567, "rx_bytes": 987654, "fcs_errors": 0 } }
//
//	Use with: custom consumers that specifically understand this nested
//	_tags/_fields convention (e.g. a Lua filter, or your own downstream
//	processor). Fluent Bit's stock InfluxDB output plugin does NOT read this
//	format — it has no concept of a nested tags/fields sub-map. Use
//	OutputFormatJSON below for InfluxDB.
//
// OutputFormatJSON — flat merged map:
//
//	{ "series":    "interface_counters",
//	  "device":    "spine1",
//	  "interface": "et-0/0/1",
//	  "role":      "spine",
//	  "tx_bytes":  1234567,
//	  "rx_bytes":  987654 }
//
//	Use with: Elasticsearch, OpenSearch, Loki, stdout (json_lines), S3, and
//	Fluent Bit's InfluxDB output plugin. All keys are at the top level.
//
//	For InfluxDB specifically: the InfluxDB output plugin reads every
//	top-level key as a FIELD by default. To make specific keys (device, role,
//	blueprint, interface, severity, status, series, etc.) become InfluxDB
//	TAGS instead, list them in the output's Tag_Keys setting — see
//	deployments/fluent-bit.conf for a worked example. Also note: InfluxDB's
//	measurement name comes from the Fluent Bit *tag* set on the [INPUT]
//	block (a single static string, e.g. "apstra.dcd"), not from any field
//	inside the record — so by default every series type (interface_counters,
//	alert_liveness, event_link_status, ...) lands in ONE InfluxDB
//	measurement, distinguishable via the "series" tag. If you want separate
//	InfluxDB measurements per series, add a rewrite_tag FILTER that sets the
//	Fluent Bit tag from each record's "series" field before the output runs
//	— also shown in deployments/fluent-bit.conf.
//
// In both cases "_ts" (int64 nanoseconds) is present in the record but stripped
// by main.go before MsgPack encoding; its value populates the Fluent Bit
// envelope FLBTime which becomes the InfluxDB / log-store point timestamp.
package decoder

import (
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/jawroper/apstra-dcd-fluentbit/pkg/config"
	"github.com/jawroper/apstra-dcd-fluentbit/pkg/restapi"
)

// Record is a single Fluent Bit log record.
type Record map[string]interface{}

// MetricsObserver receives every record's tags/fields as MakeRecord builds
// it, before they're shaped into the configured Fluent Bit output format.
// Used to feed the optional native Prometheus exporter (pkg/promexport)
// without this package needing to import it directly — promexport.Exporter
// satisfies this interface structurally.
type MetricsObserver interface {
	Observe(series string, tags map[string]string, fields map[string]interface{})
}

// Decoder holds a reference to the REST API client for metadata enrichment
// and the output format setting. It is shared across all DCD-release-specific
// decoder packages — each one receives a *Decoder and calls its exported
// methods (GetTags, MakeRecord) rather than duplicating this logic.
type Decoder struct {
	api     *restapi.Client
	format  config.OutputFormat
	metrics MetricsObserver // nil unless SetMetricsObserver is called
}

// New creates a Decoder backed by the given DCD REST API client.
func New(api *restapi.Client, cfg *config.Config) *Decoder {
	return &Decoder{api: api, format: cfg.OutputFormat}
}

// SetMetricsObserver attaches a MetricsObserver (typically a
// *promexport.Exporter) so every record's tags/fields are also forwarded to
// it. Optional — if never called, MakeRecord behaves exactly as before. Not
// safe to call concurrently with MakeRecord; call once during setup.
func (d *Decoder) SetMetricsObserver(m MetricsObserver) {
	d.metrics = m
}

// GetTags builds the enrichment tag map for a device key. Version-independent:
// every DCD release identifies devices/interfaces the same "device_key" or
// "device_key::interface" way.
func (d *Decoder) GetTags(deviceKey string) map[string]string {
	tags := make(map[string]string)

	if strings.Contains(deviceKey, "::") {
		parts := strings.SplitN(deviceKey, "::", 2)
		deviceKey = parts[0]
		tags["interface"] = parts[1]
	}
	tags["device_key"] = deviceKey

	sys := d.api.GetSystemByKey(deviceKey)
	if sys == nil {
		tags["device"] = deviceKey
		return tags
	}

	if sys.BlueprintRole != "" {
		tags["role"] = sys.BlueprintRole
	}
	if sys.BlueprintID != "" {
		if bp := d.api.GetBlueprintByID(sys.BlueprintID); bp != nil {
			tags["blueprint"] = bp.Name
		}
	}
	if sys.BlueprintName != "" {
		tags["device_name"] = sys.BlueprintName
		tags["device"] = sys.BlueprintName
	} else {
		tags["device"] = deviceKey
	}

	return tags
}

// MaxClockSkew is how far DCD's reported timestamp is allowed to differ from
// this host's wall-clock time before SanitizeTimestamp rejects it. Generous
// enough to never trigger on legitimate NTP-managed clock drift (typically
// sub-second to a few seconds), strict enough to catch a genuinely broken
// clock source — observed in production: an DCD server with a confirmed
// correct system clock (per `date`) still streamed PerfMon timestamps
// decoding to ~19 years in the past, evidently from a different/stale clock
// source used specifically by its streaming telemetry subsystem.
const MaxClockSkew = 24 * time.Hour

// SanitizeTimestamp returns candidate if it's within MaxClockSkew of this
// host's current time, otherwise logs a warning and returns time.Now()
// instead. Every DCD-release decoder package should route its decoded
// DcdMessage.timestamp through this rather than trusting it unconditionally
// — see MaxClockSkew's doc comment for why this isn't just theoretical.
func SanitizeTimestamp(candidate time.Time, originName string) time.Time {
	skew := time.Since(candidate)
	if skew < -MaxClockSkew || skew > MaxClockSkew {
		log.Printf("W! [dcd] DCD-provided timestamp %v is implausible (%v from wall-clock time) — using plugin receipt time instead (origin=%q)",
			candidate.Format(time.RFC3339), skew, originName)
		return time.Now()
	}
	return candidate
}

// MakeRecord builds a Fluent Bit record in whichever output format this
// Decoder was configured with. Exported so version-specific decoder packages
// can call it directly. If a MetricsObserver is attached (see
// SetMetricsObserver), it's also given tags/fields here — before they're
// shaped into either output format, since msgpack's nested _tags/_fields
// structure can't be cheaply un-nested downstream.
func (d *Decoder) MakeRecord(series string, tags map[string]string, fields map[string]interface{}, ts time.Time) Record {
	if d.metrics != nil {
		d.metrics.Observe(series, tags, fields)
	}
	if d.format == config.OutputFormatJSON {
		return makeFlatRecord(series, tags, fields, ts)
	}
	return makeMsgpackRecord(series, tags, fields, ts)
}

// ----------------------------------------------------------------------------
// Helpers — all version-agnostic (interface{} / reflection based).
// ----------------------------------------------------------------------------

func makeMsgpackRecord(series string, tags map[string]string, fields map[string]interface{}, ts time.Time) Record {
	return Record{
		"series":  series,
		"_tags":   tags,
		"_fields": fields,
		"_ts":     ts.UnixNano(),
	}
}

func makeFlatRecord(series string, tags map[string]string, fields map[string]interface{}, ts time.Time) Record {
	r := make(Record, 2+len(tags)+len(fields))
	r["series"] = series
	r["_ts"] = ts.UnixNano()
	for k, v := range tags {
		r[k] = v
	}
	for k, v := range fields {
		r[k] = v
	}
	return r
}

// CopyTags returns a shallow copy of a tag map. Exported for use by
// version-specific decoder packages.
func CopyTags(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// ProtoToFields JSON-round-trips any value (typically a generated protobuf
// message, but works for anything) into a map[string]interface{}. This is the
// key piece of version-independence: it never needs to know the concrete
// generated type, so the same function serves every DCD release.
func ProtoToFields(msg interface{}) map[string]interface{} {
	if msg == nil {
		return map[string]interface{}{}
	}
	b, err := marshalProtoJSON(msg)
	if err != nil {
		log.Printf("W! [dcd] ProtoToFields marshal error: %v", err)
		return map[string]interface{}{}
	}
	result := make(Record)
	if err := unmarshalJSON(b, &result); err != nil {
		log.Printf("W! [dcd] ProtoToFields unmarshal error: %v", err)
	}
	return result
}

// IsNilProto reports whether v is a nil pointer (typed or untyped). Exported
// for use by version-specific decoder packages when checking oneof cases.
func IsNilProto(v interface{}) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}
