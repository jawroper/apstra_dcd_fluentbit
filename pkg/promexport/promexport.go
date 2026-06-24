// Package promexport exposes DCD telemetry as native Prometheus metrics via a
// standalone HTTP /metrics endpoint, completely independent of Fluent Bit's
// own output pipeline.
//
// # Why this exists as a separate HTTP server, not a Fluent Bit [OUTPUT]
//
// Fluent Bit has two non-interoperable internal pipelines: "logs" and
// "metrics". This plugin is a classic log-type input (it uses the
// FLBPluginInputCallback msgpack/log API), so every record it produces lives
// in the logs pipeline. Fluent Bit's own prometheus_exporter and
// prometheus_remote_write OUTPUT plugins only read from the separate metrics
// pipeline — they cannot consume arbitrary log records, regardless of
// format. The supported bridge for log-derived data is the log_to_metrics
// FILTER, but it requires one filter block per metric and can't handle
// PerfMon's dynamic/generic series (e.g. probe_message) at all, since
// log_to_metrics needs a fixed, known field name configured in advance.
//
// So: this package runs its own embedded Prometheus client, independent of
// Fluent Bit entirely. An external Prometheus server scrapes this plugin
// directly (PrometheusPort, default 9112) — the records ALSO continue
// flowing through Fluent Bit's normal pipeline to whatever [OUTPUT] plugins
// are configured (InfluxDB, Elasticsearch, stdout, ...), unaffected. This is
// purely additive. Because this never touches Fluent Bit's logs/metrics
// pipeline distinction at all, that distinction simply doesn't apply here —
// alerts and events work exactly as well as PerfMon does, for the same
// reason a Telegraf-based equivalent (e.g. the dcdom project, which exposes
// alerts as Prometheus gauges via outputs.prometheus_client) isn't limited
// by it either: neither plugin is going through Fluent Bit's/Telegraf's own
// metrics-specific output machinery, both just run their own server.
//
// # Scope: PerfMon, alerts, and events — all of it
//
// Every series this plugin decodes is exported: PerfMon (interface_counters,
// system_info, process_info, file_info, probe_message, ...), alerts
// (alert_bgp_neighbor_mismatch, alert_liveness, ...), and events
// (event_link_status, event_bgp_neighbor, ...).
//
// # Label sets: fixed for PerfMon, per-series-name for alerts/events
//
// PerfMon series all share the same dimensions (device, interface, role,
// ...), so every PerfMon metric uses one fixed canonical label list
// (perfmonLabels, below), with "" for any that don't apply to a given record
// (e.g. system_info has no interface).
//
// Alerts and events don't share a common shape — alert_bgp_neighbor_mismatch
// carries actual_state/expected_state/lcl_asn/rmt_asn/..., while
// alert_liveness carries an entirely different set, and so on per type.
// Prometheus requires a fixed label set per metric name, so this package
// establishes that set dynamically, per series name, the first time each one
// is observed (using that record's own tag keys, sorted for determinism),
// and reuses it for every subsequent record of that same series — extra tags
// on a later record are dropped for the metric (silently — the data is
// unaffected in the normal Fluent Bit pipeline), missing ones default to "".
//
// One genuine limitation either way: truly dynamic, per-record-varying tag
// keys (e.g. probe_message's prop_<name> tags, where the key NAME itself
// varies per probe, not just its value) still can't become Prometheus
// labels, in either the fixed or per-series-dynamic scheme — Prometheus
// fundamentally requires a label's NAME to be fixed in the metric's
// descriptor. That data remains available through the normal Fluent Bit
// pipeline (InfluxDB, Elasticsearch, etc.), just not as a native Prometheus
// label.
package promexport

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// perfmonLabels is the fixed label set used by every PerfMon metric. Order
// matters: it must stay consistent between metric creation and every
// subsequent WithLabelValues call.
var perfmonLabels = []string{
	"device", "device_key", "interface", "role", "blueprint", "device_name",
	"process_name", "file_name", "probe_id", "stage_name", "item_id", "probe_label",
}

// isPerfmonSeries reports whether series should use the fixed perfmonLabels
// scheme rather than per-series-dynamic labels — matches the "alert_"/
// "event_" naming convention every decoder package uses.
func isPerfmonSeries(series string) bool {
	return !strings.HasPrefix(series, "alert_") && !strings.HasPrefix(series, "event_")
}

// Exporter holds the Prometheus registry, HTTP server, the set of
// dynamically-created gauges (one per distinct series+field combination seen
// so far), and the per-series label sets established for alert/event series.
type Exporter struct {
	registry *prometheus.Registry
	server   *http.Server

	// allowedPrefixes gates which series are exported. "perfmon" allows any
	// series that isn't alert_*/event_*; "alerts" allows alert_*; "events"
	// allows event_*. A nil map means allow everything (the default when
	// prometheus_streaming_types isn't set explicitly).
	allowedPerfmon bool
	allowedAlerts  bool
	allowedEvents  bool

	mu           sync.Mutex
	gauges       map[string]*prometheus.GaugeVec // keyed by sanitized metric name
	seriesLabels map[string][]string             // alert/event series name -> its established label set
}

// New creates an Exporter that exports records matching the given streaming
// types. types should match the prometheus_streaming_types config value —
// any combination of "perfmon", "alerts", "events". An empty/nil slice means
// allow all types (same as specifying all three explicitly). Call Start to
// begin serving /metrics.
func New(types []string) *Exporter {
	e := &Exporter{
		registry:     prometheus.NewRegistry(),
		gauges:       make(map[string]*prometheus.GaugeVec),
		seriesLabels: make(map[string][]string),
	}
	if len(types) == 0 {
		e.allowedPerfmon = true
		e.allowedAlerts = true
		e.allowedEvents = true
	} else {
		for _, t := range types {
			switch strings.TrimSpace(strings.ToLower(t)) {
			case "perfmon":
				e.allowedPerfmon = true
			case "alerts":
				e.allowedAlerts = true
			case "events":
				e.allowedEvents = true
			}
		}
	}
	return e
}

// Start begins serving Prometheus metrics on the given port at /metrics.
// Runs in its own goroutine; returns once the listener is confirmed bound,
// or an error if it couldn't bind.
func (e *Exporter) Start(port int) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(e.registry, promhttp.HandlerOpts{}))

	e.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	ln, err := net.Listen("tcp", e.server.Addr)
	if err != nil {
		return fmt.Errorf("promexport: bind port %d: %w", port, err)
	}

	go func() {
		if err := e.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("E! [dcd] Prometheus exporter HTTP server error: %v", err)
		}
	}()

	log.Printf("I! [dcd] Prometheus exporter listening on :%d/metrics (PerfMon, alerts, and events)", port)
	return nil
}

// Stop gracefully shuts down the HTTP server.
func (e *Exporter) Stop(ctx context.Context) error {
	if e.server == nil {
		return nil
	}
	return e.server.Shutdown(ctx)
}

// Observe records one decoded record's numeric fields as Prometheus gauges.
// Records whose series type is not in the allowed set (controlled by the
// prometheus_streaming_types config key) are silently skipped.
//
// tags and fields are the same maps passed to decoder.Decoder.MakeRecord,
// i.e. already cleanly separated — this must be called before they're
// shaped into either Fluent Bit output format, not after, since the msgpack
// format's nested _tags/_fields structure can't be cheaply un-nested here.
func (e *Exporter) Observe(series string, tags map[string]string, fields map[string]interface{}) {
	if series == "" {
		return
	}
	// Filter by allowed streaming type.
	switch {
	case strings.HasPrefix(series, "alert_"):
		if !e.allowedAlerts {
			return
		}
	case strings.HasPrefix(series, "event_"):
		if !e.allowedEvents {
			return
		}
	default: // perfmon
		if !e.allowedPerfmon {
			return
		}
	}

	labelNames := e.labelsFor(series, tags)
	labelValues := make([]string, len(labelNames))
	for i, name := range labelNames {
		labelValues[i] = tags[name] // "" if not present for this record
	}

	for fieldName, rawValue := range fields {
		val, ok := toFloat64(rawValue)
		if !ok {
			continue // not a numeric field; skip (shouldn't normally happen — fields are numeric by construction)
		}
		gv := e.gaugeFor(series, fieldName, labelNames)
		gv.WithLabelValues(labelValues...).Set(val)
	}
}

// labelsFor returns the label set to use for series: the fixed perfmonLabels
// for PerfMon series, or the established (first-seen-wins) per-series set for
// alert/event series, creating it from tags' own keys if this is the first
// time series has been observed.
func (e *Exporter) labelsFor(series string, tags map[string]string) []string {
	if isPerfmonSeries(series) {
		return perfmonLabels
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if labels, ok := e.seriesLabels[series]; ok {
		return labels
	}

	labels := make([]string, 0, len(tags))
	for k := range tags {
		labels = append(labels, k)
	}
	sort.Strings(labels) // deterministic order, independent of Go's randomized map iteration
	e.seriesLabels[series] = labels
	return labels
}

// gaugeFor returns the GaugeVec for a given (series, fieldName) pair,
// creating and registering it on first use with the given label set.
func (e *Exporter) gaugeFor(series, fieldName string, labelNames []string) *prometheus.GaugeVec {
	name := sanitizeMetricName("dcd_" + series + "_" + fieldName)

	e.mu.Lock()
	defer e.mu.Unlock()

	if gv, ok := e.gauges[name]; ok {
		return gv
	}

	gv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: name,
		Help: fmt.Sprintf("DCD %s.%s (auto-generated)", series, fieldName),
	}, labelNames)

	e.registry.MustRegister(gv)
	e.gauges[name] = gv
	return gv
}

// sanitizeMetricName rewrites s to satisfy Prometheus's metric name rules
// ([a-zA-Z_:][a-zA-Z0-9_:]*). DCD field/series names are already close to
// this (snake_case identifiers from protobuf/JSON), but this guards against
// any unexpected character showing up in a future schema.
func sanitizeMetricName(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			if i == 0 {
				b.WriteRune('_')
			}
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// toFloat64 converts the numeric Go types that actually appear in this
// codebase's "fields" maps into float64 for use as a Prometheus gauge value.
// Most numbers arrive as float64 already (decoder.ProtoToFields round-trips
// through encoding/json, which always produces float64 for JSON numbers),
// but a few fields are constructed directly as int64/uint64 — see
// pkg/decoder/v6_0_0/decoder.go's file_info and probe/alert/event sentinel
// fields (status, event, probe).
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	default:
		return 0, false
	}
}
