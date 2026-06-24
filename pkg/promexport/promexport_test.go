package promexport_test

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jawroper/apstra-dcd-fluentbit/pkg/promexport"
)

// startTestExporter starts an Exporter allowing all types, on an ephemeral
// port, and returns it plus a function to scrape its /metrics endpoint.
func startTestExporter(t *testing.T) (*promexport.Exporter, func() string) {
	return startTestExporterWithTypes(t, nil)
}

// startTestExporterWithTypes starts an Exporter allowing only the given
// streaming types.
func startTestExporterWithTypes(t *testing.T, types []string) (*promexport.Exporter, func() string) {
	t.Helper()
	e := promexport.New(types)

	// Use a high, likely-free port; retry a couple of times if it's taken.
	var lastErr error
	port := 19100
	for i := 0; i < 5; i++ {
		if err := e.Start(port + i); err != nil {
			lastErr = err
			continue
		}
		port = port + i
		lastErr = nil
		break
	}
	if lastErr != nil {
		t.Fatalf("could not start exporter: %v", lastErr)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})

	scrape := func() string {
		// Small retry loop: the HTTP server starts in a goroutine, so the
		// very first request right after Start() can occasionally race it.
		var body string
		for i := 0; i < 20; i++ {
			resp, err := http.Get("http://127.0.0.1:" + strconv.Itoa(port) + "/metrics")
			if err == nil {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				body = string(b)
				return body
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("could not scrape /metrics after retries")
		return ""
	}

	return e, scrape
}

func TestObserve_PerfMonSeriesExposed(t *testing.T) {
	e, scrape := startTestExporter(t)

	e.Observe("interface_counters",
		map[string]string{"device": "spine1", "interface": "et-0/0/1", "role": "spine"},
		map[string]interface{}{"tx_bytes": float64(12345), "rx_bytes": float64(67890)},
	)

	body := scrape()
	if !strings.Contains(body, "dcd_interface_counters_tx_bytes") {
		t.Errorf("expected dcd_interface_counters_tx_bytes in scrape output, got:\n%s", body)
	}
	if !strings.Contains(body, `device="spine1"`) {
		t.Errorf("expected device label in scrape output, got:\n%s", body)
	}
	if !strings.Contains(body, "12345") {
		t.Errorf("expected value 12345 in scrape output, got:\n%s", body)
	}
}

// TestObserve_AlertSeriesExposed confirms alerts are now exported (this used
// to be excluded by design — see promexport.go's package doc comment for why
// that scope decision was corrected), using that alert type's own tag keys
// as labels, matching the pattern real Telegraf-based DCD Prometheus
// exporters (e.g. the dcdom project) already use in production.
func TestObserve_AlertSeriesExposed(t *testing.T) {
	e, scrape := startTestExporter(t)

	e.Observe("alert_bgp_neighbor_mismatch",
		map[string]string{
			"device":         "virtual_mlag_1_leaf2",
			"severity":       "ALERT_CRITICAL",
			"actual_state":   "BGP_SESSION_DOWN",
			"expected_state": "BGP_SESSION_UP",
			"lcl_asn":        "101",
			"rmt_asn":        "100",
		},
		map[string]interface{}{"status": int64(1)},
	)

	body := scrape()
	if !strings.Contains(body, "dcd_alert_bgp_neighbor_mismatch_status") {
		t.Errorf("expected alert metric in scrape output, got:\n%s", body)
	}
	for _, want := range []string{`device="virtual_mlag_1_leaf2"`, `actual_state="BGP_SESSION_DOWN"`, `expected_state="BGP_SESSION_UP"`, `lcl_asn="101"`} {
		if !strings.Contains(body, want) {
			t.Errorf("expected label %s in scrape output, got:\n%s", want, body)
		}
	}
}

func TestObserve_EventSeriesExposed(t *testing.T) {
	e, scrape := startTestExporter(t)

	e.Observe("event_link_status",
		map[string]string{"device": "spine1", "interface": "et-0/0/48", "state": "down"},
		map[string]interface{}{"event": int64(1)},
	)

	body := scrape()
	if !strings.Contains(body, "dcd_event_link_status_event") {
		t.Errorf("expected event metric in scrape output, got:\n%s", body)
	}
	if !strings.Contains(body, `state="down"`) {
		t.Errorf("expected event-specific label in scrape output, got:\n%s", body)
	}
}

// TestObserve_DifferentAlertTypesGetIndependentLabelSets is the key new
// behavior this change adds: unlike PerfMon (one fixed label set for
// everything), each alert/event type gets its OWN label set, derived from
// its own tag keys — alert_bgp_neighbor_mismatch's actual_state/expected_state
// must not leak onto alert_liveness's metric, and vice versa.
func TestObserve_DifferentAlertTypesGetIndependentLabelSets(t *testing.T) {
	e, scrape := startTestExporter(t)

	e.Observe("alert_bgp_neighbor_mismatch",
		map[string]string{"device": "leaf1", "actual_state": "DOWN", "expected_state": "UP"},
		map[string]interface{}{"status": int64(1)},
	)
	e.Observe("alert_liveness",
		map[string]string{"device": "leaf2", "severity": "ALERT_LOW"},
		map[string]interface{}{"status": int64(0)},
	)

	body := scrape()

	// Find each metric's own line and confirm it doesn't carry the other
	// alert type's labels.
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "dcd_alert_liveness_status{") {
			if strings.Contains(line, "actual_state") {
				t.Errorf("alert_liveness metric should not carry alert_bgp_neighbor_mismatch's labels, got: %s", line)
			}
		}
		if strings.HasPrefix(line, "dcd_alert_bgp_neighbor_mismatch_status{") {
			if strings.Contains(line, "severity") {
				// severity wasn't included in this test's bgp tags, so this
				// would indicate label-set bleed between series.
				t.Errorf("alert_bgp_neighbor_mismatch metric should not carry alert_liveness's labels, got: %s", line)
			}
		}
	}
}

// TestObserve_AlertLabelSetStableAcrossCalls confirms first-seen-wins: once a
// series' label set is established, a later call with different tag keys
// must not panic (Prometheus client_golang panics on label cardinality
// mismatch) — extra keys are dropped, missing ones default to "".
func TestObserve_AlertLabelSetStableAcrossCalls(t *testing.T) {
	e, scrape := startTestExporter(t)

	e.Observe("alert_route",
		map[string]string{"device": "leaf1", "prefix": "10.0.0.0/24"},
		map[string]interface{}{"status": int64(1)},
	)
	// Second call: missing "prefix", has a new unseen key "extra" — must not panic.
	e.Observe("alert_route",
		map[string]string{"device": "leaf1", "extra": "unexpected"},
		map[string]interface{}{"status": int64(0)},
	)

	body := scrape()
	if !strings.Contains(body, "dcd_alert_route_status") {
		t.Errorf("expected alert_route metric in scrape output, got:\n%s", body)
	}
	if strings.Contains(body, "extra=") {
		t.Errorf("a label key not present on the first (establishing) call should not appear later, got:\n%s", body)
	}
}

func TestObserve_NonNumericFieldsSkipped(t *testing.T) {
	e, scrape := startTestExporter(t)

	// "fields" should always be numeric by construction, but Observe must not
	// panic or produce garbage output if a non-numeric value ever sneaks in.
	e.Observe("probe_message",
		map[string]string{"device": "spine1", "probe_id": "p1"},
		map[string]interface{}{
			"good_metric": float64(42),
			"bad_string":  "not a number",
			"bad_nil":     nil,
		},
	)

	body := scrape()
	if !strings.Contains(body, "dcd_probe_message_good_metric") {
		t.Errorf("expected the valid numeric field to be exported, got:\n%s", body)
	}
	if strings.Contains(body, "bad_string") || strings.Contains(body, "bad_nil") {
		t.Errorf("non-numeric fields should be silently skipped, not exported, got:\n%s", body)
	}
}

func TestObserve_MultipleFieldsBecomeSeparateMetrics(t *testing.T) {
	e, scrape := startTestExporter(t)

	e.Observe("interface_counters",
		map[string]string{"device": "spine1", "interface": "et-0/0/1"},
		map[string]interface{}{
			"tx_bytes": float64(100),
			"rx_bytes": float64(200),
		},
	)

	body := scrape()
	for _, want := range []string{"dcd_interface_counters_tx_bytes", "dcd_interface_counters_rx_bytes"} {
		if !strings.Contains(body, want) {
			t.Errorf("expected metric %s in scrape output, got:\n%s", want, body)
		}
	}
}

func TestObserve_MissingTagsDefaultToEmptyLabel(t *testing.T) {
	e, scrape := startTestExporter(t)

	// system_info records never have an "interface" tag — confirm this
	// doesn't break the fixed canonical label set (must still scrape cleanly
	// with interface="").
	e.Observe("system_info",
		map[string]string{"device": "spine1", "role": "spine"}, // no "interface" key
		map[string]interface{}{"cpu_user": float64(12.5)},
	)

	body := scrape()
	if !strings.Contains(body, `interface=""`) {
		t.Errorf("expected interface label to default to empty string, got:\n%s", body)
	}
}

func TestObserve_RepeatedObserveUpdatesGaugeValue(t *testing.T) {
	e, scrape := startTestExporter(t)

	tags := map[string]string{"device": "spine1", "interface": "et-0/0/1"}
	e.Observe("interface_counters", tags, map[string]interface{}{"tx_bytes": float64(100)})
	e.Observe("interface_counters", tags, map[string]interface{}{"tx_bytes": float64(999)})

	body := scrape()
	if strings.Contains(body, "tx_bytes{") && strings.Contains(body, " 100\n") {
		t.Errorf("expected the gauge to be updated to 999, not left at the old value 100:\n%s", body)
	}
	if !strings.Contains(body, "999") {
		t.Errorf("expected updated value 999 in scrape output, got:\n%s", body)
	}
}

// ----------------------------------------------------------------------------
// prometheus_streaming_types filtering
// ----------------------------------------------------------------------------

// TestObserve_PerfmonOnlyFiltering confirms that when only "perfmon" is
// allowed, alert and event series are silently skipped.
func TestObserve_PerfmonOnlyFiltering(t *testing.T) {
	e, scrape := startTestExporterWithTypes(t, []string{"perfmon"})

	e.Observe("interface_counters",
		map[string]string{"device": "spine1"},
		map[string]interface{}{"tx_bytes": float64(100)},
	)
	e.Observe("alert_liveness",
		map[string]string{"device": "spine1", "severity": "ALERT_CRITICAL"},
		map[string]interface{}{"status": int64(1)},
	)
	e.Observe("event_link_status",
		map[string]string{"device": "spine1"},
		map[string]interface{}{"event": int64(1)},
	)

	body := scrape()
	if !strings.Contains(body, "dcd_interface_counters_tx_bytes") {
		t.Errorf("perfmon series should be exported when type is 'perfmon', got:\n%s", body)
	}
	if strings.Contains(body, "alert_liveness") {
		t.Errorf("alert series should be excluded when type is 'perfmon' only, got:\n%s", body)
	}
	if strings.Contains(body, "event_link_status") {
		t.Errorf("event series should be excluded when type is 'perfmon' only, got:\n%s", body)
	}
}

// TestObserve_AlertsOnlyFiltering confirms that when only "alerts" is allowed,
// perfmon and event series are silently skipped.
func TestObserve_AlertsOnlyFiltering(t *testing.T) {
	e, scrape := startTestExporterWithTypes(t, []string{"alerts"})

	e.Observe("interface_counters",
		map[string]string{"device": "spine1"},
		map[string]interface{}{"tx_bytes": float64(100)},
	)
	e.Observe("alert_liveness",
		map[string]string{"device": "spine1", "severity": "ALERT_CRITICAL"},
		map[string]interface{}{"status": int64(1)},
	)
	e.Observe("event_link_status",
		map[string]string{"device": "spine1"},
		map[string]interface{}{"event": int64(1)},
	)

	body := scrape()
	if strings.Contains(body, "interface_counters") {
		t.Errorf("perfmon series should be excluded when type is 'alerts' only, got:\n%s", body)
	}
	if !strings.Contains(body, "dcd_alert_liveness_status") {
		t.Errorf("alert series should be exported when type is 'alerts', got:\n%s", body)
	}
	if strings.Contains(body, "event_link_status") {
		t.Errorf("event series should be excluded when type is 'alerts' only, got:\n%s", body)
	}
}

// TestObserve_NilTypesAllowsAll confirms that passing nil (the default when
// prometheus_streaming_types is not set) exports everything — matching
// whatever streaming_types DCD is subscribed to.
func TestObserve_NilTypesAllowsAll(t *testing.T) {
	e, scrape := startTestExporterWithTypes(t, nil)

	e.Observe("interface_counters",
		map[string]string{"device": "spine1"},
		map[string]interface{}{"tx_bytes": float64(100)},
	)
	e.Observe("alert_liveness",
		map[string]string{"device": "spine1", "severity": "ALERT_CRITICAL"},
		map[string]interface{}{"status": int64(1)},
	)
	e.Observe("event_link_status",
		map[string]string{"device": "spine1"},
		map[string]interface{}{"event": int64(1)},
	)

	body := scrape()
	for _, want := range []string{"dcd_interface_counters_tx_bytes", "dcd_alert_liveness_status", "dcd_event_link_status_event"} {
		if !strings.Contains(body, want) {
			t.Errorf("expected all types when nil passed to New(), missing %s in:\n%s", want, body)
		}
	}
}
