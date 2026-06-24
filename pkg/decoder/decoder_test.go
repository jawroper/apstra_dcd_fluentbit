package decoder_test

import (
	"testing"
	"time"

	"github.com/jawroper/apstra-dcd-fluentbit/pkg/config"
	"github.com/jawroper/apstra-dcd-fluentbit/pkg/decoder"
	"github.com/jawroper/apstra-dcd-fluentbit/pkg/restapi"
)

// ----------------------------------------------------------------------------
// Stub REST API client — no network calls
// ----------------------------------------------------------------------------

func newTestDecoder() *decoder.Decoder {
	api := restapi.NewClient("localhost", 443, "admin", "admin", "https")
	// Manually populate the cache so tests don't need a live DCD server.
	api.InjectSystem("device-001", &restapi.System{
		DeviceKey:     "device-001",
		AdminState:    "normal",
		BlueprintID:   "bp-001",
		BlueprintRole: "leaf",
		BlueprintName: "device-001-name",
	})
	api.InjectBlueprint("bp-001", &restapi.Blueprint{ID: "bp-001", Name: "test-blueprint"})

	cfg := &config.Config{OutputFormat: config.OutputFormatJSON}
	return decoder.New(api, cfg)
}

// ----------------------------------------------------------------------------
// GetTags — version-independent: every DCD release uses the same device_key
// and device_key::interface convention.
// ----------------------------------------------------------------------------

func TestGetTags_KnownDevice(t *testing.T) {
	d := newTestDecoder()
	tags := d.GetTags("device-001")
	assertEqual(t, "device-001", tags["device_key"])
	assertEqual(t, "leaf", tags["role"])
	assertEqual(t, "test-blueprint", tags["blueprint"])
	assertEqual(t, "device-001-name", tags["device"])
}

func TestGetTags_InterfaceCompoundKey(t *testing.T) {
	d := newTestDecoder()
	tags := d.GetTags("device-001::eth0")
	assertEqual(t, "device-001", tags["device_key"])
	assertEqual(t, "eth0", tags["interface"])
	assertEqual(t, "device-001-name", tags["device"])
}

func TestGetTags_UnknownDevice(t *testing.T) {
	d := newTestDecoder()
	tags := d.GetTags("ghost-device")
	assertEqual(t, "ghost-device", tags["device"])
	if _, ok := tags["role"]; ok {
		t.Error("expected no role tag for unknown device")
	}
}

// ----------------------------------------------------------------------------
// MetricsObserver wiring
// ----------------------------------------------------------------------------

type fakeObserver struct {
	calls []struct {
		series string
		tags   map[string]string
		fields map[string]interface{}
	}
}

func (f *fakeObserver) Observe(series string, tags map[string]string, fields map[string]interface{}) {
	f.calls = append(f.calls, struct {
		series string
		tags   map[string]string
		fields map[string]interface{}
	}{series, tags, fields})
}

func TestMakeRecord_ForwardsToMetricsObserver(t *testing.T) {
	d := newTestDecoder()
	obs := &fakeObserver{}
	d.SetMetricsObserver(obs)

	d.MakeRecord("interface_counters",
		map[string]string{"device": "spine1"},
		map[string]interface{}{"tx_bytes": int64(123)},
		time.Now(),
	)

	if len(obs.calls) != 1 {
		t.Fatalf("expected 1 Observe call, got %d", len(obs.calls))
	}
	assertEqual(t, "interface_counters", obs.calls[0].series)
	assertEqual(t, "spine1", obs.calls[0].tags["device"])
}

func TestMakeRecord_NoObserverDoesNotPanic(t *testing.T) {
	d := newTestDecoder() // SetMetricsObserver never called
	rec := d.MakeRecord("interface_counters",
		map[string]string{"device": "spine1"},
		map[string]interface{}{"tx_bytes": int64(123)},
		time.Now(),
	)
	if rec["series"] != "interface_counters" {
		t.Errorf("MakeRecord should work normally with no observer attached, got %v", rec)
	}
}

// ----------------------------------------------------------------------------
// SanitizeTimestamp
// ----------------------------------------------------------------------------

// TestSanitizeTimestamp_PlausibleValueKept guards the common case: a
// timestamp close to wall-clock time (e.g. DCD's actual collection time,
// which legitimately differs from "now" by network/processing delay) must
// be kept as-is, not silently overridden.
func TestSanitizeTimestamp_PlausibleValueKept(t *testing.T) {
	candidate := time.Now().Add(-3 * time.Second) // a few seconds of plausible delay
	got := decoder.SanitizeTimestamp(candidate, "device-001")
	if !got.Equal(candidate) {
		t.Errorf("expected plausible timestamp to be kept as-is: want %v, got %v", candidate, got)
	}
}

// TestSanitizeTimestamp_ImplausibleValueRejected is the actual regression
// guard for the production bug: a confirmed-correct DCD server still
// streamed PerfMon timestamps decoding to ~19 years in the past, evidently
// from a stale/different clock source than its own system clock. This must
// fall back to wall-clock time, not propagate the bad value downstream.
func TestSanitizeTimestamp_ImplausibleValueRejected(t *testing.T) {
	candidate := time.Date(2007, 3, 3, 22, 17, 1, 0, time.UTC) // the actual observed bad value
	before := time.Now()
	got := decoder.SanitizeTimestamp(candidate, "device-001")
	after := time.Now()

	if got.Before(before) || got.After(after) {
		t.Errorf("expected fallback to wall-clock time (between %v and %v), got %v — implausible DCD timestamp was not rejected", before, after, got)
	}
}

func TestSanitizeTimestamp_FutureValueAlsoRejected(t *testing.T) {
	candidate := time.Now().Add(48 * time.Hour) // clock skew can go either direction
	got := decoder.SanitizeTimestamp(candidate, "device-001")
	if got.Equal(candidate) {
		t.Error("expected an implausibly-future timestamp to also be rejected, not just past ones")
	}
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

func assertEqual(t *testing.T, expected, actual interface{}) {
	t.Helper()
	if expected != actual {
		t.Errorf("expected %v (%T), got %v (%T)", expected, expected, actual, actual)
	}
}
