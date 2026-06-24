package v6_0_0_test

import (
	"testing"
	"time"

	goproto "google.golang.org/protobuf/proto"

	"github.com/jawroper/apstra-dcd-fluentbit/pkg/config"
	"github.com/jawroper/apstra-dcd-fluentbit/pkg/decoder"
	v6_0_0 "github.com/jawroper/apstra-dcd-fluentbit/pkg/decoder/v6_0_0"
	"github.com/jawroper/apstra-dcd-fluentbit/pkg/restapi"
	dcd_proto "github.com/jawroper/apstra-dcd-fluentbit/proto/v6_0_0"
)

// ----------------------------------------------------------------------------
// Stub REST API client — no network calls
// ----------------------------------------------------------------------------

func newTestDecoder() *decoder.Decoder {
	api := restapi.NewClient("localhost", 443, "admin", "admin", "https")
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

var testTime = time.Unix(1_700_000_000, 0)

// ----------------------------------------------------------------------------
// Interface Counters
// ----------------------------------------------------------------------------

func TestExtractPerfMon_InterfaceCounters(t *testing.T) {
	d := newTestDecoder()
	pm := &dcd_proto.PerfMon{}
	pm.Data = &dcd_proto.PerfMon_InterfaceCounters{
		InterfaceCounters: &dcd_proto.InterfaceCounters{
			TxBytes: uint64Ptr(67890),
			RxBytes: uint64Ptr(12345),
		},
	}
	records := v6_0_0.ExtractPerfMon(d, pm, "device-001", testTime)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	r := records[0]
	assertEqual(t, "interface_counters", r["series"])
	assertEqual(t, "device-001-name", r["device"])
	assertEqual(t, "leaf", r["role"])
	assertEqual(t, "test-blueprint", r["blueprint"])
}

// ----------------------------------------------------------------------------
// System Info
// ----------------------------------------------------------------------------

func TestExtractPerfMon_SystemInfo(t *testing.T) {
	d := newTestDecoder()
	pm := &dcd_proto.PerfMon{}
	pm.Data = &dcd_proto.PerfMon_SystemResourceCounters{
		SystemResourceCounters: &dcd_proto.SysResourceCounters{
			SystemInfo: &dcd_proto.SystemInfo{
				CpuUser:     float32Ptr(12.5),
				MemoryUsed:  uint64Ptr(1024),
				MemoryTotal: uint64Ptr(8192),
			},
		},
	}

	records := v6_0_0.ExtractPerfMon(d, pm, "device-001", testTime)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	assertEqual(t, "system_info", records[0]["series"])
}

// ----------------------------------------------------------------------------
// Process Info
// ----------------------------------------------------------------------------

func TestExtractPerfMon_ProcessInfo(t *testing.T) {
	d := newTestDecoder()
	pm := &dcd_proto.PerfMon{}
	pm.Data = &dcd_proto.PerfMon_SystemResourceCounters{
		SystemResourceCounters: &dcd_proto.SysResourceCounters{
			ProcessInfo: []*dcd_proto.ProcessInfo{
				{ProcessName: strPtr("bgpd")},
				{ProcessName: strPtr("ospfd")},
			},
		},
	}

	records := v6_0_0.ExtractPerfMon(d, pm, "device-001", testTime)
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	assertEqual(t, "process_info", records[0]["series"])
	assertEqual(t, "bgpd", records[0]["process_name"])
	assertEqual(t, "ospfd", records[1]["process_name"])
}

// ----------------------------------------------------------------------------
// Alerts
// ----------------------------------------------------------------------------

func TestExtractAlertData_Raised(t *testing.T) {
	d := newTestDecoder()
	severity := dcd_proto.AlertSeverity_ALERT_CRITICAL
	al := &dcd_proto.Alert{
		Severity: &severity,
		Raised:   boolPtr(true),
	}
	al.Data = &dcd_proto.Alert_LivenessAlert{
		LivenessAlert: &dcd_proto.LivenessAlert{},
	}

	records := v6_0_0.ExtractAlertData(d, al, "device-001", testTime)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	r := records[0]
	assertEqual(t, "alert_liveness", r["series"])
	assertEqual(t, int64(1), r["status"])
}

func TestExtractAlertData_Cleared(t *testing.T) {
	d := newTestDecoder()
	severity := dcd_proto.AlertSeverity_ALERT_LOW
	al := &dcd_proto.Alert{
		Severity: &severity,
		Raised:   boolPtr(false),
	}
	al.Data = &dcd_proto.Alert_BgpNeighborMismatchAlert{
		BgpNeighborMismatchAlert: &dcd_proto.BGPNeighborMismatchAlert{},
	}

	records := v6_0_0.ExtractAlertData(d, al, "device-001", testTime)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	assertEqual(t, "alert_bgp_neighbor_mismatch", records[0]["series"])
	assertEqual(t, int64(0), records[0]["status"])
}

func TestExtractAlertData_UnsupportedType(t *testing.T) {
	d := newTestDecoder()
	severity := dcd_proto.AlertSeverity_ALERT_LOW
	al := &dcd_proto.Alert{Severity: &severity, Raised: boolPtr(false)}
	records := v6_0_0.ExtractAlertData(d, al, "device-001", testTime)
	if len(records) != 0 {
		t.Fatalf("expected 0 records for unsupported alert, got %d", len(records))
	}
}

// TestExtractAlertData_EmptyTagValuesOmitted guards against the production bug
// where alerts with empty protobuf string fields (e.g. rmt_hostname="",
// rmt_sysdescr="", rmt_ifname="") caused InfluxDB to reject the entire batch
// with HTTP 400 "missing tag value" — InfluxDB's line protocol requires that
// any tag present in a record has a non-empty value.
func TestExtractAlertData_EmptyTagValuesOmitted(t *testing.T) {
	d := newTestDecoder()
	severity := dcd_proto.AlertSeverity_ALERT_CRITICAL
	al := &dcd_proto.Alert{
		Severity: &severity,
		Raised:   boolPtr(true),
	}
	al.Data = &dcd_proto.Alert_CablePeerMismatchAlert{
		CablePeerMismatchAlert: &dcd_proto.CablePeerMismatchAlert{},
	}

	records := v6_0_0.ExtractAlertData(d, al, "device-001", testTime)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	for k, v := range records[0] {
		if s, ok := v.(string); ok && s == "" {
			t.Errorf("tag %q has empty string value — InfluxDB line protocol rejects this with HTTP 400 'missing tag value'", k)
		}
	}
}

// TestExtractEventData_EmptyTagValuesOmitted — same guard for events.
func TestExtractEventData_EmptyTagValuesOmitted(t *testing.T) {
	d := newTestDecoder()
	ev := &dcd_proto.Event{}
	ev.Data = &dcd_proto.Event_CablePeer{
		CablePeer: &dcd_proto.CablePeerEvent{},
	}

	records := v6_0_0.ExtractEventData(d, ev, "device-001", testTime)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	for k, v := range records[0] {
		if s, ok := v.(string); ok && s == "" {
			t.Errorf("tag %q has empty string value — InfluxDB line protocol rejects this with HTTP 400 'missing tag value'", k)
		}
	}
}

// ----------------------------------------------------------------------------
// Events
// ----------------------------------------------------------------------------

func TestExtractEventData_BgpNeighbor(t *testing.T) {
	d := newTestDecoder()
	ev := &dcd_proto.Event{}
	ev.Data = &dcd_proto.Event_BgpNeighbor{
		BgpNeighbor: &dcd_proto.BGPNeighborEvent{
			RmtIpaddr: strPtr("10.0.0.2"),
		},
	}

	records := v6_0_0.ExtractEventData(d, ev, "device-001", testTime)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	r := records[0]
	assertEqual(t, "event_bgp_neighbor", r["series"])
	assertEqual(t, int64(1), r["event"])
}

func TestExtractEventData_LinkStatus(t *testing.T) {
	d := newTestDecoder()
	ev := &dcd_proto.Event{}
	ev.Data = &dcd_proto.Event_LinkStatus{
		LinkStatus: &dcd_proto.LinkStatusEvent{},
	}

	records := v6_0_0.ExtractEventData(d, ev, "device-001", testTime)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	assertEqual(t, "event_link_status", records[0]["series"])
}

// ----------------------------------------------------------------------------
// Probe Message
// ----------------------------------------------------------------------------

func TestExtractPerfMon_ProbeMessage(t *testing.T) {
	d := newTestDecoder()
	pm := &dcd_proto.PerfMon{}
	probeID := "probe-42"
	stage := "my_stage"
	pm.Data = &dcd_proto.PerfMon_ProbeMessage{
		ProbeMessage: &dcd_proto.ProbeMessage{
			ProbeId:   &probeID,
			StageName: &stage,
		},
	}

	records := v6_0_0.ExtractPerfMon(d, pm, "device-001", testTime)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	r := records[0]
	assertEqual(t, "probe_message", r["series"])
}

// ----------------------------------------------------------------------------
// Full DecodeMessage dispatch
// ----------------------------------------------------------------------------

func TestDecodeMessage_PerfMonAndEvent(t *testing.T) {
	d := newTestDecoder()
	pm := &dcd_proto.PerfMon{}
	pm.Data = &dcd_proto.PerfMon_InterfaceCounters{
		InterfaceCounters: &dcd_proto.InterfaceCounters{RxBytes: uint64Ptr(100)},
	}

	ev := &dcd_proto.Event{}
	ev.Data = &dcd_proto.Event_LinkStatus{
		LinkStatus: &dcd_proto.LinkStatusEvent{},
	}

	msg := &dcd_proto.DcdMessage{
		OriginName: strPtr("device-001"),
	}
	msg.Data = &dcd_proto.DcdMessage_PerfMon{PerfMon: pm}

	records := v6_0_0.DecodeMessage(d, msg)
	if len(records) != 1 {
		t.Fatalf("expected 1 record from PerfMon message, got %d", len(records))
	}
	assertEqual(t, "interface_counters", records[0]["series"])

	msg2 := &dcd_proto.DcdMessage{OriginName: strPtr("device-001")}
	msg2.Data = &dcd_proto.DcdMessage_Event{Event: ev}
	records2 := v6_0_0.DecodeMessage(d, msg2)
	if len(records2) != 1 {
		t.Fatalf("expected 1 record from Event message, got %d", len(records2))
	}
	assertEqual(t, "event_link_status", records2[0]["series"])
}

func TestDecodeMessage_Empty(t *testing.T) {
	d := newTestDecoder()
	msg := &dcd_proto.DcdMessage{OriginName: strPtr("device-001")}
	records := v6_0_0.DecodeMessage(d, msg)
	if len(records) != 0 {
		t.Fatalf("expected 0 records for empty message, got %d", len(records))
	}
}

// TestDecodeMessage_TimestampMicroseconds verifies the DCD 6.0.0-specific
// uint64-microseconds timestamp conversion (the bug originally caught by
// `time.Unix(0, t)` treating microseconds as nanoseconds).
func TestDecodeMessage_TimestampMicroseconds(t *testing.T) {
	d := newTestDecoder()
	pm := &dcd_proto.PerfMon{}
	pm.Data = &dcd_proto.PerfMon_InterfaceCounters{
		InterfaceCounters: &dcd_proto.InterfaceCounters{RxBytes: uint64Ptr(1)},
	}

	// A relative offset, not a hardcoded calendar date — this needs to stay
	// within SanitizeTimestamp's plausibility window (see decoder.MaxClockSkew)
	// regardless of what year this test actually runs in. The unit-conversion
	// logic under test (microseconds -> nanoseconds round-trip fidelity)
	// doesn't care what the value is, just that it round-trips exactly.
	want := time.Now().Add(-1 * time.Hour).Truncate(time.Microsecond)
	micros := uint64(want.UnixMicro())

	msg := &dcd_proto.DcdMessage{
		OriginName: strPtr("device-001"),
		Timestamp:  &micros,
	}
	msg.Data = &dcd_proto.DcdMessage_PerfMon{PerfMon: pm}

	records := v6_0_0.DecodeMessage(d, msg)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	gotNanos, ok := records[0]["_ts"].(int64)
	if !ok {
		t.Fatalf("expected _ts to be int64, got %T", records[0]["_ts"])
	}
	got := time.Unix(0, gotNanos).UTC()
	if !got.Equal(want) {
		t.Errorf("timestamp mismatch: want %v, got %v (off by %v) — check for a microseconds/nanoseconds unit bug", want, got, want.Sub(got))
	}
}

// TestDecodeMessage_ImplausibleTimestampFallsBackToNow is the end-to-end
// regression guard for the real production bug: an DCD server with a
// confirmed-correct system clock still streamed PerfMon messages whose
// embedded timestamp decoded to 2007-03-03 — ~19 years in the past,
// evidently from a stale clock source specific to its streaming telemetry
// subsystem. DecodeMessage must not propagate that bad value into records.
func TestDecodeMessage_ImplausibleTimestampFallsBackToNow(t *testing.T) {
	d := newTestDecoder()
	pm := &dcd_proto.PerfMon{}
	pm.Data = &dcd_proto.PerfMon_InterfaceCounters{
		InterfaceCounters: &dcd_proto.InterfaceCounters{RxBytes: uint64Ptr(1)},
	}

	badTimestamp := time.Date(2007, 3, 3, 22, 17, 1, 0, time.UTC)
	micros := uint64(badTimestamp.UnixMicro())

	msg := &dcd_proto.DcdMessage{
		OriginName: strPtr("device-001"),
		Timestamp:  &micros,
	}
	msg.Data = &dcd_proto.DcdMessage_PerfMon{PerfMon: pm}

	before := time.Now()
	records := v6_0_0.DecodeMessage(d, msg)
	after := time.Now()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	gotNanos, ok := records[0]["_ts"].(int64)
	if !ok {
		t.Fatalf("expected _ts to be int64, got %T", records[0]["_ts"])
	}
	got := time.Unix(0, gotNanos)
	if got.Before(before) || got.After(after) {
		t.Errorf("expected fallback to wall-clock time (between %v and %v), got %v — the implausible 2007 timestamp was not rejected", before, after, got)
	}
}

// ----------------------------------------------------------------------------
// NewHandler — wire-level envelope unwrapping
// ----------------------------------------------------------------------------

// TestNewHandler_UnwrapsSequencedEnvelope guards against the real bug found in
// production: DCD wraps every message in an DcdSequencedMessage envelope
// (seq_num + dcd_proto, the latter being a separately-serialized DcdMessage).
// Unmarshaling the wire bytes directly as DcdMessage "succeeds" with no error
// — DcdSequencedMessage.seq_num (field 1, varint) happens to share the same
// wire type as DcdMessage.timestamp (also field 1, varint) — but silently
// drops the real payload, which sits in field 15 (dcd_proto) and isn't part
// of the DcdMessage schema at all. This test fails if that unwrapping step is
// ever accidentally removed.
func TestNewHandler_UnwrapsSequencedEnvelope(t *testing.T) {
	d := newTestDecoder()

	inner := &dcd_proto.DcdMessage{OriginName: strPtr("device-001")}
	inner.Data = &dcd_proto.DcdMessage_PerfMon{
		PerfMon: &dcd_proto.PerfMon{
			Data: &dcd_proto.PerfMon_InterfaceCounters{
				InterfaceCounters: &dcd_proto.InterfaceCounters{RxBytes: uint64Ptr(42)},
			},
		},
	}
	innerBytes, err := goproto.Marshal(inner)
	if err != nil {
		t.Fatalf("marshal inner DcdMessage: %v", err)
	}

	envelope := &dcd_proto.DcdSequencedMessage{
		SeqNum:   uint64Ptr(7),
		DcdProto: innerBytes,
	}
	wireBytes, err := goproto.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal DcdSequencedMessage envelope: %v", err)
	}

	handler := v6_0_0.NewHandler(d)
	records, err := handler(wireBytes)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record after unwrapping the envelope, got %d — envelope unwrapping may be broken", len(records))
	}
	assertEqual(t, "interface_counters", records[0]["series"])
}

// TestNewHandler_RejectsBareDcdMessage documents the inverse of the above: if
// someone ever sent a bare DcdMessage (not wrapped), this handler would NOT
// silently succeed the way the original bug did — it would either error or
// (per the same field-1 wire-type coincidence) misinterpret it. This test
// exists to make that behavior explicit and visible rather than rediscovered
// the hard way again.
func TestNewHandler_RejectsBareDcdMessage(t *testing.T) {
	d := newTestDecoder()

	bare := &dcd_proto.DcdMessage{OriginName: strPtr("device-001")}
	bare.Data = &dcd_proto.DcdMessage_PerfMon{
		PerfMon: &dcd_proto.PerfMon{
			Data: &dcd_proto.PerfMon_InterfaceCounters{
				InterfaceCounters: &dcd_proto.InterfaceCounters{RxBytes: uint64Ptr(42)},
			},
		},
	}
	bareBytes, err := goproto.Marshal(bare)
	if err != nil {
		t.Fatalf("marshal bare DcdMessage: %v", err)
	}

	handler := v6_0_0.NewHandler(d)
	records, _ := handler(bareBytes)
	if len(records) != 0 {
		t.Errorf("expected 0 records for a bare (non-enveloped) DcdMessage, got %d — "+
			"if this now passes, DCD's wire format may have changed again", len(records))
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

func strPtr(s string) *string       { return &s }
func boolPtr(b bool) *bool          { return &b }
func uint64Ptr(n uint64) *uint64    { return &n }
func float32Ptr(f float32) *float32 { return &f }
