package v6_1_2_test

import (
	"testing"
	"time"

	goproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jawroper/apstra-dcd-fluentbit/pkg/config"
	"github.com/jawroper/apstra-dcd-fluentbit/pkg/decoder"
	v6_1_2 "github.com/jawroper/apstra-dcd-fluentbit/pkg/decoder/v6_1_2"
	"github.com/jawroper/apstra-dcd-fluentbit/pkg/restapi"
	dcd_proto "github.com/jawroper/apstra-dcd-fluentbit/proto/v6_1_2"
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
	records := v6_1_2.ExtractPerfMon(d, pm, "device-001", testTime)
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

	records := v6_1_2.ExtractPerfMon(d, pm, "device-001", testTime)
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

	records := v6_1_2.ExtractPerfMon(d, pm, "device-001", testTime)
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

	records := v6_1_2.ExtractAlertData(d, al, "device-001", testTime)
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

	records := v6_1_2.ExtractAlertData(d, al, "device-001", testTime)
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
	records := v6_1_2.ExtractAlertData(d, al, "device-001", testTime)
	if len(records) != 0 {
		t.Fatalf("expected 0 records for unsupported alert, got %d", len(records))
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

	records := v6_1_2.ExtractEventData(d, ev, "device-001", testTime)
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

	records := v6_1_2.ExtractEventData(d, ev, "device-001", testTime)
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

	records := v6_1_2.ExtractPerfMon(d, pm, "device-001", testTime)
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

	records := v6_1_2.DecodeMessage(d, msg)
	if len(records) != 1 {
		t.Fatalf("expected 1 record from PerfMon message, got %d", len(records))
	}
	assertEqual(t, "interface_counters", records[0]["series"])

	msg2 := &dcd_proto.DcdMessage{OriginName: strPtr("device-001")}
	msg2.Data = &dcd_proto.DcdMessage_Event{Event: ev}
	records2 := v6_1_2.DecodeMessage(d, msg2)
	if len(records2) != 1 {
		t.Fatalf("expected 1 record from Event message, got %d", len(records2))
	}
	assertEqual(t, "event_link_status", records2[0]["series"])
}

func TestDecodeMessage_Empty(t *testing.T) {
	d := newTestDecoder()
	msg := &dcd_proto.DcdMessage{OriginName: strPtr("device-001")}
	records := v6_1_2.DecodeMessage(d, msg)
	if len(records) != 0 {
		t.Fatalf("expected 0 records for empty message, got %d", len(records))
	}
}

// TestDecodeMessage_TimestampMessage verifies the DCD 6.1.2-specific
// google.protobuf.Timestamp handling (distinct from DCD 6.0.0, which encodes
// the same field as a raw uint64 of microseconds — see the matching test in
// pkg/decoder/v6_0_0).
func TestDecodeMessage_TimestampMessage(t *testing.T) {
	d := newTestDecoder()
	pm := &dcd_proto.PerfMon{}
	pm.Data = &dcd_proto.PerfMon_InterfaceCounters{
		InterfaceCounters: &dcd_proto.InterfaceCounters{RxBytes: uint64Ptr(1)},
	}

	// A relative offset, not a hardcoded calendar date — see the matching
	// comment in pkg/decoder/v6_0_0's version of this test for why.
	want := time.Now().Add(-1 * time.Hour).Truncate(time.Nanosecond)

	msg := &dcd_proto.DcdMessage{
		OriginName: strPtr("device-001"),
		Timestamp:  timestamppb.New(want),
	}
	msg.Data = &dcd_proto.DcdMessage_PerfMon{PerfMon: pm}

	records := v6_1_2.DecodeMessage(d, msg)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	gotNanos, ok := records[0]["_ts"].(int64)
	if !ok {
		t.Fatalf("expected _ts to be int64, got %T", records[0]["_ts"])
	}
	got := time.Unix(0, gotNanos).UTC()
	if !got.Equal(want) {
		t.Errorf("timestamp mismatch: want %v, got %v", want, got)
	}
}

// TestDecodeMessage_ImplausibleTimestampFallsBackToNow — see the matching
// test in pkg/decoder/v6_0_0 for the full story. Same guard, but using this
// schema's google.protobuf.Timestamp message instead of a raw uint64.
func TestDecodeMessage_ImplausibleTimestampFallsBackToNow(t *testing.T) {
	d := newTestDecoder()
	pm := &dcd_proto.PerfMon{}
	pm.Data = &dcd_proto.PerfMon_InterfaceCounters{
		InterfaceCounters: &dcd_proto.InterfaceCounters{RxBytes: uint64Ptr(1)},
	}

	badTimestamp := time.Date(2007, 3, 3, 22, 17, 1, 0, time.UTC)

	msg := &dcd_proto.DcdMessage{
		OriginName: strPtr("device-001"),
		Timestamp:  timestamppb.New(badTimestamp),
	}
	msg.Data = &dcd_proto.DcdMessage_PerfMon{PerfMon: pm}

	before := time.Now()
	records := v6_1_2.DecodeMessage(d, msg)
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

// TestNewHandler_UnwrapsSequencedEnvelope — see the matching test in
// pkg/decoder/v6_0_0 for the full story. Same bug, same fix, different field
// number for dcd_proto (2 here instead of 15).
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

	handler := v6_1_2.NewHandler(d)
	records, err := handler(wireBytes)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record after unwrapping the envelope, got %d — envelope unwrapping may be broken", len(records))
	}
	assertEqual(t, "interface_counters", records[0]["series"])
}

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

	handler := v6_1_2.NewHandler(d)
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
