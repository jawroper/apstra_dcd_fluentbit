// Package v6_0_0 decodes DCD 6.0.0 streaming telemetry messages.
//
// This file is intentionally a near-mechanical copy of the v6_1_2 package —
// the only differences are the import path (proto/v6_0_0 instead of
// proto/v6_1_2) and whichever oneof cases that specific schema version
// actually defines. See proto/README.md for the full process of adding a
// new DCD release.
package v6_0_0

import (
	"fmt"
	"log"
	"strings"
	"time"

	goproto "google.golang.org/protobuf/proto"

	"github.com/jawroper/apstra-dcd-fluentbit/pkg/decoder"
	proto "github.com/jawroper/apstra-dcd-fluentbit/proto/v6_0_0"
)

// Release is the DCD version string this package decodes, matching the
// `dcd_release` config value that selects it.
const Release = "6.0.0"

// NewHandler returns a function that unmarshals a raw DCD 6.0.0 wire message
// and decodes it into Fluent Bit records, suitable for listener.MessageHandler.
//
// DCD wraps every message in an DcdSequencedMessage envelope (seq_num +
// dcd_proto, the latter being an embedded, separately-serialized DcdMessage)
// — it does NOT send a bare DcdMessage directly, despite what the original
// port of this plugin assumed. Skipping the outer unmarshal previously
// "succeeded" silently: DcdSequencedMessage.seq_num (field 1, varint) shares
// the same wire type as DcdMessage.timestamp (also field 1, varint), so the
// direct-unmarshal-as-DcdMessage attempt produced no error — it just
// captured the sequence number into what looked like a timestamp field,
// while the actual telemetry (sitting in field 15, dcd_proto) was silently
// dropped as an unrecognized field every time.
func NewHandler(d *decoder.Decoder) func([]byte) ([]decoder.Record, error) {
	return func(b []byte) ([]decoder.Record, error) {
		seq := new(proto.DcdSequencedMessage)
		if err := goproto.Unmarshal(b, seq); err != nil {
			return nil, fmt.Errorf("unmarshal DcdSequencedMessage envelope: %w", err)
		}
		msg := new(proto.DcdMessage)
		if err := goproto.Unmarshal(seq.GetDcdProto(), msg); err != nil {
			return nil, fmt.Errorf("unmarshal embedded DcdMessage (seq_num=%d): %w", seq.GetSeqNum(), err)
		}
		return DecodeMessage(d, msg), nil
	}
}

// DecodeMessage is the top-level entry point for one DCD 6.0.0 message.
func DecodeMessage(d *decoder.Decoder, msg *proto.DcdMessage) []decoder.Record {
	var records []decoder.Record

	originName := msg.GetOriginName()

	// timestamp = 1 is a plain uint64, in MICROSECONDS since the epoch (per
	// the .proto comment) — not a google.protobuf.Timestamp message. A value
	// of 0 means DCD didn't set it, so we fall back to wall-clock time.
	ts := time.Now()
	if t := msg.GetTimestamp(); t != 0 {
		ts = decoder.SanitizeTimestamp(time.UnixMicro(int64(t)), originName)
	}

	if pm := msg.GetPerfMon(); pm != nil {
		records = append(records, ExtractPerfMon(d, pm, originName, ts)...)
	}
	if ev := msg.GetEvent(); ev != nil {
		records = append(records, ExtractEventData(d, ev, originName, ts)...)
	}
	if al := msg.GetAlert(); al != nil {
		records = append(records, ExtractAlertData(d, al, originName, ts)...)
	}

	if len(records) == 0 {
		// Diagnostic: none of perfmon(5)/event(4)/alert(3) matched. Find out
		// what — if anything — actually IS set on the wire, even if it's a
		// field number our schema doesn't recognize. This is the same class
		// of silent-failure we already proved happens with mismatched wire
		// types (see the DcdMessage.timestamp investigation): Unmarshal
		// reports no error, the typed getter just returns nil/zero.
		oneofs := msg.ProtoReflect().Descriptor().Oneofs()
		matched := false
		for i := 0; i < oneofs.Len(); i++ {
			o := oneofs.Get(i)
			if fd := msg.ProtoReflect().WhichOneof(o); fd != nil {
				matched = true
				log.Printf("W! [dcd] (v6.0.0) DecodeMessage: oneof %q has field #%d (%s) set on the wire, but it isn't alert(3)/event(4)/perf_mon(5) — schema field numbers may not match this DCD server (origin=%q)",
					o.Name(), fd.Number(), fd.Name(), originName)
			}
		}
		if !matched {
			log.Printf("W! [dcd] (v6.0.0) DecodeMessage: no top-level oneof field set at all (origin=%q, timestamp=%d, hostname=%q, role=%q) — message decoded with zero data fields",
				originName, msg.GetTimestamp(), msg.GetOriginHostname(), msg.GetOriginRole())
		}
	}

	return records
}

// ExtractPerfMon handles all PerfMon sub-types.
func ExtractPerfMon(d *decoder.Decoder, pm *proto.PerfMon, originName string, ts time.Time) []decoder.Record {
	var records []decoder.Record
	tags := d.GetTags(originName)

	// Interface Counters
	if ic := pm.GetInterfaceCounters(); ic != nil {
		fields := decoder.ProtoToFields(ic)
		records = append(records, d.MakeRecord("interface_counters", tags, fields, ts))
	}

	// System Resource Counters
	if rc := pm.GetSystemResourceCounters(); rc != nil {
		if si := rc.GetSystemInfo(); si != nil {
			fields := decoder.ProtoToFields(si)
			records = append(records, d.MakeRecord("system_info", tags, fields, ts))
		}
		for _, p := range rc.GetProcessInfo() {
			procTags := decoder.CopyTags(tags)
			procTags["process_name"] = p.GetProcessName()
			fields := decoder.ProtoToFields(p)
			delete(fields, "process_name")
			records = append(records, d.MakeRecord("process_info", procTags, fields, ts))
		}
		for _, f := range rc.GetFileInfo() {
			fileTags := decoder.CopyTags(tags)
			fileTags["file_name"] = f.GetFileName()
			fields := map[string]interface{}{"size": f.GetFileSize()}
			records = append(records, d.MakeRecord("file_info", fileTags, fields, ts))
		}
	}

	// Probe Message
	if pMsg := pm.GetProbeMessage(); pMsg != nil {
		records = append(records, ExtractProbeData(d, pMsg, originName, ts)...)
	}

	return records
}

// ExtractProbeData processes probe/analytics messages.
func ExtractProbeData(d *decoder.Decoder, msg *proto.ProbeMessage, originName string, ts time.Time) []decoder.Record {
	tags := d.GetTags(originName)

	if id := msg.GetProbeId(); id != "" {
		tags["probe_id"] = id
	}
	if sn := msg.GetStageName(); sn != "" {
		tags["stage_name"] = sn
	}
	if iid := msg.GetItemId(); iid != "" {
		tags["item_id"] = iid
	}
	if pl := msg.GetProbeLabel(); pl != "" {
		tags["probe_label"] = pl
	}
	for _, prop := range msg.GetProperty() {
		if prop.GetName() != "" {
			tags["prop_"+prop.GetName()] = prop.GetValue()
		}
	}

	allFields := decoder.ProtoToFields(msg)
	fields := make(map[string]interface{})
	for k, v := range allFields {
		switch val := v.(type) {
		case string:
			tags[k] = val
		default:
			fields[k] = v
		}
	}
	if len(fields) == 0 {
		fields["probe"] = int64(1)
	}
	return []decoder.Record{d.MakeRecord("probe_message", tags, fields, ts)}
}

// ExtractEventData processes all event sub-types.
func ExtractEventData(d *decoder.Decoder, ev *proto.Event, originName string, ts time.Time) []decoder.Record {
	tags := d.GetTags(originName)

	type eventCase struct {
		name string
		data interface{}
	}

	cases := []eventCase{
		{"device_state", ev.GetDeviceState()},
		{"streaming", ev.GetStreaming()},
		{"cable_peer", ev.GetCablePeer()},
		{"bgp_neighbor", ev.GetBgpNeighbor()},
		{"link_status", ev.GetLinkStatus()},
		{"mac_state", ev.GetMacState()},
		{"arp_state", ev.GetArpState()},
		{"lag_state", ev.GetLagState()},
		{"mlag_state", ev.GetMlagState()},
		{"extensible_event", ev.GetExtensibleEvent()},
		{"route_state", ev.GetRouteState()},
	}

	for _, c := range cases {
		if decoder.IsNilProto(c.data) {
			continue
		}
		eventTags := decoder.CopyTags(tags)
		for k, v := range decoder.ProtoToFields(c.data) {
			s := fmt.Sprintf("%v", v)
			if s != "" {
				eventTags[k] = s
			}
		}
		fields := map[string]interface{}{"event": int64(1)}
		return []decoder.Record{d.MakeRecord("event_"+c.name, eventTags, fields, ts)}
	}

	log.Printf("W! [dcd] (v6.0.0) Unsupported event type received")
	return nil
}

// ExtractAlertData processes all alert sub-types.
func ExtractAlertData(d *decoder.Decoder, al *proto.Alert, originName string, ts time.Time) []decoder.Record {
	tags := d.GetTags(originName)
	tags["severity"] = fmt.Sprintf("%v", al.GetSeverity())
	raised := al.GetRaised()

	type alertCase struct {
		name string
		data interface{}
	}

	cases := []alertCase{
		{"config_deviation_alert", al.GetConfigDeviationAlert()},
		{"streaming_alert", al.GetStreamingAlert()},
		{"cable_peer_mismatch_alert", al.GetCablePeerMismatchAlert()},
		{"bgp_neighbor_mismatch_alert", al.GetBgpNeighborMismatchAlert()},
		{"interface_link_status_mismatch_alert", al.GetInterfaceLinkStatusMismatchAlert()},
		{"hostname_alert", al.GetHostnameAlert()},
		{"route_alert", al.GetRouteAlert()},
		{"liveness_alert", al.GetLivenessAlert()},
		{"deployment_alert", al.GetDeploymentAlert()},
		{"blueprint_rendering_alert", al.GetBlueprintRenderingAlert()},
		{"mac_alert", al.GetMacAlert()},
		{"arp_alert", al.GetArpAlert()},
		{"lag_alert", al.GetLagAlert()},
		{"mlag_alert", al.GetMlagAlert()},
		{"probe_alert", al.GetProbeAlert()},
		{"extensible_alert", al.GetExtensibleAlert()},
		{"test_alert", al.GetTestAlert()},
	}

	for _, c := range cases {
		if decoder.IsNilProto(c.data) {
			continue
		}
		alertTags := decoder.CopyTags(tags)
		for k, v := range decoder.ProtoToFields(c.data) {
			s := fmt.Sprintf("%v", v)
			if s != "" {
				alertTags[k] = s
			}
		}
		seriesName := "alert_" + strings.TrimSuffix(c.name, "_alert")
		status := int64(0)
		if raised {
			status = 1
		}
		fields := map[string]interface{}{"status": status}
		return []decoder.Record{d.MakeRecord(seriesName, alertTags, fields, ts)}
	}

	log.Printf("W! [dcd] (v6.0.0) Unsupported alert type received")
	return nil
}
