package decoder

import "encoding/json"

// marshalProtoJSON serialises any value to JSON bytes.
// For real deployments the proto-generated types implement json.Marshaler.
// The proto stub types used in tests fall through to standard encoding/json.
func marshalProtoJSON(msg interface{}) ([]byte, error) {
	return json.Marshal(msg)
}

func unmarshalJSON(b []byte, out *Record) error {
	return json.Unmarshal(b, out)
}
