package codec

import "encoding/json"

type jsonCodec struct{}

// JSON returns the default JSON codec.
func JSON() Codec { return jsonCodec{} }

func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
