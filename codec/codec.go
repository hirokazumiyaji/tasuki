package codec

// Codec serializes workflow and activity payloads.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}
