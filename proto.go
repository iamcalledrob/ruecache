package ruecache

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"reflect"
)

// ProtoMapCoder automatically encodes and decodes a map of [ID]proto.Message models to the cache, and also
// serves as a reference for implementing a CacheCoder.
type ProtoMapCoder[T proto.Message] struct{}

func (c ProtoMapCoder[T]) Encode(decoded map[string]T) (encoded map[string][]byte, err error) {
	encoded = make(map[string][]byte)
	for id, result := range decoded {
		var b []byte

		b, err = proto.Marshal(result)
		if err != nil {
			err = fmt.Errorf("marshaling cache key %s: %w", id, err)
			return
		}
		encoded[id] = b
	}
	return
}

func (c ProtoMapCoder[T]) Decode(encoded map[string][]byte) (decoded map[string]T, err error) {
	decoded = make(map[string]T)
	for id, result := range encoded {
		value := alloc[T]()
		err = proto.Unmarshal(result, value)
		if err != nil {
			err = fmt.Errorf("unmarshaling cache key %s: %w", id, err)
			return
		}
		decoded[id] = value
	}
	return
}

func alloc[T any]() T {
	var v T
	t := reflect.TypeOf(v)

	// Initialize a pointer to the zero value of the type T points to.
	// e.g. initialized[*Widget] = &Widget{}, NOT (*Widget)(nil)
	if t != nil && t.Kind() == reflect.Pointer {
		return reflect.New(t.Elem()).Interface().(T)
	}

	return v
}

var _ CacheCoder[map[string]proto.Message] = ProtoMapCoder[proto.Message]{}
