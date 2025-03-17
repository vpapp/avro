package avro_test

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoderTypeConversion_Single(t *testing.T) {
	defer ConfigTeardown()

	schema := `"int"`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterEncoderTypeConversion(float32(0), avro.Int, func(in any) (any, error) {
		return int(in.(float32)), nil
	})

	val := any(float32(27))

	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36}, buf.Bytes())
}

func TestEncoderTypeConversion_Array(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items":"int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterEncoderTypeConversion(float32(0), avro.Int, func(in any) (any, error) {
		return int(in.(float32)), nil
	})
	avro.RegisterEncoderTypeConversion(float64(0), avro.Int, func(in any) (any, error) {
		return int(in.(float64)), nil
	})

	val := []any{
		float32(27),
		float64(28),
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x03, 0x04, 0x36, 0x38, 0x0}, buf.Bytes())
}

func TestEncoderTypeConversion_RecordMap(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type":"int"},{"name":"b", "type": {"type":"fixed", "name":"fixed", "size":6, "logicalType":"decimal", "precision":4, "scale":2}}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterEncoderTypeConversion(float64(0), avro.Int, func(in any) (any, error) {
		return int(in.(float64)), nil
	})
	avro.RegisterEncoderTypeConversion("", avro.SpecificType(avro.Fixed, avro.Decimal), func(in any) (any, error) {
		val, _ := new(big.Rat).SetString(in.(string))
		return val, nil
	})

	val := map[string]any{
		"a": float64(27),
		"b": "346.8",
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x00, 0x00, 0x00, 0x00, 0x87, 0x78}, buf.Bytes())
}

func TestEncoderTypeConversion_RecordStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type":"int"},{"name":"b", "type": {"type":"fixed", "name":"fixed", "size":6, "logicalType":"decimal", "precision":4, "scale":2}}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterEncoderTypeConversion(float64(0), avro.Int, func(in any) (any, error) {
		return int(in.(float64)), nil
	})
	avro.RegisterEncoderTypeConversion("", avro.SpecificType(avro.Fixed, avro.Decimal), func(in any) (any, error) {
		val, _ := new(big.Rat).SetString(in.(string))
		return val, nil
	})

	type TestRecord struct {
		A any `avro:"a"`
		B any `avro:"b"`
	}

	val := TestRecord{
		A: float64(27),
		B: "346.8",
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x00, 0x00, 0x00, 0x00, 0x87, 0x78}, buf.Bytes())
}

func TestEncoderTypeConversion_UnionInterfaceUnregisteredArray(t *testing.T) {
	defer ConfigTeardown()

	schema := `["int", {"type":"array", "items":"int"}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterEncoderTypeConversion(float32(0), avro.Int, func(in any) (any, error) {
		return int(in.(float32)), nil
	})
	avro.RegisterEncoderTypeConversion(float64(0), avro.Int, func(in any) (any, error) {
		return int(in.(float64)), nil
	})

	var val any = map[string]any{
		"array": []any{
			float32(27),
			float64(28),
		},
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x03, 0x04, 0x36, 0x38, 0x00}, buf.Bytes())
}

func TestEncoderTypeConversion_Error(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items":"int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterEncoderTypeConversion(float32(0), avro.Int, func(in any) (any, error) {
		f := in.(float32)
		if float32(int(f)) != f {
			return 0, fmt.Errorf("%v is not an integer", in)
		}
		return int(in.(float32)), nil
	})

	val := []any{
		float32(27.1),
	}
	err = enc.Encode(val)

	assert.ErrorContains(t, err, "27.1 is not an integer")
	assert.Empty(t, buf.Bytes())
}

func TestDecoderTypeConversion_Single(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := `"boolean"`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	avro.RegisterDecoderTypeConversion(avro.Boolean, func(in any) (any, error) {
		b := in.(bool)
		if b {
			return "yes", nil
		} else {
			return "no", nil
		}
	})

	var b any
	err = dec.Decode(&b)

	require.NoError(t, err)
	assert.Equal(t, "yes", b)
}

func TestDecoderTypeConversion_FixedRat(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x00, 0x00, 0x00, 0x87, 0x78}
	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":4,"scale":2}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	avro.RegisterDecoderTypeConversion(avro.SpecificType(avro.Fixed, avro.Decimal), func(in any) (any, error) {
		r := in.(*big.Rat)
		f, _ := r.Float64()

		return f, nil
	})

	var got any
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, float64(346.8), got)
}

func TestDecoderTypeConversion_Error(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x80, 0x80, 0x80, 0x80, 0x10}
	schema := `"long"`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	avro.RegisterDecoderTypeConversion(avro.Long, func(in any) (any, error) {
		i := in.(int64)
		if i > math.MaxInt32 {
			return int32(0), fmt.Errorf("%v is out range for int32", in)
		}
		return int32(i), nil
	})

	var got any
	err = dec.Decode(&got)

	assert.ErrorContains(t, err, "2147483648 is out range for int32")
	assert.Nil(t, got)
}
