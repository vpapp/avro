package avro

import (
	"sync"

	"github.com/modern-go/reflect2"
)

type encoderTypeConversion struct {
	from uintptr
	to   Type
}

// SpecificType combines the main data type with a logical type to create a specific type.
func SpecificType(typ Type, lt LogicalType) Type {
	return typ + Type(".") + Type(lt)
}

// ConversionFn is the function signature of the user-provided type conversion function.
type ConversionFn func(in any) (any, error)

// EncoderTypeConverter holds the user-provided type conversion functions for encoding.
type EncoderTypeConverter struct {
	fns sync.Map // map[encoderTypeConversion]ConverterFn
}

// NewEncoderTypeConverter creates a new type converter for encoding.
func NewEncoderTypeConverter() *EncoderTypeConverter {
	return &EncoderTypeConverter{}
}

// RegisterEncoderTypeConversion registers type conversion functions for encoding to the specified data type.
func (r *EncoderTypeConverter) RegisterEncoderTypeConversion(obj any, typ Type, fn ConversionFn) {
	rtype := reflect2.TypeOf(obj).RType()
	r.fns.Store(encoderTypeConversion{from: rtype, to: typ}, fn)
}

// Convert runs the conversion function for the given value and schema.
func (r *EncoderTypeConverter) Convert(in any, schema Schema) (any, error) {
	typ := schema.Type()
	if lt := getLogicalType(schema); len(lt) > 0 {
		typ = SpecificType(typ, lt)
	}
	fn, ok := r.fns.Load(encoderTypeConversion{from: reflect2.RTypeOf(in), to: typ})
	if !ok {
		return in, nil
	}

	return fn.(ConversionFn)(in)
}

// RegisterEncoderTypeConversion registers type conversion functions for encoding to the specified data type.
func RegisterEncoderTypeConversion(obj any, typ Type, fn ConversionFn) {
	DefaultConfig.RegisterEncoderTypeConversion(obj, typ, fn)
}

type decoderConversion struct {
	from Type
}

// DecoderTypeConverter holds the user-provided type conversion functions for decoding.
type DecoderTypeConverter struct {
	fns sync.Map // map[decoderConversion]ConverterFn
}

// NewDecoderTypeConverter creates a new type converter for decoding.
func NewDecoderTypeConverter() *DecoderTypeConverter {
	return &DecoderTypeConverter{}
}

// RegisterDecoderTypeConversion registers type conversion functions for decoding from the specified data type.
func (r *DecoderTypeConverter) RegisterDecoderTypeConversion(typ Type, fn ConversionFn) {
	r.fns.Store(decoderConversion{from: typ}, fn)
}

// Convert runs the conversion function for the given schema and value.
func (r *DecoderTypeConverter) Convert(schema Schema, in any) error {
	pObj, ok := in.(*any)
	if !ok {
		return nil
	}

	typ := schema.Type()
	if lt := getLogicalType(schema); len(lt) > 0 {
		typ = SpecificType(typ, lt)
	}
	fn, ok := r.fns.Load(decoderConversion{from: typ})
	if !ok {
		return nil
	}

	val, err := fn.(ConversionFn)(*pObj)
	if err != nil {
		*pObj = nil
		return err
	}

	*pObj = val

	return nil
}

// RegisterDecoderTypeConversion registers type conversion functions for decoding from the specified data type.
func RegisterDecoderTypeConversion(typ Type, fn ConversionFn) {
	DefaultConfig.RegisterDecoderTypeConversion(typ, fn)
}
