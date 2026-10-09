package chainstorage_test

// Contract + switch-coverage tests for the bitcoin wire walker.
//
// There are three distinct drift hazards the walker must defend against.
// Each has a dedicated test:
//
//  1. A new proto field that the walker doesn't know about.
//     Caught by TestWireWalkerContract_*. The contract test iterates
//     the proto descriptor and asserts every field has an entry in
//     the walker's Known* map, and vice versa.
//
//  2. A walker Known* map entry that doesn't match the proto shape
//     (kind / cardinality / oneof).
//     Also caught by TestWireWalkerContract_*.
//
//  3. A field registered in Known* but missing from the walker's
//     switch statement.
//     Caught by TestWireWalkerSwitchCoverage_*. These tests build
//     a proto containing only one registered field (per field), walk
//     it, and assert the walker did not return an "unknown field"
//     error. If the switch case is missing, the walk fails.
//
// Together these defenses make it impossible to merge a proto edit
// that the walker doesn't fully handle — the drift surfaces at CI
// with an actionable message pointing at blockchain_bitcoin.stream.go.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// TestWireWalkerContract_Block cross-references api.KnownBlockFields
// against the Block proto descriptor.
func TestWireWalkerContract_Block(t *testing.T) {
	assertContractMatches(t,
		(&api.Block{}).ProtoReflect().Descriptor(),
		api.KnownBlockFields,
	)
}

// TestWireWalkerContract_BitcoinBlobdata cross-references
// api.KnownBitcoinBlobFields against the BitcoinBlobdata descriptor.
func TestWireWalkerContract_BitcoinBlobdata(t *testing.T) {
	assertContractMatches(t,
		(&api.BitcoinBlobdata{}).ProtoReflect().Descriptor(),
		api.KnownBitcoinBlobFields,
	)
}

// TestWireWalkerContract_SolanaBlobdata cross-references
// api.KnownSolanaBlobFields against the SolanaBlobdata descriptor.
func TestWireWalkerContract_SolanaBlobdata(t *testing.T) {
	assertContractMatches(t,
		(&api.SolanaBlobdata{}).ProtoReflect().Descriptor(),
		api.KnownSolanaBlobFields,
	)
}

func assertContractMatches(
	t *testing.T,
	d protoreflect.MessageDescriptor,
	known map[protoreflect.FieldNumber]api.WireWalkerKnownField,
) {
	t.Helper()
	fields := d.Fields()

	// --- Forward: every proto field must be registered in the walker ---
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		entry, ok := known[f.Number()]
		require.Truef(t, ok,
			"%s.%s (number %d, kind %s): proto has a field the wire walker does not know about.\n"+
				"Add it to the Known* map in blockchain_bitcoin.stream.go AND either handle it in "+
				"WalkBitcoinEnvelope (Process: true) or add its number to the known-but-unused "+
				"skip group (Process: false). Without an update the walker will return "+
				"\"unknown field\" errors on any block that contains this field.",
			d.FullName(), f.Name(), f.Number(), f.Kind())

		require.Equalf(t, entry.Kind, f.Kind(),
			"%s.%s: kind drift (walker expects %s, proto has %s); verify the walker's decode branch still applies",
			d.FullName(), f.Name(), entry.Kind, f.Kind())

		isRepeated := f.IsList() || f.IsMap()
		require.Equalf(t, entry.Repeated, isRepeated,
			"%s.%s: cardinality drift (walker expects repeated=%v, proto has list=%v map=%v)",
			d.FullName(), f.Name(), entry.Repeated, f.IsList(), f.IsMap())

		require.Equalf(t, entry.Map, f.IsMap(),
			"%s.%s: map drift (walker expects map=%v, proto has map=%v)",
			d.FullName(), f.Name(), entry.Map, f.IsMap())

		if entry.Oneof != "" {
			oneof := f.ContainingOneof()
			require.NotNilf(t, oneof,
				"%s.%s: walker expects oneof %q, but field is not in any oneof",
				d.FullName(), f.Name(), entry.Oneof)
			require.Equalf(t, entry.Oneof, string(oneof.Name()),
				"%s.%s: oneof drift (walker expects %q, proto has %q)",
				d.FullName(), f.Name(), entry.Oneof, oneof.Name())
		} else if oneof := f.ContainingOneof(); oneof != nil {
			t.Fatalf("%s.%s: walker does not register an oneof, but proto places this field in oneof %q",
				d.FullName(), f.Name(), oneof.Name())
		}
	}

	// --- Reverse: every walker entry must map to a real proto field ---
	for num := range known {
		f := fields.ByNumber(num)
		require.NotNilf(t, f,
			"%s: walker has a stale Known* entry for field number %d that no longer exists in the proto — remove it from KnownBlockFields / KnownBitcoinBlobFields and the corresponding walker switch case",
			d.FullName(), num)
	}
}

// TestWireWalkerSwitchCoverage_Block exercises every field registered
// in api.KnownBlockFields by constructing a Block with ONLY that field
// set, serializing it, and walking it. If the walker's switch is
// missing a case for the field, the walk returns an "unknown Block
// field" error and the test fails with an actionable message.
func TestWireWalkerSwitchCoverage_Block(t *testing.T) {
	desc := (&api.Block{}).ProtoReflect().Descriptor()

	for num := range api.KnownBlockFields {
		fd := desc.Fields().ByNumber(num)
		require.NotNilf(t, fd, "Known* map references field %d that is missing from the proto descriptor (contract test should have caught this already)", num)

		t.Run(string(fd.Name()), func(t *testing.T) {
			m := (&api.Block{}).ProtoReflect()
			populateFieldNonZero(m, fd)

			raw, err := proto.Marshal(m.Interface())
			require.NoError(t, err)

			_, _, err = api.WalkBitcoinEnvelope(bytes.NewReader(raw))
			require.NoErrorf(t, err,
				"walker returned an error on a Block containing only field %q (number %d).\n"+
					"If the error mentions \"unknown Block field\", the switch in WalkBitcoinEnvelope is missing a case for this field — either handle it (Process) or add its number to the known-but-unused skip group in blockchain_bitcoin.stream.go.",
				fd.Name(), num)
		})
	}
}

// TestWireWalkerSwitchCoverage_BitcoinBlobdata does the same for
// BitcoinBlobdata. Because BitcoinBlobdata is nested in Block, each
// probe is wrapped in a Block with the Bitcoin variant of the oneof
// set.
func TestWireWalkerSwitchCoverage_BitcoinBlobdata(t *testing.T) {
	desc := (&api.BitcoinBlobdata{}).ProtoReflect().Descriptor()

	for num := range api.KnownBitcoinBlobFields {
		fd := desc.Fields().ByNumber(num)
		require.NotNilf(t, fd, "Known* map references BitcoinBlobdata field %d that is missing from the proto descriptor", num)

		t.Run(string(fd.Name()), func(t *testing.T) {
			blob := (&api.BitcoinBlobdata{}).ProtoReflect()
			populateFieldNonZero(blob, fd)

			block := &api.Block{
				Blobdata: &api.Block_Bitcoin{
					Bitcoin: blob.Interface().(*api.BitcoinBlobdata),
				},
			}
			raw, err := proto.Marshal(block)
			require.NoError(t, err)

			_, _, err = api.WalkBitcoinEnvelope(bytes.NewReader(raw))
			require.NoErrorf(t, err,
				"walker returned an error on a Block containing only BitcoinBlobdata.%s (number %d).\n"+
					"If the error mentions \"unknown BitcoinBlobdata field\", the switch in walkBitcoinBlob is missing a case for this field — either handle it (Process) or add its number to the known-but-unused skip group in blockchain_bitcoin.stream.go.",
				fd.Name(), num)
		})
	}
}

// populateFieldNonZero sets a single field on m to a non-zero value
// so that proto3 serializes it. For oneof fields, setting one variant
// auto-unsets siblings — which is what we want for per-field probing.
func populateFieldNonZero(m protoreflect.Message, fd protoreflect.FieldDescriptor) {
	switch {
	case fd.IsMap():
		mp := m.Mutable(fd).Map()
		key := mapKeyNonZero(fd.MapKey())
		var val protoreflect.Value
		if fd.MapValue().Kind() == protoreflect.MessageKind {
			val = mp.NewValue()
		} else {
			val = scalarNonZero(fd.MapValue().Kind())
		}
		mp.Set(key, val)
	case fd.IsList():
		list := m.Mutable(fd).List()
		if fd.Kind() == protoreflect.MessageKind {
			list.Append(list.NewElement())
		} else {
			list.Append(scalarNonZero(fd.Kind()))
		}
	case fd.Kind() == protoreflect.MessageKind:
		// Mutable creates an empty submessage and marks the field as
		// set. An empty sub-message serializes in proto3 as a zero-
		// length length-delimited entry, which is enough for the
		// walker to see the field on the wire.
		m.Mutable(fd)
	default:
		m.Set(fd, scalarNonZero(fd.Kind()))
	}
}

// scalarNonZero returns a Value that survives proto3 serialization
// for the given scalar kind (i.e. not the default zero value).
func scalarNonZero(k protoreflect.Kind) protoreflect.Value {
	switch k {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true)
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(1)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(1)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(1)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(1)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(1)
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(1)
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(1)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("x")
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte{0x01})
	}
	panic("unsupported scalar kind: " + k.String())
}

func mapKeyNonZero(fd protoreflect.FieldDescriptor) protoreflect.MapKey {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("k").MapKey()
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(1).MapKey()
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(1).MapKey()
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(1).MapKey()
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(1).MapKey()
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true).MapKey()
	}
	panic("unsupported map key kind: " + fd.Kind().String())
}
