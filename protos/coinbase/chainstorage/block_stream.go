package chainstorage

// Generic chain-agnostic wire walker for a proto-encoded api.Block.
//
// Motivation: the Download path today does `ioutil.ReadAll(dec)` +
// `proto.Unmarshal(...)`, which holds the full decompressed block AND
// the fully-populated api.Block in memory simultaneously. Peak RAM is
// ~2× block size.
//
// WalkBlockEnvelope uses a streaming bufio.Reader and protoreflect to
// walk the wire format recursively, allocating destination fields
// directly (no intermediate []byte of the whole block). Peak RAM
// drops to ~1× block size.
//
// This variant is bitcoin-agnostic and populates the returned *Block
// fully (including BitcoinBlobdata.Header as a []byte, not as an
// offset). For the bitcoin streaming path where Header is exposed as
// an io.Reader instead of materialized, use WalkBitcoinEnvelope.

import (
	"bufio"
	"io"
	"math"

	"golang.org/x/xerrors"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// WalkBlockEnvelope reads a proto-encoded Block from r and populates
// the full *Block without materializing a complete []byte copy of the
// decompressed proto bytes.
//
// Compared to ioutil.ReadAll + proto.Unmarshal, peak RAM during this
// call drops by roughly one block size. The effect is chain-agnostic:
// ethereum / solana / aptos / rosetta / ethereum_beacon all benefit
// proportionally to their blobdata size.
//
// Unknown fields on the wire return an error rather than being
// silently skipped. The proto descriptor is the source of truth: new
// fields are accepted automatically as long as their kind is a
// standard proto scalar, enum, message, repeated, or map.
func WalkBlockEnvelope(r io.Reader) (*Block, error) {
	block := &Block{}
	w := &walker{br: bufio.NewReaderSize(r, 64*1024)}
	if err := w.walkMessage(block.ProtoReflect(), -1); err != nil {
		return nil, xerrors.Errorf("walk Block: %w", err)
	}
	return block, nil
}

type walker struct {
	br  *bufio.Reader
	pos int64

	// scratch is a reusable buffer for discardFast. Sized so that
	// skipping large fields doesn't trigger io.Copy's default 32 KB
	// per-call allocation or a fresh LimitedReader wrapper. The
	// bufio.Reader's internal buffer handles the actual read
	// amplification to the underlying stream.
	scratch [64 * 1024]byte
}

// discardFast reads and discards n bytes from the bufio reader without
// the io.CopyN allocation (io.CopyN wraps with io.LimitReader and
// allocates a fresh buffer per call; over thousands of calls this
// dominates allocator pressure). Uses the walker's pre-allocated
// scratch buffer instead.
func (w *walker) discardFast(n int64) error {
	for n > 0 {
		chunk := int64(len(w.scratch))
		if chunk > n {
			chunk = n
		}
		got, err := io.ReadFull(w.br, w.scratch[:chunk])
		w.pos += int64(got)
		n -= int64(got)
		if err != nil {
			return err
		}
	}
	return nil
}

// walkMessage reads wire-format fields into dst until endPos (absolute
// byte offset in the underlying stream). endPos == -1 means read until
// EOF, used for the top-level Block.
func (w *walker) walkMessage(dst protoreflect.Message, endPos int64) error {
	desc := dst.Descriptor()

	for {
		if endPos >= 0 && w.pos >= endPos {
			break
		}
		if endPos < 0 {
			if _, err := w.br.Peek(1); err != nil {
				if err == io.EOF {
					break
				}
				return xerrors.Errorf("peek: %w", err)
			}
		}

		tag, err := w.readVarint()
		if err != nil {
			return xerrors.Errorf("read tag on %s: %w", desc.FullName(), err)
		}
		fieldNum := protoreflect.FieldNumber(tag >> 3)
		wireType := int(tag & 0x7)

		fd := desc.Fields().ByNumber(fieldNum)
		if fd == nil {
			return xerrors.Errorf("unknown field %d (wt=%d) on %s: proto changed; update walker or the field's proto definition", fieldNum, wireType, desc.FullName())
		}

		if err := w.decodeField(dst, fd, wireType); err != nil {
			return xerrors.Errorf("decode %s.%s: %w", desc.FullName(), fd.Name(), err)
		}
	}

	if endPos >= 0 && w.pos != endPos {
		return xerrors.Errorf("sub-message %s consumed %d bytes, expected %d", desc.FullName(), w.pos, endPos)
	}
	return nil
}

func (w *walker) decodeField(dst protoreflect.Message, fd protoreflect.FieldDescriptor, wireType int) error {
	switch {
	case fd.IsMap():
		return w.decodeMapEntry(dst, fd)
	case fd.IsList():
		return w.decodeListElement(dst, fd, wireType)
	default:
		return w.decodeSingular(dst, fd, wireType)
	}
}

func (w *walker) decodeSingular(dst protoreflect.Message, fd protoreflect.FieldDescriptor, wireType int) error {
	switch fd.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		length, err := w.readVarint()
		if err != nil {
			return err
		}
		end := w.pos + int64(length)
		inner := dst.Mutable(fd).Message()
		return w.walkMessage(inner, end)
	case protoreflect.BytesKind:
		b, err := w.readBytes()
		if err != nil {
			return err
		}
		dst.Set(fd, protoreflect.ValueOfBytes(b))
		return nil
	case protoreflect.StringKind:
		b, err := w.readBytes()
		if err != nil {
			return err
		}
		dst.Set(fd, protoreflect.ValueOfString(string(b)))
		return nil
	default:
		v, err := w.readScalarValue(fd.Kind(), wireType)
		if err != nil {
			return err
		}
		dst.Set(fd, v)
		return nil
	}
}

func (w *walker) decodeListElement(dst protoreflect.Message, fd protoreflect.FieldDescriptor, wireType int) error {
	list := dst.Mutable(fd).List()
	switch fd.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		length, err := w.readVarint()
		if err != nil {
			return err
		}
		end := w.pos + int64(length)
		newElem := list.NewElement()
		if err := w.walkMessage(newElem.Message(), end); err != nil {
			return err
		}
		list.Append(newElem)
		return nil
	case protoreflect.BytesKind:
		b, err := w.readBytes()
		if err != nil {
			return err
		}
		list.Append(protoreflect.ValueOfBytes(b))
		return nil
	case protoreflect.StringKind:
		b, err := w.readBytes()
		if err != nil {
			return err
		}
		list.Append(protoreflect.ValueOfString(string(b)))
		return nil
	default:
		// Packed repeated scalars come as a single length-delimited
		// run; non-packed is one tag per value. Both are supported.
		if wireType == wireBytes {
			length, err := w.readVarint()
			if err != nil {
				return err
			}
			end := w.pos + int64(length)
			for w.pos < end {
				v, err := w.readScalarValue(fd.Kind(), wireTypeFor(fd.Kind()))
				if err != nil {
					return err
				}
				list.Append(v)
			}
			return nil
		}
		v, err := w.readScalarValue(fd.Kind(), wireType)
		if err != nil {
			return err
		}
		list.Append(v)
		return nil
	}
}

// decodeMapEntry reads one length-delimited map entry message and sets
// it on the map. Map entries follow the synthetic "MapEntry" message
// pattern: field 1 = key, field 2 = value.
func (w *walker) decodeMapEntry(dst protoreflect.Message, fd protoreflect.FieldDescriptor) error {
	length, err := w.readVarint()
	if err != nil {
		return err
	}
	end := w.pos + int64(length)
	mp := dst.Mutable(fd).Map()

	keyDesc := fd.MapKey()
	valDesc := fd.MapValue()

	var keyVal protoreflect.Value
	valVal := mp.NewValue() // empty value of the right shape (for message types)

	for w.pos < end {
		tag, err := w.readVarint()
		if err != nil {
			return err
		}
		inner := protoreflect.FieldNumber(tag >> 3)
		innerWT := int(tag & 0x7)
		switch inner {
		case 1: // key
			switch keyDesc.Kind() {
			case protoreflect.StringKind:
				b, err := w.readBytes()
				if err != nil {
					return err
				}
				keyVal = protoreflect.ValueOfString(string(b))
			case protoreflect.BytesKind:
				b, err := w.readBytes()
				if err != nil {
					return err
				}
				keyVal = protoreflect.ValueOfBytes(b)
			default:
				v, err := w.readScalarValue(keyDesc.Kind(), innerWT)
				if err != nil {
					return err
				}
				keyVal = v
			}
		case 2: // value
			switch valDesc.Kind() {
			case protoreflect.MessageKind, protoreflect.GroupKind:
				mlen, err := w.readVarint()
				if err != nil {
					return err
				}
				mend := w.pos + int64(mlen)
				if err := w.walkMessage(valVal.Message(), mend); err != nil {
					return err
				}
			case protoreflect.BytesKind:
				b, err := w.readBytes()
				if err != nil {
					return err
				}
				valVal = protoreflect.ValueOfBytes(b)
			case protoreflect.StringKind:
				b, err := w.readBytes()
				if err != nil {
					return err
				}
				valVal = protoreflect.ValueOfString(string(b))
			default:
				v, err := w.readScalarValue(valDesc.Kind(), innerWT)
				if err != nil {
					return err
				}
				valVal = v
			}
		default:
			if err := w.skipValue(innerWT); err != nil {
				return err
			}
		}
	}

	if !keyVal.IsValid() {
		return xerrors.Errorf("map entry on %s has no key", dst.Descriptor().FullName())
	}
	mp.Set(keyVal.MapKey(), valVal)
	return nil
}

// readBytes reads a length-delimited byte slice. Allocates a single
// []byte of exactly the right size and reads into it — no source-side
// intermediate buffer is retained.
func (w *walker) readBytes() ([]byte, error) {
	length, err := w.readVarint()
	if err != nil {
		return nil, err
	}
	if length > (1 << 30) {
		return nil, xerrors.Errorf("length-delimited payload too large: %d", length)
	}
	b := make([]byte, length)
	n, err := io.ReadFull(w.br, b)
	w.pos += int64(n)
	if err != nil {
		return nil, xerrors.Errorf("readBytes: %w", err)
	}
	return b, nil
}

func (w *walker) readScalarValue(kind protoreflect.Kind, wireType int) (protoreflect.Value, error) {
	switch kind {
	case protoreflect.BoolKind:
		v, err := w.readVarint()
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfBool(v != 0), nil
	case protoreflect.EnumKind:
		v, err := w.readVarint()
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(int32(v))), nil
	case protoreflect.Int32Kind:
		v, err := w.readVarint()
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt32(int32(v)), nil
	case protoreflect.Sint32Kind:
		v, err := w.readVarint()
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt32(int32(decodeZigZag32(uint32(v)))), nil
	case protoreflect.Uint32Kind:
		v, err := w.readVarint()
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint32(uint32(v)), nil
	case protoreflect.Int64Kind:
		v, err := w.readVarint()
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt64(int64(v)), nil
	case protoreflect.Sint64Kind:
		v, err := w.readVarint()
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt64(decodeZigZag64(v)), nil
	case protoreflect.Uint64Kind:
		v, err := w.readVarint()
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint64(v), nil
	case protoreflect.Sfixed32Kind, protoreflect.Fixed32Kind, protoreflect.FloatKind:
		v, err := w.readFixed32()
		if err != nil {
			return protoreflect.Value{}, err
		}
		switch kind {
		case protoreflect.Sfixed32Kind:
			return protoreflect.ValueOfInt32(int32(v)), nil
		case protoreflect.Fixed32Kind:
			return protoreflect.ValueOfUint32(v), nil
		default: // FloatKind
			return protoreflect.ValueOfFloat32(float32FromBits(v)), nil
		}
	case protoreflect.Sfixed64Kind, protoreflect.Fixed64Kind, protoreflect.DoubleKind:
		v, err := w.readFixed64()
		if err != nil {
			return protoreflect.Value{}, err
		}
		switch kind {
		case protoreflect.Sfixed64Kind:
			return protoreflect.ValueOfInt64(int64(v)), nil
		case protoreflect.Fixed64Kind:
			return protoreflect.ValueOfUint64(v), nil
		default: // DoubleKind
			return protoreflect.ValueOfFloat64(float64FromBits(v)), nil
		}
	}
	_ = wireType
	return protoreflect.Value{}, xerrors.Errorf("unsupported scalar kind %s", kind)
}

func (w *walker) readFixed32() (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(w.br, buf[:]); err != nil {
		return 0, err
	}
	w.pos += 4
	return uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24, nil
}

func (w *walker) readFixed64() (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(w.br, buf[:]); err != nil {
		return 0, err
	}
	w.pos += 8
	return uint64(buf[0]) | uint64(buf[1])<<8 | uint64(buf[2])<<16 | uint64(buf[3])<<24 |
		uint64(buf[4])<<32 | uint64(buf[5])<<40 | uint64(buf[6])<<48 | uint64(buf[7])<<56, nil
}

func (w *walker) readVarint() (uint64, error) {
	var v uint64
	var shift uint
	for i := 0; i < 10; i++ {
		b, err := w.br.ReadByte()
		if err != nil {
			if err == io.EOF && i > 0 {
				return 0, io.ErrUnexpectedEOF
			}
			return 0, err
		}
		w.pos++
		v |= uint64(b&0x7f) << shift
		if b < 0x80 {
			return v, nil
		}
		shift += 7
	}
	return 0, xerrors.New("varint too long")
}

func (w *walker) skipValue(wireType int) error {
	switch wireType {
	case wireVarint:
		_, err := w.readVarint()
		return err
	case wireFixed64:
		n, err := io.CopyN(io.Discard, w.br, 8)
		w.pos += n
		return err
	case wireBytes:
		length, err := w.readVarint()
		if err != nil {
			return err
		}
		n, err := io.CopyN(io.Discard, w.br, int64(length))
		w.pos += n
		return err
	case wireFixed32:
		n, err := io.CopyN(io.Discard, w.br, 4)
		w.pos += n
		return err
	}
	return xerrors.Errorf("cannot skip wire type %d", wireType)
}

// wireTypeFor returns the wire type used for the given kind in a
// non-packed repeated encoding, used when decoding packed repeated
// values inside a length-delimited run.
func wireTypeFor(k protoreflect.Kind) int {
	switch k {
	case protoreflect.BoolKind, protoreflect.EnumKind,
		protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Uint32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Uint64Kind:
		return wireVarint
	case protoreflect.Sfixed32Kind, protoreflect.Fixed32Kind, protoreflect.FloatKind:
		return wireFixed32
	case protoreflect.Sfixed64Kind, protoreflect.Fixed64Kind, protoreflect.DoubleKind:
		return wireFixed64
	}
	return wireBytes
}

func decodeZigZag32(v uint32) int32 {
	return int32((v >> 1) ^ -(v & 1))
}

func decodeZigZag64(v uint64) int64 {
	return int64((v >> 1) ^ -(v & 1))
}

func float32FromBits(b uint32) float32 { return math.Float32frombits(b) }
func float64FromBits(b uint64) float64 { return math.Float64frombits(b) }
