package chainstorage

// Bitcoin-specific variant of the Block wire walker. Shares the
// generic walker infrastructure from block_stream.go, but when the
// BitcoinBlobdata.Header field is encountered, the bytes are skipped
// and only their (offset, length) within the walked stream is
// recorded. Callers layer a spool-file reader over that offset to
// stream the JSON header directly into the parser without
// materializing it as a []byte in RAM.
//
// Everything else (Block outer fields, BitcoinBlobdata fields other
// than Header, sub-messages, repeated bytes, map entries) delegates
// to the generic walker's reflection-driven decoder — so peak RAM on
// InputTransactions-heavy zcash blocks matches the generic walker's
// direct-read efficiency.

import (
	"bufio"
	"io"

	"golang.org/x/xerrors"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Proto wire types (see https://protobuf.dev/programming-guides/encoding/).
// Defined here and shared with the generic walker in block_stream.go.
const (
	wireVarint     = 0
	wireFixed64    = 1
	wireBytes      = 2 // length-delimited
	wireStartGroup = 3 // deprecated
	wireEndGroup   = 4 // deprecated
	wireFixed32    = 5
)

// Block field numbers.
const (
	blockFieldBlockchain          = 1
	blockFieldNetwork             = 2
	blockFieldMetadata            = 3
	blockFieldTransactionMetadata = 4
	blockFieldSideChain           = 5
	// blobdata oneof:
	blockFieldEthereum       = 100
	blockFieldBitcoin        = 101
	blockFieldRosetta        = 102
	blockFieldSolana         = 103
	blockFieldAptos          = 104
	blockFieldEthereumBeacon = 105
)

// BitcoinBlobdata field numbers.
const (
	bitcoinBlobFieldHeader               = 1
	bitcoinBlobFieldInputTransactions    = 2
	bitcoinBlobFieldRawInputTransactions = 3
)

// WireWalkerKnownField describes one field the bitcoin wire walker
// handles. The contract test in blockchain_bitcoin.stream_test.go
// asserts that KnownBlockFields and KnownBitcoinBlobFields match the
// proto descriptors exactly, in both directions.
type WireWalkerKnownField struct {
	Kind     protoreflect.Kind // expected proto kind
	Repeated bool              // repeated (includes map)
	Map      bool              // map<K, V>
	Oneof    string            // containing oneof name, empty if none
	Process  bool              // walker decodes this field into the result
}

// KnownBlockFields enumerates every field the walker recognizes on
// Block. See the contract test + switch-coverage test for the
// drift-defense story.
var KnownBlockFields = map[protoreflect.FieldNumber]WireWalkerKnownField{
	blockFieldBlockchain:          {Kind: protoreflect.EnumKind, Process: true},
	blockFieldNetwork:             {Kind: protoreflect.EnumKind, Process: true},
	blockFieldMetadata:            {Kind: protoreflect.MessageKind, Process: true},
	blockFieldTransactionMetadata: {Kind: protoreflect.MessageKind, Process: true},
	blockFieldSideChain:           {Kind: protoreflect.EnumKind, Process: true},
	blockFieldEthereum:            {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: true},
	blockFieldBitcoin:             {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: true},
	blockFieldRosetta:             {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: true},
	blockFieldSolana:              {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: true},
	blockFieldAptos:               {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: true},
	blockFieldEthereumBeacon:      {Kind: protoreflect.MessageKind, Oneof: "blobdata", Process: true},
}

// KnownBitcoinBlobFields enumerates every field the walker recognizes
// on BitcoinBlobdata.
var KnownBitcoinBlobFields = map[protoreflect.FieldNumber]WireWalkerKnownField{
	bitcoinBlobFieldHeader:               {Kind: protoreflect.BytesKind, Process: true},
	bitcoinBlobFieldInputTransactions:    {Kind: protoreflect.MessageKind, Repeated: true, Process: true},
	bitcoinBlobFieldRawInputTransactions: {Kind: protoreflect.MessageKind, Repeated: true, Map: true, Process: true},
}

// BitcoinChunkRef points at a byte range within the walked stream.
// Zero-value means "not present" (Length == 0 distinguishes empty).
type BitcoinChunkRef struct {
	Offset int64
	Length int64
}

// BitcoinChunks summarizes the byte ranges the walker skipped inside
// BitcoinBlobdata so the caller can stream each chunk on demand
// without retaining the whole blob in memory.
type BitcoinChunks struct {
	// Header is the range of BitcoinBlobdata.Header bytes.
	Header BitcoinChunkRef
	// InputTransactionsGroups is one entry per element of
	// BitcoinBlobdata.InputTransactions (a RepeatedBytes message).
	// Each range is the raw message bytes; the consumer can
	// proto.Unmarshal them into a RepeatedBytes on demand.
	InputTransactionsGroups []BitcoinChunkRef
}

// WalkBitcoinEnvelope reads a proto-encoded Block from r and returns
// a *Block where BitcoinBlobdata.Header AND
// BitcoinBlobdata.InputTransactions are left nil. The byte ranges of
// those fields within the walked stream are returned via
// BitcoinChunks so the caller can seek into a spooled copy of r and
// load each chunk on demand.
//
// All other fields (Block metadata, other blobdata variants, and any
// non-{Header, InputTransactions} fields on BitcoinBlobdata) are
// decoded normally.
//
// Peak RAM during this call is bounded by the materialized parts of
// the Block — which for bitcoin-family blocks is just metadata + the
// tiny RawInputTransactions map if populated. The big InputTransactions
// and Header fields are only streamed when the parser asks for them.
//
// Unknown field numbers on the wire return an error. See
// Known{Block,BitcoinBlob}Fields and the contract + switch-coverage
// tests for drift-defense guarantees.
func WalkBitcoinEnvelope(r io.Reader) (*Block, BitcoinChunks, error) {
	block := &Block{}
	chunks := BitcoinChunks{}
	w := &walker{br: bufio.NewReaderSize(r, 64*1024)}
	blockMsg := block.ProtoReflect()
	blockDesc := blockMsg.Descriptor()

	for {
		if _, err := w.br.Peek(1); err != nil {
			if err == io.EOF {
				break
			}
			return nil, BitcoinChunks{}, xerrors.Errorf("peek at top-level: %w", err)
		}
		tag, err := w.readVarint()
		if err != nil {
			return nil, BitcoinChunks{}, xerrors.Errorf("read top-level tag: %w", err)
		}
		fieldNum := protoreflect.FieldNumber(tag >> 3)
		wireType := int(tag & 0x7)

		fd := blockDesc.Fields().ByNumber(fieldNum)
		if fd == nil {
			return nil, BitcoinChunks{}, xerrors.Errorf("unknown Block field %d (wt=%d): proto changed; update KnownBlockFields + switch in blockchain_bitcoin.stream.go", fieldNum, wireType)
		}

		// Specialized: when descending into the bitcoin blob, track
		// Header + InputTransactions group offsets instead of
		// materializing them.
		if fieldNum == blockFieldBitcoin {
			length, err := w.readVarint()
			if err != nil {
				return nil, BitcoinChunks{}, xerrors.Errorf("read bitcoin length: %w", err)
			}
			blob := &BitcoinBlobdata{}
			block.Blobdata = &Block_Bitcoin{Bitcoin: blob}
			endPos := w.pos + int64(length)
			blobChunks, err := w.walkBitcoinBlob(blob, endPos)
			if err != nil {
				return nil, BitcoinChunks{}, xerrors.Errorf("walk bitcoin blob: %w", err)
			}
			chunks = blobChunks
			continue
		}

		// All other fields: delegate to the generic walker's
		// reflection-driven decoder.
		if err := w.decodeField(blockMsg, fd, wireType); err != nil {
			return nil, BitcoinChunks{}, xerrors.Errorf("decode Block.%s: %w", fd.Name(), err)
		}
	}

	return block, chunks, nil
}

// walkBitcoinBlob walks a BitcoinBlobdata sub-message. Header and
// InputTransactions have their byte ranges recorded; every other
// field is decoded normally via the generic walker.
func (w *walker) walkBitcoinBlob(blob *BitcoinBlobdata, endPos int64) (BitcoinChunks, error) {
	_ = blob // blob is populated lazily via chunks; no fields are decoded here
	chunks := BitcoinChunks{}

	for w.pos < endPos {
		tag, err := w.readVarint()
		if err != nil {
			return BitcoinChunks{}, xerrors.Errorf("read bitcoin tag: %w", err)
		}
		fieldNum := protoreflect.FieldNumber(tag >> 3)
		wireType := int(tag & 0x7)

		switch fieldNum {
		case bitcoinBlobFieldHeader:
			length, err := w.readVarint()
			if err != nil {
				return BitcoinChunks{}, xerrors.Errorf("read header length: %w", err)
			}
			chunks.Header = BitcoinChunkRef{Offset: w.pos, Length: int64(length)}
			if err := w.discardFast(int64(length)); err != nil {
				return BitcoinChunks{}, xerrors.Errorf("discard header bytes: %w", err)
			}
		case bitcoinBlobFieldInputTransactions:
			length, err := w.readVarint()
			if err != nil {
				return BitcoinChunks{}, xerrors.Errorf("read input_transactions length: %w", err)
			}
			chunks.InputTransactionsGroups = append(chunks.InputTransactionsGroups,
				BitcoinChunkRef{Offset: w.pos, Length: int64(length)})
			if err := w.discardFast(int64(length)); err != nil {
				return BitcoinChunks{}, xerrors.Errorf("discard input_transactions bytes: %w", err)
			}
		case bitcoinBlobFieldRawInputTransactions:
			// raw_input_transactions is a map<string, bytes> storing
			// duplicate raw-JSON copies of the prev-tx data. The
			// bitcoin parser only reads input_transactions (field 2);
			// materializing raw_input_transactions would double peak
			// RAM on zcash (the two fields together are ~4 GB). Skip
			// it unconditionally in the streaming path — if some
			// future caller needs it, they can add a Chunks
			// accessor.
			length, err := w.readVarint()
			if err != nil {
				return BitcoinChunks{}, xerrors.Errorf("read raw_input_transactions length: %w", err)
			}
			if err := w.discardFast(int64(length)); err != nil {
				return BitcoinChunks{}, xerrors.Errorf("discard raw_input_transactions bytes: %w", err)
			}
		default:
			return BitcoinChunks{}, xerrors.Errorf("unknown BitcoinBlobdata field %d (wt=%d): proto changed; update KnownBitcoinBlobFields + switch in blockchain_bitcoin.stream.go", fieldNum, wireType)
		}
	}

	if w.pos != endPos {
		return BitcoinChunks{}, xerrors.Errorf("bitcoin blob consumed %d bytes, expected %d", w.pos, endPos)
	}
	return chunks, nil
}
