package chainstorage

// Solana-specific variant of the Block wire walker. Shares the generic
// walker infrastructure from block_stream.go, but when the
// SolanaBlobdata.Header field is encountered the bytes are skipped and
// only their (offset, length) within the walked stream is recorded.
// Callers layer a spool-file reader over that offset to stream the
// getBlock JSON directly into the parser without materializing it as a
// []byte in RAM.
//
// Mirrors blockchain_bitcoin.stream.go. The Solana blob is simpler: it
// has exactly one field (the raw getBlock JSON), so there are no
// sibling chunks to track.

import (
	"bufio"
	"io"

	"golang.org/x/xerrors"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SolanaBlobdata field numbers.
const (
	solanaBlobFieldHeader = 1
)

// KnownSolanaBlobFields enumerates every field the walker recognizes
// on SolanaBlobdata. The contract test in wire_walker_contract_test.go
// asserts it matches the proto descriptor exactly, in both directions.
var KnownSolanaBlobFields = map[protoreflect.FieldNumber]WireWalkerKnownField{
	solanaBlobFieldHeader: {Kind: protoreflect.BytesKind, Process: true},
}

// SolanaChunkRef points at a byte range within the walked stream.
// Zero-value means "not present".
type SolanaChunkRef struct {
	Offset int64
	Length int64
}

// SolanaChunks summarizes the byte ranges the walker skipped inside
// SolanaBlobdata so the caller can stream them on demand without
// retaining the whole blob in memory.
type SolanaChunks struct {
	// Header is the range of SolanaBlobdata.Header bytes: the raw
	// getBlock JSON (jsonParsed encoding) for the slot.
	Header SolanaChunkRef
}

// WalkSolanaEnvelope reads a proto-encoded Block from r and returns a
// *Block whose SolanaBlobdata.Header is left nil, plus the byte range
// of that header within the walked stream so the caller can seek into
// a spooled copy of r and stream it on demand.
//
// Every other field (Block metadata, other blobdata variants) is
// decoded normally via the generic walker. Peak RAM during this call is
// bounded by the materialized parts of the Block — for Solana that is
// the metadata only, since the blob is nothing but the header.
//
// Unknown field numbers on the wire return an error. See
// Known{Block,SolanaBlob}Fields and the contract test for the
// drift-defense guarantees.
func WalkSolanaEnvelope(r io.Reader) (*Block, SolanaChunks, error) {
	block := &Block{}
	chunks := SolanaChunks{}
	w := &walker{br: bufio.NewReaderSize(r, 64*1024)}
	blockMsg := block.ProtoReflect()
	blockDesc := blockMsg.Descriptor()

	for {
		if _, err := w.br.Peek(1); err != nil {
			if err == io.EOF {
				break
			}
			return nil, SolanaChunks{}, xerrors.Errorf("peek at top-level: %w", err)
		}
		tag, err := w.readVarint()
		if err != nil {
			return nil, SolanaChunks{}, xerrors.Errorf("read top-level tag: %w", err)
		}
		fieldNum := protoreflect.FieldNumber(tag >> 3)
		wireType := int(tag & 0x7)

		fd := blockDesc.Fields().ByNumber(fieldNum)
		if fd == nil {
			return nil, SolanaChunks{}, xerrors.Errorf("unknown Block field %d (wt=%d): proto changed; update KnownBlockFields + switch in blockchain_solana.stream.go", fieldNum, wireType)
		}

		// Specialized: when descending into the solana blob, record
		// the header offset instead of materializing it.
		if fieldNum == blockFieldSolana {
			if wireType != wireBytes {
				return nil, SolanaChunks{}, xerrors.Errorf("Block.solana has wire type %d, expected %d", wireType, wireBytes)
			}
			length, err := w.readVarint()
			if err != nil {
				return nil, SolanaChunks{}, xerrors.Errorf("read solana length: %w", err)
			}
			block.Blobdata = &Block_Solana{Solana: &SolanaBlobdata{}}
			endPos := w.pos + int64(length)
			blobChunks, err := w.walkSolanaBlob(endPos)
			if err != nil {
				return nil, SolanaChunks{}, xerrors.Errorf("walk solana blob: %w", err)
			}
			chunks = blobChunks
			continue
		}

		// All other fields: delegate to the generic walker's
		// reflection-driven decoder.
		if err := w.decodeField(blockMsg, fd, wireType); err != nil {
			return nil, SolanaChunks{}, xerrors.Errorf("decode Block.%s: %w", fd.Name(), err)
		}
	}

	return block, chunks, nil
}

// walkSolanaBlob walks a SolanaBlobdata sub-message, recording the
// header's byte range and discarding its bytes.
func (w *walker) walkSolanaBlob(endPos int64) (SolanaChunks, error) {
	chunks := SolanaChunks{}

	for w.pos < endPos {
		tag, err := w.readVarint()
		if err != nil {
			return SolanaChunks{}, xerrors.Errorf("read solana tag: %w", err)
		}
		fieldNum := protoreflect.FieldNumber(tag >> 3)
		wireType := int(tag & 0x7)

		switch fieldNum {
		case solanaBlobFieldHeader:
			if wireType != wireBytes {
				return SolanaChunks{}, xerrors.Errorf("SolanaBlobdata.header has wire type %d, expected %d", wireType, wireBytes)
			}
			length, err := w.readVarint()
			if err != nil {
				return SolanaChunks{}, xerrors.Errorf("read header length: %w", err)
			}
			chunks.Header = SolanaChunkRef{Offset: w.pos, Length: int64(length)}
			if err := w.discardFast(int64(length)); err != nil {
				return SolanaChunks{}, xerrors.Errorf("discard header bytes: %w", err)
			}
		default:
			return SolanaChunks{}, xerrors.Errorf("unknown SolanaBlobdata field %d (wt=%d): proto changed; update KnownSolanaBlobFields + switch in blockchain_solana.stream.go", fieldNum, wireType)
		}
	}

	if w.pos != endPos {
		return SolanaChunks{}, xerrors.Errorf("solana blob consumed %d bytes, expected %d", w.pos, endPos)
	}
	return chunks, nil
}
