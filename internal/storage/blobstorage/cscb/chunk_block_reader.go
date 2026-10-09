package cscb

import (
	"hash/crc32"
	"io"

	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// ChunkBlockReader extracts the requested blocks of one compressed CSCB
// chunk frame sequentially, one payload at a time. It is the
// one-block-resident counterpart of ExtractBlockPayloadsFromChunkFrame:
// a 25-block chunk is decompressed exactly once, but only the block
// being handed out is materialized.
//
// blocks must be in ascending chunk-offset order and must not overlap;
// the reader yields them in that order. Each payload is CRC-validated
// against its descriptor before it is returned. The reader owns the
// frame and closes it on Close; Close is safe to call at any point,
// including after an error.
type ChunkBlockReader struct {
	frame  io.Closer
	reader io.ReadCloser
	reads  []blockPayloadRead
	pos    int
	offset uint64
	closed bool
}

// NewChunkBlockReader opens a sequential reader over frame (the
// compressed bytes of exactly one chunk, as served by an HTTP range
// request) and positions it at the first requested block. On error the
// frame is closed.
func NewChunkBlockReader(frame io.ReadCloser, codec api.Compression, chunk *ChunkDescriptor, blocks []*BlockDescriptor) (*ChunkBlockReader, error) {
	if frame == nil {
		return nil, xerrors.New("CSCB chunk frame is required")
	}
	if chunk == nil {
		_ = frame.Close()
		return nil, xerrors.New("CSCB chunk descriptor is required")
	}
	if len(blocks) == 0 {
		_ = frame.Close()
		return nil, xerrors.New("CSCB block descriptors are required")
	}

	reads := make([]blockPayloadRead, len(blocks))
	var previousEnd uint64
	for i, block := range blocks {
		if err := validateBlockPayloadBounds(chunk, block); err != nil {
			_ = frame.Close()
			return nil, err
		}
		end, err := BlockPayloadEnd(block)
		if err != nil {
			_ = frame.Close()
			return nil, err
		}
		if i > 0 && block.ChunkRelativeOffset < previousEnd {
			_ = frame.Close()
			return nil, xerrors.Errorf("CSCB block payloads out of order at height %d: start=%d previous_end=%d", block.Height, block.ChunkRelativeOffset, previousEnd)
		}
		reads[i] = blockPayloadRead{
			originalIndex: i,
			block:         block,
			start:         block.ChunkRelativeOffset,
			end:           end,
		}
		previousEnd = end
	}

	reader, err := NewChunkDecompressor(frame, codec)
	if err != nil {
		_ = frame.Close()
		return nil, err
	}
	return &ChunkBlockReader{
		frame:  frame,
		reader: reader,
		reads:  reads,
	}, nil
}

// BlockPayloadEnd returns the chunk-relative offset one past the block's
// payload, rejecting descriptors whose offset+length overflows.
func BlockPayloadEnd(block *BlockDescriptor) (uint64, error) {
	if block == nil {
		return 0, xerrors.New("CSCB block descriptor is required")
	}
	return checkedAdd(block.ChunkRelativeOffset, block.PayloadLength)
}

// Next returns the next requested block and its validated payload, or
// io.EOF once every requested block has been returned. Any other error
// leaves the reader unusable; the caller reopens the frame to resume.
func (r *ChunkBlockReader) Next() (*BlockDescriptor, []byte, error) {
	if r.closed {
		return nil, nil, xerrors.New("CSCB chunk block reader is closed")
	}
	if r.pos >= len(r.reads) {
		return nil, nil, io.EOF
	}
	read := r.reads[r.pos]
	if err := discardExactly(r.reader, read.start-r.offset); err != nil {
		return nil, nil, err
	}
	length, err := toInt(read.block.PayloadLength)
	if err != nil {
		return nil, nil, err
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r.reader, payload); err != nil {
		return nil, nil, xerrors.Errorf("failed to read CSCB block payload at height %d: %w", read.block.Height, err)
	}
	r.offset = read.end
	r.pos++
	if crc := crc32.ChecksumIEEE(payload); crc != read.block.PayloadCRC32 {
		return nil, nil, xerrors.Errorf("CSCB block CRC mismatch at height %d: got %08x want %08x", read.block.Height, crc, read.block.PayloadCRC32)
	}
	return read.block, payload, nil
}

// Close releases the decompressor and the underlying frame. Unread
// chunk bytes are not drained: unlike OpenBlockPayloadFromChunkFrame,
// per-block CRCs are checked on return, so there is no deferred
// validation to observe. (As with the rest of the CSCB download path,
// an undrained HTTP body forfeits keep-alive for that connection.)
func (r *ChunkBlockReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	closeErr := r.reader.Close()
	if err := r.frame.Close(); closeErr == nil && err != nil {
		closeErr = err
	}
	return closeErr
}
