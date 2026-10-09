package cscb

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"sort"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Header struct {
		Codec                     api.Compression
		BlockCount                uint64
		ChunkCount                uint64
		StartHeight               uint64
		EndHeight                 uint64
		EnvelopeOffset            uint64
		EnvelopeLength            uint64
		EnvelopeCRC32             uint32
		BlockIndexRecordSize      uint32
		ChunkIndexRecordSize      uint32
		PayloadOffset             uint64
		PayloadCompressedLength   uint64
		PayloadUncompressedLength uint64
		PayloadCRC32              uint32
	}

	Index struct {
		Header Header
		Blocks []BlockDescriptor
		Chunks []ChunkDescriptor

		blocksByHeight map[uint64]int
	}

	BlockDescriptor struct {
		Height               uint64
		LogicalPayloadOffset uint64
		PayloadLength        uint64
		PayloadCRC32         uint32
		ChunkIndex           uint32
		ChunkRelativeOffset  uint64
		HashSHA256           [32]byte
		MetadataID           uint64
	}

	ChunkDescriptor struct {
		Index                   uint32
		StartHeight             uint64
		EndHeight               uint64
		CompressedPayloadOffset uint64
		CompressedLength        uint64
		UncompressedOffset      uint64
		UncompressedLength      uint64
		ChunkCRC32              uint32
		BlockCount              uint32
	}
)

func HeaderEnvelopeLength(data []byte) (uint64, error) {
	header, err := ParseHeader(data)
	if err != nil {
		return 0, err
	}
	length, err := checkedAdd(header.EnvelopeOffset, header.EnvelopeLength)
	if err != nil {
		return 0, err
	}
	return length, nil
}

func ParseHeader(data []byte) (*Header, error) {
	if len(data) < HeaderSize {
		return nil, xerrors.Errorf("CSCB header requires %d bytes, got %d", HeaderSize, len(data))
	}
	if !bytes.Equal(data[0:4], magic[:]) {
		return nil, xerrors.New("invalid CSCB magic")
	}
	if version := data[4]; version != formatVersion {
		return nil, xerrors.Errorf("unsupported CSCB format version: %d", version)
	}
	codec, err := parseCodec(data[5])
	if err != nil {
		return nil, err
	}
	if scope := data[6]; scope != compressionScopeChunked {
		return nil, xerrors.Errorf("unsupported CSCB compression scope: %d", scope)
	}

	header := &Header{
		Codec:                     codec,
		BlockCount:                uint64(binary.LittleEndian.Uint32(data[8:12])),
		ChunkCount:                uint64(binary.LittleEndian.Uint32(data[12:16])),
		StartHeight:               binary.LittleEndian.Uint64(data[16:24]),
		EndHeight:                 binary.LittleEndian.Uint64(data[24:32]),
		EnvelopeOffset:            binary.LittleEndian.Uint64(data[32:40]),
		EnvelopeLength:            binary.LittleEndian.Uint64(data[40:48]),
		EnvelopeCRC32:             binary.LittleEndian.Uint32(data[48:52]),
		BlockIndexRecordSize:      binary.LittleEndian.Uint32(data[52:56]),
		ChunkIndexRecordSize:      binary.LittleEndian.Uint32(data[56:60]),
		PayloadOffset:             binary.LittleEndian.Uint64(data[60:68]),
		PayloadCompressedLength:   binary.LittleEndian.Uint64(data[68:76]),
		PayloadUncompressedLength: binary.LittleEndian.Uint64(data[76:84]),
		PayloadCRC32:              binary.LittleEndian.Uint32(data[84:88]),
	}
	if header.BlockCount == 0 {
		return nil, xerrors.New("CSCB header has zero blocks")
	}
	if header.ChunkCount == 0 {
		return nil, xerrors.New("CSCB header has zero chunks")
	}
	if header.EnvelopeOffset != HeaderSize {
		return nil, xerrors.Errorf("unexpected CSCB envelope offset: got %d want %d", header.EnvelopeOffset, HeaderSize)
	}
	if header.BlockIndexRecordSize != BlockIndexRecordSize {
		return nil, xerrors.Errorf("unexpected CSCB block record size: got %d want %d", header.BlockIndexRecordSize, BlockIndexRecordSize)
	}
	if header.ChunkIndexRecordSize != ChunkIndexRecordSize {
		return nil, xerrors.Errorf("unexpected CSCB chunk record size: got %d want %d", header.ChunkIndexRecordSize, ChunkIndexRecordSize)
	}
	expectedPayloadOffset, err := checkedAdd(header.EnvelopeOffset, header.EnvelopeLength)
	if err != nil {
		return nil, err
	}
	if header.PayloadOffset != expectedPayloadOffset {
		return nil, xerrors.Errorf("unexpected CSCB payload offset: got %d want %d", header.PayloadOffset, expectedPayloadOffset)
	}
	return header, nil
}

func ParseIndex(data []byte) (*Index, error) {
	header, err := ParseHeader(data)
	if err != nil {
		return nil, err
	}
	required, err := HeaderEnvelopeLength(data)
	if err != nil {
		return nil, err
	}
	if required > uint64(len(data)) {
		return nil, xerrors.Errorf("CSCB index requires %d bytes, got %d", required, len(data))
	}

	envelopeStart, err := toInt(header.EnvelopeOffset)
	if err != nil {
		return nil, err
	}
	envelopeLength, err := toInt(header.EnvelopeLength)
	if err != nil {
		return nil, err
	}
	envelope := data[envelopeStart : envelopeStart+envelopeLength]
	if crc := crc32.ChecksumIEEE(envelope); crc != header.EnvelopeCRC32 {
		return nil, xerrors.Errorf("CSCB envelope CRC mismatch: got %08x want %08x", crc, header.EnvelopeCRC32)
	}
	if len(envelope) < EnvelopeHeaderSize {
		return nil, xerrors.Errorf("CSCB envelope requires %d bytes, got %d", EnvelopeHeaderSize, len(envelope))
	}
	if !bytes.Equal(envelope[0:4], envelopeMagic[:]) {
		return nil, xerrors.New("invalid CSCB envelope magic")
	}

	blockCount := binary.LittleEndian.Uint64(envelope[8:16])
	chunkCount := binary.LittleEndian.Uint64(envelope[16:24])
	if blockCount != header.BlockCount {
		return nil, xerrors.Errorf("CSCB block count mismatch: header=%d envelope=%d", header.BlockCount, blockCount)
	}
	if chunkCount != header.ChunkCount {
		return nil, xerrors.Errorf("CSCB chunk count mismatch: header=%d envelope=%d", header.ChunkCount, chunkCount)
	}
	if startHeight := binary.LittleEndian.Uint64(envelope[24:32]); startHeight != header.StartHeight {
		return nil, xerrors.Errorf("CSCB start height mismatch: header=%d envelope=%d", header.StartHeight, startHeight)
	}
	if endHeight := binary.LittleEndian.Uint64(envelope[32:40]); endHeight != header.EndHeight {
		return nil, xerrors.Errorf("CSCB end height mismatch: header=%d envelope=%d", header.EndHeight, endHeight)
	}

	blockIndexOffset := binary.LittleEndian.Uint64(envelope[40:48])
	blockIndexLength := binary.LittleEndian.Uint64(envelope[48:56])
	chunkIndexOffset := binary.LittleEndian.Uint64(envelope[56:64])
	chunkIndexLength := binary.LittleEndian.Uint64(envelope[64:72])
	if blockIndexOffset != EnvelopeHeaderSize {
		return nil, xerrors.Errorf("unexpected CSCB block index offset: got %d want %d", blockIndexOffset, EnvelopeHeaderSize)
	}
	expectedBlockIndexLength, err := checkedMul(blockCount, BlockIndexRecordSize)
	if err != nil {
		return nil, err
	}
	if blockIndexLength != expectedBlockIndexLength {
		return nil, xerrors.Errorf("unexpected CSCB block index length: got %d want %d", blockIndexLength, expectedBlockIndexLength)
	}
	expectedChunkIndexOffset, err := checkedAdd(blockIndexOffset, blockIndexLength)
	if err != nil {
		return nil, err
	}
	if chunkIndexOffset != expectedChunkIndexOffset {
		return nil, xerrors.Errorf("unexpected CSCB chunk index offset: got %d want %d", chunkIndexOffset, expectedChunkIndexOffset)
	}
	expectedChunkIndexLength, err := checkedMul(chunkCount, ChunkIndexRecordSize)
	if err != nil {
		return nil, err
	}
	if chunkIndexLength != expectedChunkIndexLength {
		return nil, xerrors.Errorf("unexpected CSCB chunk index length: got %d want %d", chunkIndexLength, expectedChunkIndexLength)
	}
	indexEnd, err := checkedAdd(chunkIndexOffset, chunkIndexLength)
	if err != nil {
		return nil, err
	}
	if indexEnd != header.EnvelopeLength {
		return nil, xerrors.Errorf("unexpected CSCB envelope length: index_end=%d envelope_length=%d", indexEnd, header.EnvelopeLength)
	}

	blocks, err := parseBlockDescriptors(envelope, blockIndexOffset, blockCount)
	if err != nil {
		return nil, err
	}
	chunks, err := parseChunkDescriptors(envelope, chunkIndexOffset, chunkCount)
	if err != nil {
		return nil, err
	}
	index := &Index{
		Header:         *header,
		Blocks:         blocks,
		Chunks:         chunks,
		blocksByHeight: make(map[uint64]int, len(blocks)),
	}
	for i := range blocks {
		if _, ok := index.blocksByHeight[blocks[i].Height]; ok {
			return nil, xerrors.Errorf("duplicate CSCB block height: %d", blocks[i].Height)
		}
		index.blocksByHeight[blocks[i].Height] = i
	}
	return index, nil
}

func (i *Index) LookupBlock(metadata *api.BlockMetadata) (*BlockDescriptor, *ChunkDescriptor, error) {
	if metadata == nil {
		return nil, nil, xerrors.New("block metadata is required")
	}
	blockIndex, ok := i.blocksByHeight[metadata.GetHeight()]
	if !ok {
		return nil, nil, xerrors.Errorf("CSCB block height %d not found", metadata.GetHeight())
	}
	block := &i.Blocks[blockIndex]
	if block.LogicalPayloadOffset != metadata.GetByteOffset() {
		return nil, nil, xerrors.Errorf("CSCB block offset mismatch at height %d: index=%d metadata=%d", metadata.GetHeight(), block.LogicalPayloadOffset, metadata.GetByteOffset())
	}
	if block.PayloadLength != metadata.GetByteLength() {
		return nil, nil, xerrors.Errorf("CSCB block length mismatch at height %d: index=%d metadata=%d", metadata.GetHeight(), block.PayloadLength, metadata.GetByteLength())
	}
	if metadata.GetUncompressedLength() != 0 && metadata.GetUncompressedLength() != block.PayloadLength {
		return nil, nil, xerrors.Errorf("CSCB block uncompressed length mismatch at height %d: index=%d metadata=%d", metadata.GetHeight(), block.PayloadLength, metadata.GetUncompressedLength())
	}
	hashDigest := sha256.Sum256([]byte(metadata.GetHash()))
	if hashDigest != block.HashSHA256 {
		return nil, nil, xerrors.Errorf("CSCB block hash mismatch at height %d", metadata.GetHeight())
	}
	if int(block.ChunkIndex) >= len(i.Chunks) {
		return nil, nil, xerrors.Errorf("CSCB chunk index %d out of range for height %d", block.ChunkIndex, metadata.GetHeight())
	}
	chunk := &i.Chunks[block.ChunkIndex]
	if metadata.GetHeight() < chunk.StartHeight || metadata.GetHeight() >= chunk.EndHeight {
		return nil, nil, xerrors.Errorf("CSCB block height %d outside chunk range [%d, %d)", metadata.GetHeight(), chunk.StartHeight, chunk.EndHeight)
	}
	return block, chunk, nil
}

func ValidateChunkPayload(chunkPayload []byte, chunk *ChunkDescriptor) error {
	if chunk == nil {
		return xerrors.New("CSCB chunk descriptor is required")
	}
	if uint64(len(chunkPayload)) != chunk.UncompressedLength {
		return xerrors.Errorf("CSCB chunk length mismatch: got %d want %d", len(chunkPayload), chunk.UncompressedLength)
	}
	if crc := crc32.ChecksumIEEE(chunkPayload); crc != chunk.ChunkCRC32 {
		return xerrors.Errorf("CSCB chunk CRC mismatch: got %08x want %08x", crc, chunk.ChunkCRC32)
	}
	return nil
}

func DecodeChunkFrame(frame io.Reader, codec api.Compression) ([]byte, error) {
	reader, err := NewChunkDecompressor(frame, codec)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	decoded, err := io.ReadAll(reader)
	if err != nil {
		return nil, xerrors.Errorf("failed to decompress CSCB chunk: %w", err)
	}
	return decoded, nil
}

type blockPayloadRead struct {
	originalIndex int
	block         *BlockDescriptor
	start         uint64
	end           uint64
}

type blockPayloadStream struct {
	frame     io.Closer
	reader    io.ReadCloser
	block     *BlockDescriptor
	remaining uint64
	crc       hash.Hash32
	validated bool
	closed    bool
}

// ExtractBlockPayloadsFromChunkFrame streams a compressed chunk and returns
// only the requested block payloads in the same order as blocks. It validates
// each requested block CRC without materializing the full decompressed chunk.
func ExtractBlockPayloadsFromChunkFrame(frame io.Reader, codec api.Compression, chunk *ChunkDescriptor, blocks []*BlockDescriptor) ([][]byte, error) {
	if chunk == nil {
		return nil, xerrors.New("CSCB chunk descriptor is required")
	}
	if len(blocks) == 0 {
		return nil, nil
	}

	reads := make([]blockPayloadRead, len(blocks))
	for i, block := range blocks {
		if block == nil {
			return nil, xerrors.New("CSCB block descriptor is required")
		}
		if block.ChunkIndex != chunk.Index {
			return nil, xerrors.Errorf("CSCB block chunk mismatch at height %d: block=%d chunk=%d", block.Height, block.ChunkIndex, chunk.Index)
		}
		end, err := checkedAdd(block.ChunkRelativeOffset, block.PayloadLength)
		if err != nil {
			return nil, err
		}
		if end > chunk.UncompressedLength {
			return nil, xerrors.Errorf("CSCB block payload exceeds chunk bounds: end=%d chunk_length=%d", end, chunk.UncompressedLength)
		}
		reads[i] = blockPayloadRead{
			originalIndex: i,
			block:         block,
			start:         block.ChunkRelativeOffset,
			end:           end,
		}
	}
	sort.SliceStable(reads, func(i, j int) bool {
		return reads[i].start < reads[j].start
	})

	reader, err := NewChunkDecompressor(frame, codec)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()

	payloads := make([][]byte, len(blocks))
	var offset uint64
	var hasPrevious bool
	var previousStart uint64
	var previousEnd uint64
	var previousPayload []byte
	var previousCRC uint32
	for _, read := range reads {
		if read.start < offset {
			if hasPrevious && read.start == previousStart && read.end == previousEnd {
				if previousCRC != read.block.PayloadCRC32 {
					return nil, xerrors.Errorf("CSCB duplicate block CRC mismatch at height %d: got %08x want %08x", read.block.Height, previousCRC, read.block.PayloadCRC32)
				}
				payloads[read.originalIndex] = append([]byte(nil), previousPayload...)
				continue
			}
			return nil, xerrors.Errorf("CSCB block payload overlaps previous read at height %d: start=%d offset=%d", read.block.Height, read.start, offset)
		}
		if err := discardExactly(reader, read.start-offset); err != nil {
			return nil, err
		}

		length, err := toInt(read.block.PayloadLength)
		if err != nil {
			return nil, err
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, xerrors.Errorf("failed to read CSCB block payload at height %d: %w", read.block.Height, err)
		}
		offset = read.end
		if crc := crc32.ChecksumIEEE(payload); crc != read.block.PayloadCRC32 {
			return nil, xerrors.Errorf("CSCB block CRC mismatch at height %d: got %08x want %08x", read.block.Height, crc, read.block.PayloadCRC32)
		} else {
			previousCRC = crc
		}
		payloads[read.originalIndex] = payload
		hasPrevious = true
		previousStart = read.start
		previousEnd = read.end
		previousPayload = payload
	}
	return payloads, nil
}

// OpenBlockPayloadFromChunkFrame streams one block payload out of a compressed
// CSCB chunk frame. The returned reader validates the block payload length and
// CRC when read to EOF. Close drains unread payload bytes before closing so
// callers that stop early can still observe validation/short-read errors.
func OpenBlockPayloadFromChunkFrame(frame io.ReadCloser, codec api.Compression, chunk *ChunkDescriptor, block *BlockDescriptor) (io.ReadCloser, error) {
	if frame == nil {
		return nil, xerrors.New("CSCB chunk frame is required")
	}
	if chunk == nil {
		_ = frame.Close()
		return nil, xerrors.New("CSCB chunk descriptor is required")
	}
	if err := validateBlockPayloadBounds(chunk, block); err != nil {
		_ = frame.Close()
		return nil, err
	}

	reader, err := NewChunkDecompressor(frame, codec)
	if err != nil {
		_ = frame.Close()
		return nil, err
	}
	if err := discardExactly(reader, block.ChunkRelativeOffset); err != nil {
		_ = reader.Close()
		_ = frame.Close()
		return nil, err
	}
	return &blockPayloadStream{
		frame:     frame,
		reader:    reader,
		block:     block,
		remaining: block.PayloadLength,
		crc:       crc32.NewIEEE(),
	}, nil
}

func NewChunkDecompressor(frame io.Reader, codec api.Compression) (io.ReadCloser, error) {
	switch codec {
	case api.Compression_GZIP:
		reader, err := gzip.NewReader(frame)
		if err != nil {
			return nil, xerrors.Errorf("gzip reader: %w", err)
		}
		return reader, nil
	case api.Compression_ZSTD:
		reader, err := zstd.NewReader(
			frame,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxMemory(maxZstdDecoderMemoryBytes),
		)
		if err != nil {
			return nil, xerrors.Errorf("zstd reader: %w", err)
		}
		return &zstdReadCloser{reader}, nil
	default:
		return nil, xerrors.Errorf("unsupported CSCB codec: %v", codec)
	}
}

func validateBlockPayloadBounds(chunk *ChunkDescriptor, block *BlockDescriptor) error {
	if block == nil {
		return xerrors.New("CSCB block descriptor is required")
	}
	if block.ChunkIndex != chunk.Index {
		return xerrors.Errorf("CSCB block chunk mismatch at height %d: block=%d chunk=%d", block.Height, block.ChunkIndex, chunk.Index)
	}
	end, err := checkedAdd(block.ChunkRelativeOffset, block.PayloadLength)
	if err != nil {
		return err
	}
	if end > chunk.UncompressedLength {
		return xerrors.Errorf("CSCB block payload exceeds chunk bounds: end=%d chunk_length=%d", end, chunk.UncompressedLength)
	}
	return nil
}

func (r *blockPayloadStream) Read(p []byte) (int, error) {
	if r.closed {
		return 0, xerrors.New("CSCB block payload reader is closed")
	}
	if r.remaining == 0 {
		if err := r.validate(); err != nil {
			return 0, err
		}
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(len(p)) > r.remaining {
		p = p[:int(r.remaining)]
	}
	n, err := r.reader.Read(p)
	if n > 0 {
		if _, writeErr := r.crc.Write(p[:n]); writeErr != nil {
			return n, writeErr
		}
		r.remaining -= uint64(n)
	}
	if err != nil && r.remaining > 0 {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return n, err
	}
	if r.remaining == 0 {
		if validateErr := r.validate(); validateErr != nil {
			return n, validateErr
		}
	}
	return n, err
}

func (r *blockPayloadStream) Close() error {
	if r.closed {
		return nil
	}
	var closeErr error
	if !r.validated {
		if _, err := io.Copy(io.Discard, r); err != nil {
			closeErr = err
		}
	}
	r.closed = true
	if err := r.reader.Close(); closeErr == nil && err != nil {
		closeErr = err
	}
	if err := r.frame.Close(); closeErr == nil && err != nil {
		closeErr = err
	}
	return closeErr
}

func (r *blockPayloadStream) validate() error {
	if r.validated {
		return nil
	}
	r.validated = true
	if r.remaining != 0 {
		return xerrors.Errorf("CSCB block payload length mismatch at height %d: missing %d bytes", r.block.Height, r.remaining)
	}
	if crc := r.crc.Sum32(); crc != r.block.PayloadCRC32 {
		return xerrors.Errorf("CSCB block CRC mismatch at height %d: got %08x want %08x", r.block.Height, crc, r.block.PayloadCRC32)
	}
	return nil
}

func ExtractBlockPayload(chunkPayload []byte, block *BlockDescriptor) ([]byte, error) {
	if block == nil {
		return nil, xerrors.New("CSCB block descriptor is required")
	}
	end, err := checkedAdd(block.ChunkRelativeOffset, block.PayloadLength)
	if err != nil {
		return nil, err
	}
	if end > uint64(len(chunkPayload)) {
		return nil, xerrors.Errorf("CSCB block payload exceeds chunk bounds: end=%d chunk_length=%d", end, len(chunkPayload))
	}
	startIndex, err := toInt(block.ChunkRelativeOffset)
	if err != nil {
		return nil, err
	}
	endIndex, err := toInt(end)
	if err != nil {
		return nil, err
	}
	payload := chunkPayload[startIndex:endIndex]
	if crc := crc32.ChecksumIEEE(payload); crc != block.PayloadCRC32 {
		return nil, xerrors.Errorf("CSCB block CRC mismatch: got %08x want %08x", crc, block.PayloadCRC32)
	}
	return payload, nil
}

func discardExactly(reader io.Reader, n uint64) error {
	if n == 0 {
		return nil
	}
	const maxInt64 = uint64(1<<63 - 1)
	if n >= maxInt64 {
		return xerrors.Errorf("discard length %d is too large", n)
	}
	if _, err := io.CopyN(io.Discard, reader, int64(n)); err != nil {
		return xerrors.Errorf("failed to skip CSCB chunk bytes: %w", err)
	}
	return nil
}

type zstdReadCloser struct {
	*zstd.Decoder
}

func (z *zstdReadCloser) Close() error {
	z.Decoder.Close()
	return nil
}

func parseBlockDescriptors(envelope []byte, offset uint64, count uint64) ([]BlockDescriptor, error) {
	start, err := toInt(offset)
	if err != nil {
		return nil, err
	}
	countInt, err := toInt(count)
	if err != nil {
		return nil, err
	}
	blocks := make([]BlockDescriptor, countInt)
	for i := 0; i < countInt; i++ {
		recordStart := start + i*BlockIndexRecordSize
		recordEnd := recordStart + BlockIndexRecordSize
		if recordEnd > len(envelope) {
			return nil, xerrors.Errorf("CSCB block index record %d exceeds envelope length", i)
		}
		record := envelope[recordStart:recordEnd]
		var hash [32]byte
		copy(hash[:], record[48:80])
		blocks[i] = BlockDescriptor{
			Height:               binary.LittleEndian.Uint64(record[0:8]),
			LogicalPayloadOffset: binary.LittleEndian.Uint64(record[8:16]),
			PayloadLength:        binary.LittleEndian.Uint64(record[16:24]),
			PayloadCRC32:         binary.LittleEndian.Uint32(record[24:28]),
			ChunkIndex:           binary.LittleEndian.Uint32(record[28:32]),
			ChunkRelativeOffset:  binary.LittleEndian.Uint64(record[32:40]),
			HashSHA256:           hash,
			MetadataID:           binary.LittleEndian.Uint64(record[80:88]),
		}
	}
	return blocks, nil
}

func parseChunkDescriptors(envelope []byte, offset uint64, count uint64) ([]ChunkDescriptor, error) {
	start, err := toInt(offset)
	if err != nil {
		return nil, err
	}
	countInt, err := toInt(count)
	if err != nil {
		return nil, err
	}
	chunks := make([]ChunkDescriptor, countInt)
	for i := 0; i < countInt; i++ {
		recordStart := start + i*ChunkIndexRecordSize
		recordEnd := recordStart + ChunkIndexRecordSize
		if recordEnd > len(envelope) {
			return nil, xerrors.Errorf("CSCB chunk index record %d exceeds envelope length", i)
		}
		record := envelope[recordStart:recordEnd]
		chunks[i] = ChunkDescriptor{
			Index:                   binary.LittleEndian.Uint32(record[0:4]),
			StartHeight:             binary.LittleEndian.Uint64(record[8:16]),
			EndHeight:               binary.LittleEndian.Uint64(record[16:24]),
			CompressedPayloadOffset: binary.LittleEndian.Uint64(record[24:32]),
			CompressedLength:        binary.LittleEndian.Uint64(record[32:40]),
			UncompressedOffset:      binary.LittleEndian.Uint64(record[40:48]),
			UncompressedLength:      binary.LittleEndian.Uint64(record[48:56]),
			ChunkCRC32:              binary.LittleEndian.Uint32(record[56:60]),
			BlockCount:              binary.LittleEndian.Uint32(record[60:64]),
		}
		if chunks[i].Index != uint32(i) {
			return nil, xerrors.Errorf("unexpected CSCB chunk index: got %d want %d", chunks[i].Index, i)
		}
	}
	return chunks, nil
}

func parseCodec(id byte) (api.Compression, error) {
	switch id {
	case codecGzip:
		return api.Compression_GZIP, nil
	case codecZstd:
		return api.Compression_ZSTD, nil
	default:
		return api.Compression_NONE, xerrors.Errorf("unsupported CSCB codec id: %d", id)
	}
}

func checkedAdd(a uint64, b uint64) (uint64, error) {
	if a > ^uint64(0)-b {
		return 0, xerrors.Errorf("uint64 overflow: %d + %d", a, b)
	}
	return a + b, nil
}

func checkedMul(a uint64, b uint64) (uint64, error) {
	if a != 0 && b > ^uint64(0)/a {
		return 0, xerrors.Errorf("uint64 overflow: %d * %d", a, b)
	}
	return a * b, nil
}

func toInt(v uint64) (int, error) {
	maxInt := uint64(^uint(0) >> 1)
	if v > maxInt {
		return 0, xerrors.Errorf("value %d exceeds max int %d", v, maxInt)
	}
	return int(v), nil
}
