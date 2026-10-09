package retirement

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	storageutils "github.com/coinbase/chainstorage/internal/storage/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const retirementInitialIndexReadSize = 64 * 1024

type blockPayloadVerifier interface {
	Verify(ctx context.Context, candidate Candidate) (string, error)
	VerifyConsolidated(ctx context.Context, candidate Candidate) (string, error)
}

type (
	PinnedPayloadInspection struct {
		CanonicalSHA256     string
		HasStoragePlacement bool
	}

	PinnedPayloadVerifier interface {
		ReadSingleBlock(ctx context.Context, candidate Candidate) (*api.Block, PinnedPayloadInspection, error)
		InspectSingleBlock(ctx context.Context, candidate Candidate) (PinnedPayloadInspection, error)
		InspectConsolidated(ctx context.Context, candidate Candidate) (PinnedPayloadInspection, error)
	}
)

type payloadVerifier struct {
	store ObjectStore

	indexes       map[string]*cscb.Index
	lastChunkKey  string
	lastChunkData []byte
}

func newPayloadVerifier(store ObjectStore) *payloadVerifier {
	return &payloadVerifier{
		store:   store,
		indexes: make(map[string]*cscb.Index),
	}
}

func NewPinnedPayloadVerifier(store ObjectStore) PinnedPayloadVerifier {
	return newPayloadVerifier(store)
}

func (v *payloadVerifier) Verify(ctx context.Context, candidate Candidate) (string, error) {
	singleBlockInspection, err := v.InspectSingleBlock(ctx, candidate)
	if err != nil {
		return "", err
	}
	consolidatedInspection, err := v.InspectConsolidated(ctx, candidate)
	if err != nil {
		return "", err
	}
	if consolidatedInspection.HasStoragePlacement {
		return "", xerrors.Errorf("pinned CSCB block payload retains storage placement metadata at height %d", candidate.Height)
	}
	if singleBlockInspection.CanonicalSHA256 != consolidatedInspection.CanonicalSHA256 {
		return "", xerrors.Errorf("single-block and CSCB block payloads differ at height %d", candidate.Height)
	}
	return singleBlockInspection.CanonicalSHA256, nil
}

func (v *payloadVerifier) InspectSingleBlock(ctx context.Context, candidate Candidate) (PinnedPayloadInspection, error) {
	_, inspection, err := v.ReadSingleBlock(ctx, candidate)
	return inspection, err
}

func (v *payloadVerifier) ReadSingleBlock(
	ctx context.Context,
	candidate Candidate,
) (*api.Block, PinnedPayloadInspection, error) {
	singleBlockCompressed, err := v.store.ReadObjectVersion(ctx, candidate.Bucket, candidate.Key, candidate.VersionID)
	if err != nil {
		return nil, PinnedPayloadInspection{}, err
	}
	singleBlockPayload, err := storageutils.Decompress(singleBlockCompressed, storageutils.GetCompressionType(candidate.Key))
	if err != nil {
		return nil, PinnedPayloadInspection{}, xerrors.Errorf("failed to decompress pinned single-block object: %w", err)
	}
	var singleBlockBlock api.Block
	if err := proto.Unmarshal(singleBlockPayload, &singleBlockBlock); err != nil {
		return nil, PinnedPayloadInspection{}, xerrors.Errorf("failed to parse pinned single-block block payload: %w", err)
	}
	if err := validatePayloadIdentity(&singleBlockBlock, candidate); err != nil {
		return nil, PinnedPayloadInspection{}, xerrors.Errorf("pinned single-block block identity mismatch: %w", err)
	}
	normalized := storageutils.CloneBlockWithoutStoragePlacement(&singleBlockBlock)
	digest, err := canonicalBlockDigest(normalized)
	if err != nil {
		return nil, PinnedPayloadInspection{}, err
	}
	return &singleBlockBlock, PinnedPayloadInspection{
		CanonicalSHA256:     digest,
		HasStoragePlacement: storageutils.HasBlockStoragePlacement(&singleBlockBlock),
	}, nil
}

func (v *payloadVerifier) InspectConsolidated(ctx context.Context, candidate Candidate) (PinnedPayloadInspection, error) {
	_, cscbBlock, err := v.readConsolidatedPayload(ctx, candidate)
	if err != nil {
		return PinnedPayloadInspection{}, err
	}
	if err := validatePayloadIdentity(cscbBlock, candidate); err != nil {
		return PinnedPayloadInspection{}, xerrors.Errorf("pinned CSCB block identity mismatch: %w", err)
	}
	normalized := storageutils.CloneBlockWithoutStoragePlacement(cscbBlock)
	digest, err := canonicalBlockDigest(normalized)
	if err != nil {
		return PinnedPayloadInspection{}, err
	}
	return PinnedPayloadInspection{
		CanonicalSHA256:     digest,
		HasStoragePlacement: storageutils.HasBlockStoragePlacement(cscbBlock),
	}, nil
}

func (v *payloadVerifier) VerifyConsolidated(ctx context.Context, candidate Candidate) (string, error) {
	inspection, err := v.InspectConsolidated(ctx, candidate)
	if err != nil {
		return "", err
	}
	if inspection.HasStoragePlacement {
		return "", xerrors.Errorf("pinned CSCB block payload retains storage placement metadata at height %d", candidate.Height)
	}
	return inspection.CanonicalSHA256, nil
}

func canonicalBlockDigest(block *api.Block) (string, error) {
	// This digest is persisted in retirement and repair manifests. Keep its
	// deterministic-protobuf algorithm stable across upgrades and rollbacks.
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(block)
	if err != nil {
		return "", xerrors.Errorf("failed to marshal canonical block payload: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func validatePayloadIdentity(block *api.Block, candidate Candidate) error {
	metadata := block.GetMetadata()
	if metadata == nil {
		return xerrors.New("block metadata is missing")
	}
	if metadata.GetTag() != candidate.Tag || metadata.GetHeight() != candidate.Height || metadata.GetHash() != candidate.Hash {
		return xerrors.Errorf(
			"got tag=%d height=%d hash=%q; want tag=%d height=%d hash=%q",
			metadata.GetTag(),
			metadata.GetHeight(),
			metadata.GetHash(),
			candidate.Tag,
			candidate.Height,
			candidate.Hash,
		)
	}
	return nil
}

func (v *payloadVerifier) readConsolidatedPayload(ctx context.Context, candidate Candidate) ([]byte, *api.Block, error) {
	index, err := v.readIndex(ctx, candidate)
	if err != nil {
		return nil, nil, err
	}
	block, chunk, err := index.LookupBlock(&api.BlockMetadata{
		Tag:                candidate.Tag,
		Height:             candidate.Height,
		Hash:               candidate.Hash,
		ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		ByteOffset:         candidate.ByteOffset,
		ByteLength:         candidate.ByteLength,
		UncompressedLength: candidate.UncompressedLength,
	})
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to resolve pinned CSCB placement: %w", err)
	}
	chunkData, err := v.readChunk(ctx, candidate, index, chunk)
	if err != nil {
		return nil, nil, err
	}
	payload, err := cscb.ExtractBlockPayload(chunkData, block)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to extract pinned CSCB block payload: %w", err)
	}
	var parsed api.Block
	if err := proto.Unmarshal(payload, &parsed); err != nil {
		return nil, nil, xerrors.Errorf("failed to parse pinned CSCB block payload: %w", err)
	}
	return payload, &parsed, nil
}

func (v *payloadVerifier) readIndex(ctx context.Context, candidate Candidate) (*cscb.Index, error) {
	cacheKey := candidate.Bucket + "\x00" + candidate.ConsolidatedKey + "\x00" + candidate.CSCBVersionID
	if index, ok := v.indexes[cacheKey]; ok {
		return index, nil
	}
	index, err := readCSCBIndex(ctx, v.store, candidate.Bucket, candidate.ConsolidatedKey, candidate.CSCBVersionID)
	if err != nil {
		return nil, err
	}
	v.indexes[cacheKey] = index
	return index, nil
}

// readCSCBIndex reads and parses a pinned CSCB object's index envelope. It is
// shared by payload verification and by the planner's memory sizing so both
// observe exactly the same index bytes.
func readCSCBIndex(
	ctx context.Context,
	store ObjectStore,
	bucket string,
	key string,
	versionID string,
) (*cscb.Index, error) {
	first, err := store.ReadObjectVersionRange(ctx, bucket, key, versionID, 0, retirementInitialIndexReadSize)
	if err != nil {
		return nil, xerrors.Errorf("failed to read pinned CSCB index prefix: %w", err)
	}
	required, err := cscb.HeaderEnvelopeLength(first)
	if err != nil {
		return nil, err
	}
	data := first
	if required > uint64(len(first)) {
		remaining, err := store.ReadObjectVersionRange(
			ctx,
			bucket,
			key,
			versionID,
			uint64(len(first)),
			required-uint64(len(first)),
		)
		if err != nil {
			return nil, xerrors.Errorf("failed to read pinned CSCB index remainder: %w", err)
		}
		data = make([]byte, 0, required)
		data = append(data, first...)
		data = append(data, remaining...)
	}
	return cscb.ParseIndex(data)
}

// maxChunkUncompressedBytes returns the largest decompressed chunk the index
// describes. Chunk payloads are uneven and an immutable object may have been
// encoded under different chunk settings than the current configuration, so
// this — not an average derived from the object's total length — is the bound
// a verifier's chunk cache can actually reach.
func maxChunkUncompressedBytes(index *cscb.Index) uint64 {
	if index == nil {
		return 0
	}
	var max uint64
	for i := range index.Chunks {
		if index.Chunks[i].UncompressedLength > max {
			max = index.Chunks[i].UncompressedLength
		}
	}
	return max
}

func (v *payloadVerifier) readChunk(
	ctx context.Context,
	candidate Candidate,
	index *cscb.Index,
	chunk *cscb.ChunkDescriptor,
) ([]byte, error) {
	cacheKey := candidate.Bucket + "\x00" + candidate.ConsolidatedKey + "\x00" + candidate.CSCBVersionID + "\x00" + strconv.FormatUint(uint64(chunk.Index), 10)
	if cacheKey == v.lastChunkKey {
		return v.lastChunkData, nil
	}
	frame, err := v.store.ReadObjectVersionRange(
		ctx,
		candidate.Bucket,
		candidate.ConsolidatedKey,
		candidate.CSCBVersionID,
		chunk.CompressedPayloadOffset,
		chunk.CompressedLength,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to read pinned CSCB chunk: %w", err)
	}
	data, err := cscb.DecodeChunkFrame(bytes.NewReader(frame), index.Header.Codec)
	if err != nil {
		return nil, err
	}
	if err := cscb.ValidateChunkPayload(data, chunk); err != nil {
		return nil, err
	}
	v.lastChunkKey = cacheKey
	v.lastChunkData = data
	return data, nil
}
