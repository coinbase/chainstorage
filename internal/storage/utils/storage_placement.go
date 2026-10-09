package utils

import (
	"google.golang.org/protobuf/proto"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// CloneBlockWithoutStoragePlacement returns a block payload that does not retain
// the object location from which it was read. Storage placement is authoritative
// in meta storage and must not be embedded in an immutable consolidated object.
func CloneBlockWithoutStoragePlacement(block *api.Block) *api.Block {
	if block == nil {
		return nil
	}
	clone := proto.Clone(block).(*api.Block)
	if clone.Metadata == nil {
		return clone
	}
	clone.Metadata.ObjectKeyMain = ""
	clone.Metadata.ByteOffset = 0
	clone.Metadata.ByteLength = 0
	clone.Metadata.UncompressedLength = 0
	clone.Metadata.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK
	clone.Metadata.StorageGeneration = ""
	return clone
}

// HasBlockStoragePlacement reports whether a serialized block payload retains
// an object key or byte placement that belongs in meta storage.
func HasBlockStoragePlacement(block *api.Block) bool {
	if block == nil || block.Metadata == nil {
		return false
	}
	metadata := block.Metadata
	return metadata.ObjectKeyMain != "" ||
		metadata.ByteOffset != 0 ||
		metadata.ByteLength != 0 ||
		metadata.UncompressedLength != 0 ||
		metadata.ObjectFormat != api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK ||
		metadata.StorageGeneration != ""
}
