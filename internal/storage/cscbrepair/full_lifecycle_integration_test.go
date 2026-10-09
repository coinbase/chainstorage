package cscbrepair_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/coinbase/chainstorage/internal/config"
	chains3 "github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/cscbrepair"
	"github.com/coinbase/chainstorage/internal/storage/cscbrepairlock"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metapostgres "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	storageutils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestIntegrationCSCBRepairPendingRangeIsolation(t *testing.T) {
	if os.Getenv("TEST_TYPE") != "integration" {
		t.Skip("integration test")
	}
	require := require.New(t)
	ctx := context.Background()
	unique := time.Now().UTC().UnixNano()
	tag := uint32(1_600_000_000 + unique%100_000_000)
	startHeight := uint64(8_500_000_000 + unique%100_000_000)
	endHeight := startHeight + 1_000

	cfg, err := config.New(
		config.WithEnvironment(config.EnvLocal),
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_SOLANA),
		config.WithNetwork(common.Network_NETWORK_SOLANA_MAINNET),
	)
	require.NoError(err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	configureRepairTestEnvironment(t, cfg.AWS.Postgres)
	db, err := openRepairDB(ctx, cfg.AWS.Postgres)
	require.NoError(err)
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))

	type pendingRange struct {
		key   string
		start uint64
		end   uint64
	}
	insertPending := func(tag uint32, pending pendingRange) {
		_, err := db.ExecContext(ctx, `
			INSERT INTO cscb_repair_manifest (
				tag, state, bucket, old_consolidated_object_key_main,
				start_height, end_height, canonical_block_count, total_block_count, row_set_sha256
			) VALUES ($1, $2, $3, $4, $5, $6, 0, 0, $7)`,
			tag,
			cscbrepair.StatePreparing,
			"range-isolation-bucket",
			pending.key,
			pending.start,
			pending.end,
			repairSHA256(pending.key),
		)
		require.NoError(err)
	}
	inRangeKey := fmt.Sprintf("consolidated/in-range-%d.cscb.zstd", unique)
	pendingRanges := []pendingRange{
		{key: fmt.Sprintf("consolidated/below-%d.cscb.zstd", unique), start: startHeight - 1_000, end: startHeight},
		{key: fmt.Sprintf("consolidated/above-%d.cscb.zstd", unique), start: endHeight, end: endHeight + 1_000},
		{key: inRangeKey, start: startHeight, end: endHeight},
	}
	for _, pending := range pendingRanges {
		insertPending(tag, pending)
	}

	repository := cscbrepair.NewPostgresRepository(db)
	pending, err := repository.FindPending(ctx, tag, startHeight, endHeight)
	require.NoError(err)
	require.NotNil(pending)
	require.Equal(inRangeKey, pending.OldConsolidatedObjectKey)

	objectKeys, err := repository.ListCandidateObjectKeys(ctx, tag, startHeight, endHeight, 10)
	require.NoError(err)
	require.Equal([]string{inRangeKey}, objectKeys)

	emptyStart := endHeight + 2_000
	pending, err = repository.FindPending(ctx, tag, emptyStart, emptyStart+1_000)
	require.NoError(err)
	require.Nil(pending)
	objectKeys, err = repository.ListCandidateObjectKeys(ctx, tag, emptyStart, emptyStart+1_000, 10)
	require.NoError(err)
	require.Empty(objectKeys)

	crossingTag := tag + 1
	crossingKey := fmt.Sprintf("consolidated/cross-right-%d.cscb.zstd", unique)
	insertPending(crossingTag, pendingRange{
		key:   crossingKey,
		start: startHeight + 500,
		end:   endHeight + 1,
	})
	pending, err = repository.FindPending(ctx, crossingTag, startHeight, endHeight)
	require.NoError(err)
	require.NotNil(pending)
	require.Equal(crossingKey, pending.OldConsolidatedObjectKey)
	_, err = repository.ListCandidateObjectKeys(ctx, crossingTag, startHeight, endHeight, 10)
	require.ErrorContains(err, "exceeds approved range")
}

func TestIntegrationCSCBRepairFullLifecycle(t *testing.T) {
	if os.Getenv("TEST_TYPE") != "integration" {
		t.Skip("integration test")
	}
	require := require.New(t)
	ctx := context.Background()
	unique := time.Now().UTC().UnixNano()
	tag := uint32(1_700_000_000 + unique%100_000_000)
	height := uint64(9_500_000_000 + unique%100_000_000)
	bucket := fmt.Sprintf("chainstorage-cscb-repair-%d", unique)

	cfg, err := config.New(
		config.WithEnvironment(config.EnvLocal),
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_SOLANA),
		config.WithNetwork(common.Network_NETWORK_SOLANA_MAINNET),
	)
	require.NoError(err)
	if cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured")
	}
	cfg.StorageType.BlobStorageType = config.BlobStorageType_S3
	cfg.StorageType.MetaStorageType = config.MetaStorageType_POSTGRES
	cfg.AWS.Bucket = bucket
	cfg.AWS.IsLocalStack = true
	cfg.AWS.IsResetLocal = true
	localStackEndpoint := configureRepairTestEnvironment(t, cfg.AWS.Postgres)
	cfg.Chain.BlockStartHeight = height
	cfg.AWS.Storage.Consolidation.Enabled = true
	cfg.AWS.Storage.Consolidation.Mode = config.ConsolidationModeHistoricalBackfill

	db, err := openRepairDB(ctx, cfg.AWS.Postgres)
	require.NoError(err)
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))

	var (
		meta                metastorage.MetaStorage
		blob                blobstorage.BlobStorage
		singleBlockUploader blobstorage.SingleBlockUploader
		s3Session           *chains3.S3
	)
	app := testapp.New(
		t,
		testapp.WithIntegration(),
		testapp.WithConfig(cfg),
		chains3.Module,
		metastorage.Module,
		blobstorage.Module,
		fx.Replace(newRepairLocalStackConfig(ctx, localStackEndpoint)),
		fx.Populate(&meta, &blob, &singleBlockUploader, &s3Session),
	)
	defer app.Close()
	require.NotNil(meta)
	require.NotNil(blob)
	require.NotNil(singleBlockUploader)
	require.NotNil(s3Session)

	rawS3 := awss3.NewFromConfig(s3Session.Config)
	_, err = rawS3.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &awss3types.VersioningConfiguration{
			Status: awss3types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(err)
	defer cleanupRepairBucket(t, rawS3, bucket)
	store := retirement.NewS3ObjectStore(rawS3)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          tag,
			Height:       height,
			Hash:         fmt.Sprintf("repair-hash-%d", unique),
			ParentHash:   fmt.Sprintf("repair-parent-%d", unique),
			ParentHeight: height - 1,
			Timestamp:    timestamppb.New(time.Now().UTC().Truncate(time.Second)),
		},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{Header: []byte(`{"slot":9500000000,"transactions":["repair"]}`)},
		},
	}
	historicalBlock := proto.Clone(block).(*api.Block)
	historicalBlock.GetSolana().Header = []byte(`{"slot":9500000000,"transactions":["stale"]}`)
	singleBlockKey, err := singleBlockUploader.Upload(ctx, historicalBlock, api.Compression_GZIP)
	require.NoError(err)
	semanticDuplicate := proto.Clone(block).(*api.Block)
	semanticDuplicate.GetSolana().Header = []byte("{\n  \"transactions\": [\"repair\"],\n  \"slot\": 9500000000\n}")
	require.JSONEq(string(block.GetSolana().Header), string(semanticDuplicate.GetSolana().Header))
	require.NotEqual(string(historicalBlock.GetSolana().Header), string(semanticDuplicate.GetSolana().Header))
	duplicateSingleBlockKey, err := singleBlockUploader.Upload(ctx, semanticDuplicate, api.Compression_GZIP)
	require.NoError(err)
	require.Equal(singleBlockKey, duplicateSingleBlockKey)
	singleBlockTopology, err := store.ListObjectVersions(ctx, bucket, singleBlockKey)
	require.NoError(err)
	require.Len(singleBlockTopology.Versions, 2)
	require.Empty(singleBlockTopology.DeleteMarkers)
	var currentSingleBlockVersion, historicalSingleBlockVersion retirement.ObjectVersion
	for _, version := range singleBlockTopology.Versions {
		if version.IsLatest {
			currentSingleBlockVersion = version
		} else {
			historicalSingleBlockVersion = version
		}
	}
	require.NotEmpty(currentSingleBlockVersion.VersionID)
	require.NotEmpty(historicalSingleBlockVersion.VersionID)
	require.NotEqual(currentSingleBlockVersion.ETag, historicalSingleBlockVersion.ETag)
	require.NotEqual(currentSingleBlockVersion.Bytes, historicalSingleBlockVersion.Bytes)
	block.Metadata.ObjectKeyMain = singleBlockKey
	require.NoError(meta.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block.Metadata}, nil))

	records, err := meta.GetBlocksMissingConsolidationShadow(ctx, tag, height, height+1, 1)
	require.NoError(err)
	require.Len(records, 1)
	blockMetadataID := records[0].ID
	var repairID int64
	defer cleanupRepairMetadata(t, db, blockMetadataID, &repairID)

	downloaded, err := blob.Download(ctx, records[0].Metadata)
	require.NoError(err)
	require.True(proto.Equal(
		storageutils.CloneBlockWithoutStoragePlacement(semanticDuplicate),
		storageutils.CloneBlockWithoutStoragePlacement(downloaded),
	))
	dirtyBlock := proto.Clone(historicalBlock).(*api.Block)
	dirtyBlock.Metadata.ObjectKeyMain = singleBlockKey
	dirtyBlock.Metadata.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK
	require.True(storageutils.HasBlockStoragePlacement(dirtyBlock))
	require.False(proto.Equal(
		storageutils.CloneBlockWithoutStoragePlacement(semanticDuplicate),
		storageutils.CloneBlockWithoutStoragePlacement(dirtyBlock),
	))
	dirtyPayload, err := proto.Marshal(dirtyBlock)
	require.NoError(err)
	dirtyKey, dirtyPlacements, err := blob.UploadConsolidated(ctx, []blobstorage.ConsolidatedBlockPayload{{
		Metadata:           records[0].Metadata,
		MetadataID:         blockMetadataID,
		RawBlockPayload:    blobstorage.BytesPayloadSource(dirtyPayload),
		UncompressedLength: uint64(len(dirtyPayload)),
	}})
	require.NoError(err)
	require.Len(dirtyPlacements, 1)
	dirtyPlacement := dirtyPlacements[0]
	require.NoError(meta.PersistBlockConsolidationShadows(ctx, []*metastorage.ConsolidationShadowPlacement{{
		BlockMetadataID:           blockMetadataID,
		Tag:                       tag,
		Height:                    height,
		Hash:                      block.Metadata.Hash,
		SingleBlockObjectKeyMain:  singleBlockKey,
		ConsolidatedObjectKeyMain: dirtyKey,
		ObjectFormat:              dirtyPlacement.ObjectFormat,
		ByteOffset:                dirtyPlacement.ByteOffset,
		ByteLength:                dirtyPlacement.ByteLength,
		UncompressedLength:        dirtyPlacement.UncompressedLength,
	}}))
	promotion, err := meta.PromoteBlockConsolidationShadows(ctx, tag, height, height+1, 1, 72*time.Hour)
	require.NoError(err)
	require.Equal(uint64(1), promotion.Blocks)

	repository := cscbrepair.NewPostgresRepository(db)
	candidateLockTx, err := db.BeginTx(ctx, nil)
	require.NoError(err)
	require.NoError(cscbrepairlock.AcquireTag(ctx, candidateLockTx, tag))
	listCtx, cancelList := context.WithTimeout(ctx, 2*time.Second)
	candidateKeys, err := repository.ListCandidateObjectKeys(listCtx, tag, height, height+1, 10)
	cancelList()
	require.NoError(err)
	require.Equal([]string{dirtyKey}, candidateKeys)
	require.NoError(candidateLockTx.Rollback(), "read-only discovery must not wait for the placement-writer lock")
	exactCandidate, err := repository.FindCandidateByObjectKey(ctx, tag, dirtyKey)
	require.NoError(err)
	require.Equal(dirtyKey, exactCandidate.OldConsolidatedObjectKey)

	repairer := cscbrepair.NewRepairer(repository, store, bucket)
	executionKey := repairSHA256(fmt.Sprintf("repair-main-%d", unique))

	// A writer that acquired the upload guard before repair preparation must
	// finish its S3 PUT before the repair can fence and pin the current version.
	preFenceGuard, err := meta.AcquireSingleBlockUploadGuard(ctx, tag, height, block.Metadata.Hash)
	require.NoError(err)
	require.False(preFenceGuard.RetirementFenced())
	oldCurrentSingleBlockVersion := currentSingleBlockVersion
	guardedPayload, err := store.ReadObjectVersion(ctx, bucket, singleBlockKey, oldCurrentSingleBlockVersion.VersionID)
	require.NoError(err)
	type prepareResult struct {
		manifest *cscbrepair.Manifest
		err      error
	}
	prepared := make(chan prepareResult, 1)
	go func() {
		preparedManifest, prepareErr := repairer.PrepareObject(
			ctx,
			executionKey,
			tag,
			height,
			height+1,
			1,
			dirtyKey,
			nil,
		)
		prepared <- prepareResult{manifest: preparedManifest, err: prepareErr}
	}()
	select {
	case result := <-prepared:
		require.Failf("repair preparation bypassed in-flight upload guard", "result=%+v", result)
	case <-time.After(100 * time.Millisecond):
	}
	guardedPut, err := rawS3.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(singleBlockKey),
		Body:   bytes.NewReader(guardedPayload),
	})
	require.NoError(err)
	require.NotNil(guardedPut.VersionId)
	require.NoError(preFenceGuard.Release())
	preparedResult := <-prepared
	require.NoError(preparedResult.err)
	manifest := preparedResult.manifest
	require.NotNil(manifest)
	repairID = manifest.ID
	postWriterTopology, err := store.ListObjectVersions(ctx, bucket, singleBlockKey)
	require.NoError(err)
	for _, version := range postWriterTopology.Versions {
		if version.VersionID == aws.ToString(guardedPut.VersionId) {
			currentSingleBlockVersion = version
		}
	}
	require.Equal(aws.ToString(guardedPut.VersionId), currentSingleBlockVersion.VersionID)
	_, err = rawS3.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(singleBlockKey),
		VersionId: aws.String(oldCurrentSingleBlockVersion.VersionID),
	})
	require.NoError(err)
	require.Equal(cscbrepair.StatePrepared, manifest.State)
	require.Equal(dirtyKey, manifest.OldConsolidatedObjectKey)
	require.Len(manifest.Blocks, 1)
	require.Equal(currentSingleBlockVersion.VersionID, manifest.Blocks[0].SingleBlockObjectVersion.VersionID)
	require.Len(manifest.Blocks[0].PayloadSHA256, 64)
	pendingKeys, err := repository.ListCandidateObjectKeys(ctx, tag, height, height+1, 10)
	require.NoError(err)
	require.Equal([]string{dirtyKey}, pendingKeys)

	// The trigger must serialize any reference to the pinned key, even when a
	// writer changes only object_format to a non-CSCB value. This closes the
	// final-reference-check race independently of the writer's claimed format.
	objectLockTx, err := db.BeginTx(ctx, nil)
	require.NoError(err)
	_, err = objectLockTx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 1))`, dirtyKey)
	require.NoError(err)
	blockedWrite := make(chan error, 1)
	writeCtx, cancelWrite := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWrite()
	go func() {
		_, writeErr := db.ExecContext(writeCtx, `
			UPDATE block_metadata
			SET object_format = $2
			WHERE id = $1`, blockMetadataID, api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK)
		blockedWrite <- writeErr
	}()
	select {
	case writeErr := <-blockedWrite:
		require.Failf("format-only old-key writer bypassed advisory lock", "error=%v", writeErr)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(objectLockTx.Commit())
	require.ErrorContains(<-blockedWrite, "cannot reference a pinned old CSCB object")

	uploadGuard, err := meta.AcquireSingleBlockUploadGuard(ctx, tag, height, block.Metadata.Hash)
	require.NoError(err)
	require.True(uploadGuard.RetirementFenced(), "active repair must fence single-block uploads")
	require.NoError(uploadGuard.Release())
	replayedSingleBlock := proto.Clone(block.Metadata).(*api.BlockMetadata)
	replayedSingleBlock.ObjectKeyMain = singleBlockKey
	replayedSingleBlock.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK
	replayedSingleBlock.ByteOffset = 0
	replayedSingleBlock.ByteLength = 0
	replayedSingleBlock.UncompressedLength = 0
	require.NoError(meta.PersistBlockMetas(ctx, false, []*api.BlockMetadata{replayedSingleBlock}, nil))
	pinnedActive, err := meta.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(dirtyKey, pinnedActive.ObjectKeyMain, "late metadata persistence must not strand a prepared repair")
	require.Equal(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH, pinnedActive.ObjectFormat)
	require.Equal(manifest.Blocks[0].OldByteOffset, pinnedActive.ByteOffset)
	require.Equal(manifest.Blocks[0].OldByteLength, pinnedActive.ByteLength)
	require.Equal(manifest.Blocks[0].OldUncompressedLength, pinnedActive.UncompressedLength)
	_, err = db.ExecContext(ctx, `
		UPDATE block_consolidation_shadow
		SET single_block_delete_after = clock_timestamp() - INTERVAL '1 second'
		WHERE block_metadata_id = $1`, blockMetadataID)
	require.NoError(err)
	err = retirement.NewPostgresRepository(db).PrepareRetirement(ctx, retirement.RetirementManifest{
		BlockMetadataID:                blockMetadataID,
		Tag:                            tag,
		Height:                         height,
		Hash:                           block.Metadata.Hash,
		State:                          retirement.RetirementStateEligible,
		Bucket:                         bucket,
		SingleBlockObjectKey:           singleBlockKey,
		SingleBlockObjectKeySHA256:     repairSHA256(singleBlockKey),
		SingleBlockObjectVersionIDs:    []string{manifest.Blocks[0].SingleBlockObjectVersion.VersionID},
		SingleBlockObjectETag:          manifest.Blocks[0].SingleBlockObjectVersion.ETag,
		SingleBlockObjectBytes:         manifest.Blocks[0].SingleBlockObjectVersion.Bytes,
		ConsolidatedObjectKey:          dirtyKey,
		ConsolidatedObjectVersionID:    manifest.OldConsolidatedObjectVersion.VersionID,
		ConsolidatedObjectETag:         manifest.OldConsolidatedObjectVersion.ETag,
		ConsolidatedByteOffset:         manifest.Blocks[0].OldByteOffset,
		ConsolidatedByteLength:         manifest.Blocks[0].OldByteLength,
		ConsolidatedUncompressedLength: manifest.Blocks[0].OldUncompressedLength,
		PayloadSHA256:                  manifest.Blocks[0].PayloadSHA256,
		PreparedAt:                     time.Now().UTC(),
	}, "")
	require.ErrorContains(err, "failed to lock canonical retirement metadata")

	activeBeforeRebuild, err := meta.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(dirtyKey, activeBeforeRebuild.ObjectKeyMain)
	require.Equal(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH, activeBeforeRebuild.ObjectFormat)

	var pinnedPayloads []cscbrepair.PinnedPayload
	require.NoError(repairer.VisitPinnedPayloads(ctx, manifest.ID, nil, func(payload cscbrepair.PinnedPayload) error {
		pinnedPayloads = append(pinnedPayloads, payload)
		return nil
	}))
	require.Len(pinnedPayloads, 1)
	var pinnedBlock api.Block
	require.NoError(proto.Unmarshal(pinnedPayloads[0].RawBlockPayload, &pinnedBlock))
	require.False(storageutils.HasBlockStoragePlacement(&pinnedBlock))
	require.True(proto.Equal(
		storageutils.CloneBlockWithoutStoragePlacement(semanticDuplicate),
		storageutils.CloneBlockWithoutStoragePlacement(&pinnedBlock),
	))
	cleanPayload := pinnedPayloads[0].RawBlockPayload
	cleanKey, cleanPlacements, err := blob.UploadConsolidated(ctx, []blobstorage.ConsolidatedBlockPayload{{
		Metadata:           pinnedPayloads[0].Metadata,
		MetadataID:         blockMetadataID,
		RawBlockPayload:    blobstorage.BytesPayloadSource(cleanPayload),
		UncompressedLength: uint64(len(cleanPayload)),
	}})
	require.NoError(err)
	require.NotEqual(dirtyKey, cleanKey)
	require.Len(cleanPlacements, 1)
	cleanPlacement := cleanPlacements[0]
	activeBeforePromotion, err := meta.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(dirtyKey, activeBeforePromotion.ObjectKeyMain, "source CSCB must remain active until atomic promotion")

	manifest, err = repairer.VerifyAndPromote(ctx, manifest.ID, cleanKey, []cscbrepair.RebuiltPlacement{{
		BlockMetadataID:    cleanPlacement.MetadataID,
		Height:             cleanPlacement.Height,
		Hash:               cleanPlacement.Hash,
		ObjectFormat:       cleanPlacement.ObjectFormat,
		ByteOffset:         cleanPlacement.ByteOffset,
		ByteLength:         cleanPlacement.ByteLength,
		UncompressedLength: cleanPlacement.UncompressedLength,
	}}, 72*time.Hour, nil)
	require.NoError(err)
	require.Equal(cscbrepair.StateCompleted, manifest.State)
	require.Equal(cleanKey, manifest.NewConsolidatedObjectKey)
	require.NotZero(manifest.NewConsolidatedObjectVersion.Bytes)
	require.NotNil(manifest.RestoredAt)
	require.NotNil(manifest.VerifiedAt)
	require.NotNil(manifest.CompletedAt)
	require.Equal("old_consolidated_object_retained_unreferenced", manifest.Outcome)
	var retentionStartedAt, singleBlockDeleteAfter time.Time
	require.NoError(db.QueryRowContext(ctx, `
		SELECT single_block_retention_started_at, single_block_delete_after
		FROM block_consolidation_shadow
		WHERE block_metadata_id = $1`, blockMetadataID).Scan(&retentionStartedAt, &singleBlockDeleteAfter))
	require.False(retentionStartedAt.Before(*manifest.RestoredAt))
	require.WithinDuration(retentionStartedAt.Add(73*time.Hour), singleBlockDeleteAfter, time.Microsecond)
	activeAfterPromotion, err := meta.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(cleanKey, activeAfterPromotion.ObjectKeyMain)
	require.Equal(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH, activeAfterPromotion.ObjectFormat)
	pendingAfterPromotion, err := repository.FindPending(ctx, tag, height, height+1)
	require.NoError(err)
	require.Nil(pendingAfterPromotion, "rollback must not resume a mixed placement after atomic promotion")
	candidatesAfterPromotion, err := repository.ListCandidateObjectKeys(ctx, tag, height, height+1, 10)
	require.NoError(err)
	require.Empty(candidatesAfterPromotion, "rerunning a completed repair range must terminate without new work")

	manifest, err = repairer.Complete(ctx, manifest.ID, nil)
	require.NoError(err)
	require.Equal(cscbrepair.StateCompleted, manifest.State)
	require.NotNil(manifest.CompletedAt)
	require.Equal("old_consolidated_object_retained_unreferenced", manifest.Outcome)
	requireOldCSCBUnreferenced(t, db, dirtyKey)
	dirtyTopology, err := store.ListObjectVersions(ctx, bucket, dirtyKey)
	require.NoError(err)
	require.Len(dirtyTopology.Versions, 1)
	require.Empty(dirtyTopology.DeleteMarkers)
	require.Equal(manifest.OldConsolidatedObjectVersion.VersionID, dirtyTopology.Versions[0].VersionID)
	cleanTopology, err := store.ListObjectVersions(ctx, bucket, cleanKey)
	require.NoError(err)
	require.Len(cleanTopology.Versions, 1)
	rawClean, err := retirement.NewPinnedPayloadVerifier(store).InspectConsolidated(ctx, retirement.Candidate{
		Bucket:             bucket,
		ConsolidatedKey:    cleanKey,
		CSCBVersionID:      cleanTopology.Versions[0].VersionID,
		Tag:                tag,
		Height:             height,
		Hash:               block.Metadata.Hash,
		ByteOffset:         cleanPlacement.ByteOffset,
		ByteLength:         cleanPlacement.ByteLength,
		UncompressedLength: cleanPlacement.UncompressedLength,
	})
	require.NoError(err)
	require.False(rawClean.HasStoragePlacement)
	singleBlockTopology, err = store.ListObjectVersions(ctx, bucket, singleBlockKey)
	require.NoError(err)
	require.Len(singleBlockTopology.Versions, 2)
	_, err = rawS3.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(singleBlockKey),
		VersionId: aws.String(historicalSingleBlockVersion.VersionID),
	})
	require.NoError(err)
	singleBlockTopology, err = store.ListObjectVersions(ctx, bucket, singleBlockKey)
	require.NoError(err)
	require.Len(singleBlockTopology.Versions, 1)
	require.Equal(currentSingleBlockVersion.VersionID, singleBlockTopology.Versions[0].VersionID)

	activeClean, err := meta.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(cleanKey, activeClean.ObjectKeyMain)
	readClean, err := blob.Download(ctx, activeClean)
	require.NoError(err)
	// Normal downloads overlay the active database placement after parsing; raw
	// CSCB neutrality is asserted independently above.
	require.True(storageutils.HasBlockStoragePlacement(readClean))
	require.True(proto.Equal(
		storageutils.CloneBlockWithoutStoragePlacement(semanticDuplicate),
		storageutils.CloneBlockWithoutStoragePlacement(readClean),
	))

	resumed, err := repairer.Complete(ctx, manifest.ID, nil)
	require.NoError(err)
	require.Equal(cscbrepair.StateCompleted, resumed.State)
	retried, err := repairer.PrepareNext(ctx, executionKey, tag, height, height+1, 1, nil)
	require.NoError(err)
	require.Equal(manifest.ID, retried.ID)
	require.Equal(cscbrepair.StateCompleted, retried.State)
	concurrentExecutionKey := repairSHA256(fmt.Sprintf("repair-concurrent-%d", unique))
	completedByObject, err := repairer.PrepareObject(
		ctx,
		concurrentExecutionKey,
		tag,
		height,
		height+1,
		1,
		dirtyKey,
		nil,
	)
	require.NoError(err)
	require.Equal(manifest.ID, completedByObject.ID)
	require.Equal(cscbrepair.StateCompleted, completedByObject.State)

	// Exercise the real retirement executor after repair completion. The S3
	// object deletion, shadow cleanup, retirement manifest, and repair audit
	// path scrub must commit as one recoverable lifecycle.
	_, err = db.ExecContext(ctx, `
		UPDATE block_consolidation_shadow
		SET single_block_retention_started_at = clock_timestamp() - INTERVAL '96 hours',
			single_block_delete_after = clock_timestamp() - INTERVAL '24 hours'
		WHERE block_metadata_id = $1`, blockMetadataID)
	require.NoError(err)
	policy := repairRetentionSafeBucketPolicy(bucket)
	_, err = rawS3.PutBucketPolicy(ctx, &awss3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(policy)})
	require.NoError(err)
	retirementPlanner := retirement.NewPlanner(retirement.NewPostgresRepository(db), store)
	retirementRequest := retirement.PlanRequest{
		Environment:                 "local",
		Blockchain:                  "solana",
		Network:                     "mainnet",
		Bucket:                      bucket,
		Tag:                         tag,
		StartHeight:                 height,
		EndHeight:                   height + 1,
		Limit:                       1,
		Now:                         time.Now().UTC(),
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		Approval: retirement.Approval{
			Chain:       "solana-mainnet",
			StartHeight: height,
			EndHeight:   height + 1,
		},
	}
	retirementReport, err := retirementPlanner.Plan(ctx, retirementRequest)
	require.NoError(err)
	require.Len(retirementReport.Items, 1)
	require.Equal(retirement.ActionDeleteObjectVersion, retirementReport.Items[0].Action)
	require.Empty(retirementReport.Items[0].SkipReason)
	safety, err := store.InspectObjectRetentionSafety(ctx, bucket, cleanKey)
	require.NoError(err)
	_, err = db.ExecContext(ctx, `
		INSERT INTO cscb_retirement_safety_observation (
			bucket, consolidated_object_key_main, configuration_sha256,
			first_observed_at, last_observed_at
		) VALUES ($1, $2, $3, clock_timestamp() - INTERVAL '16 minutes', clock_timestamp() - INTERVAL '16 minutes')
		ON CONFLICT (bucket, consolidated_object_key_main) DO UPDATE SET
			configuration_sha256 = EXCLUDED.configuration_sha256,
			first_observed_at = EXCLUDED.first_observed_at,
			last_observed_at = EXCLUDED.last_observed_at`, bucket, cleanKey, safety.ConfigurationSHA256)
	require.NoError(err)
	require.NoError(retirementPlanner.Apply(ctx, retirementRequest, retirementReport))
	require.Equal(retirement.ActionDeletedVerified, retirementReport.Items[0].Action)

	retiredSingleTopology, err := store.ListObjectVersions(ctx, bucket, singleBlockKey)
	require.NoError(err)
	require.Empty(retiredSingleTopology.Versions)
	require.Empty(retiredSingleTopology.DeleteMarkers)
	var repairSinglePath sql.NullString
	var repairSinglePathHash string
	require.NoError(db.QueryRowContext(ctx, `
		SELECT single_block_object_key_main, single_block_object_key_sha256
		FROM cscb_repair_block
		WHERE repair_id = $1 AND block_metadata_id = $2`, manifest.ID, blockMetadataID).Scan(
		&repairSinglePath,
		&repairSinglePathHash,
	))
	require.False(repairSinglePath.Valid)
	require.Equal(repairSHA256(singleBlockKey), repairSinglePathHash)
	activeAfterRetirement, err := meta.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(cleanKey, activeAfterRetirement.ObjectKeyMain)
	readAfterRetirement, err := blob.Download(ctx, activeAfterRetirement)
	require.NoError(err)
	require.True(proto.Equal(
		storageutils.CloneBlockWithoutStoragePlacement(semanticDuplicate),
		storageutils.CloneBlockWithoutStoragePlacement(readAfterRetirement),
	))

	// A CSCB whose rows all became non-canonical must still be restored and
	// audited. It does not produce a replacement CSCB because normal
	// consolidation intentionally processes canonical rows only.
	nonCanonicalHeight := height + 1
	nonCanonicalBlock := proto.Clone(block).(*api.Block)
	nonCanonicalBlock.Metadata = proto.Clone(block.Metadata).(*api.BlockMetadata)
	nonCanonicalBlock.Metadata.Height = nonCanonicalHeight
	nonCanonicalBlock.Metadata.ParentHeight = height
	nonCanonicalBlock.Metadata.Hash = fmt.Sprintf("repair-noncanonical-hash-%d", unique)
	nonCanonicalBlock.Metadata.ParentHash = block.Metadata.Hash
	nonCanonicalSingleKey, err := singleBlockUploader.Upload(ctx, nonCanonicalBlock, api.Compression_GZIP)
	require.NoError(err)
	nonCanonicalBlock.Metadata.ObjectKeyMain = nonCanonicalSingleKey
	require.NoError(meta.PersistBlockMetas(ctx, true, []*api.BlockMetadata{nonCanonicalBlock.Metadata}, nil))
	nonCanonicalRecords, err := meta.GetBlocksMissingConsolidationShadow(ctx, tag, nonCanonicalHeight, nonCanonicalHeight+1, 1)
	require.NoError(err)
	require.Len(nonCanonicalRecords, 1)
	nonCanonicalMetadataID := nonCanonicalRecords[0].ID
	var nonCanonicalRepairID int64
	defer cleanupRepairMetadata(t, db, nonCanonicalMetadataID, &nonCanonicalRepairID)

	nonCanonicalDownloaded, err := blob.Download(ctx, nonCanonicalRecords[0].Metadata)
	require.NoError(err)
	nonCanonicalDirty := proto.Clone(nonCanonicalDownloaded).(*api.Block)
	nonCanonicalDirty.Metadata.ObjectKeyMain = nonCanonicalSingleKey
	nonCanonicalDirty.Metadata.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK
	nonCanonicalPayload, err := proto.Marshal(nonCanonicalDirty)
	require.NoError(err)
	nonCanonicalDirtyKey, nonCanonicalPlacements, err := blob.UploadConsolidated(ctx, []blobstorage.ConsolidatedBlockPayload{{
		Metadata:           nonCanonicalRecords[0].Metadata,
		MetadataID:         nonCanonicalMetadataID,
		RawBlockPayload:    blobstorage.BytesPayloadSource(nonCanonicalPayload),
		UncompressedLength: uint64(len(nonCanonicalPayload)),
	}})
	require.NoError(err)
	require.Len(nonCanonicalPlacements, 1)
	nonCanonicalPlacement := nonCanonicalPlacements[0]
	require.NoError(meta.PersistBlockConsolidationShadows(ctx, []*metastorage.ConsolidationShadowPlacement{{
		BlockMetadataID:           nonCanonicalMetadataID,
		Tag:                       tag,
		Height:                    nonCanonicalHeight,
		Hash:                      nonCanonicalBlock.Metadata.Hash,
		SingleBlockObjectKeyMain:  nonCanonicalSingleKey,
		ConsolidatedObjectKeyMain: nonCanonicalDirtyKey,
		ObjectFormat:              nonCanonicalPlacement.ObjectFormat,
		ByteOffset:                nonCanonicalPlacement.ByteOffset,
		ByteLength:                nonCanonicalPlacement.ByteLength,
		UncompressedLength:        nonCanonicalPlacement.UncompressedLength,
	}}))
	promotion, err = meta.PromoteBlockConsolidationShadows(ctx, tag, nonCanonicalHeight, nonCanonicalHeight+1, 1, 72*time.Hour)
	require.NoError(err)
	require.Equal(uint64(1), promotion.Blocks)
	replacementBlock := proto.Clone(nonCanonicalBlock).(*api.Block)
	replacementBlock.Metadata = proto.Clone(nonCanonicalBlock.Metadata).(*api.BlockMetadata)
	replacementBlock.Metadata.Hash = fmt.Sprintf("repair-replacement-hash-%d", unique)
	replacementBlock.Metadata.ParentHash = block.Metadata.Hash
	replacementSingleKey, err := singleBlockUploader.Upload(ctx, replacementBlock, api.Compression_GZIP)
	require.NoError(err)
	replacementBlock.Metadata.ObjectKeyMain = replacementSingleKey
	require.NoError(meta.PersistBlockMetas(ctx, true, []*api.BlockMetadata{replacementBlock.Metadata}, nil))
	var replacementMetadataID int64
	err = db.QueryRowContext(ctx, `
		SELECT id FROM block_metadata
		WHERE tag = $1 AND height = $2 AND hash = $3 AND skipped = FALSE`,
		tag,
		nonCanonicalHeight,
		replacementBlock.Metadata.Hash,
	).Scan(&replacementMetadataID)
	require.NoError(err)
	defer cleanupRepairMetadata(t, db, replacementMetadataID, nil)

	nonCanonicalExecutionKey := repairSHA256(fmt.Sprintf("repair-noncanonical-%d", unique))
	nonCanonicalManifest, err := repairer.PrepareNext(
		ctx,
		nonCanonicalExecutionKey,
		tag,
		nonCanonicalHeight,
		nonCanonicalHeight+1,
		1,
		nil,
	)
	require.NoError(err)
	require.NotNil(nonCanonicalManifest)
	nonCanonicalRepairID = nonCanonicalManifest.ID
	require.Zero(nonCanonicalManifest.CanonicalBlockCount)
	require.Equal(uint64(1), nonCanonicalManifest.TotalBlockCount)
	nonCanonicalManifest, err = repairer.Restore(ctx, nonCanonicalManifest.ID, nil)
	require.NoError(err)
	require.Equal(cscbrepair.StateRestored, nonCanonicalManifest.State)
	activeNonCanonical, err := meta.GetBlockByHash(ctx, tag, nonCanonicalHeight, nonCanonicalBlock.Metadata.Hash)
	require.NoError(err)
	require.Equal(nonCanonicalSingleKey, activeNonCanonical.ObjectKeyMain)
	require.Equal(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK, activeNonCanonical.ObjectFormat)
	readNonCanonical, err := blob.Download(ctx, activeNonCanonical)
	require.NoError(err)
	require.True(proto.Equal(
		storageutils.CloneBlockWithoutStoragePlacement(nonCanonicalBlock),
		storageutils.CloneBlockWithoutStoragePlacement(readNonCanonical),
	))
	nonCanonicalManifest, err = repairer.Complete(ctx, nonCanonicalManifest.ID, nil)
	require.NoError(err)
	require.Equal(cscbrepair.StateCompleted, nonCanonicalManifest.State)
	require.Empty(nonCanonicalManifest.NewConsolidatedObjectKey)
	requireOldCSCBUnreferenced(t, db, nonCanonicalDirtyKey)
	nonCanonicalDirtyTopology, err := store.ListObjectVersions(ctx, bucket, nonCanonicalDirtyKey)
	require.NoError(err)
	require.Len(nonCanonicalDirtyTopology.Versions, 1)
	require.Empty(nonCanonicalDirtyTopology.DeleteMarkers)

	shadowOnlyKey := fmt.Sprintf("BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/consolidated/v=2/shadow-only-%d.cscb.zstd", unique)
	tagLockTx, err := db.BeginTx(ctx, nil)
	require.NoError(err)
	_, err = tagLockTx.ExecContext(
		ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 2))`,
		fmt.Sprintf("cscb_repair_tag/%d", tag),
	)
	require.NoError(err)
	shadowWrite := make(chan error, 1)
	go func() {
		shadowWrite <- meta.PersistBlockConsolidationShadows(ctx, []*metastorage.ConsolidationShadowPlacement{{
			BlockMetadataID:           nonCanonicalMetadataID,
			Tag:                       tag,
			Height:                    nonCanonicalHeight,
			Hash:                      nonCanonicalBlock.Metadata.Hash,
			SingleBlockObjectKeyMain:  nonCanonicalSingleKey,
			ConsolidatedObjectKeyMain: shadowOnlyKey,
			ObjectFormat:              api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:                nonCanonicalPlacement.ByteOffset,
			ByteLength:                nonCanonicalPlacement.ByteLength,
			UncompressedLength:        nonCanonicalPlacement.UncompressedLength,
		}})
	}()
	select {
	case writeErr := <-shadowWrite:
		require.Failf("consolidated shadow writer bypassed repair tag lock", "error=%v", writeErr)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(tagLockTx.Commit())
	require.NoError(<-shadowWrite)
	shadowOnlyExecutionKey := repairSHA256(fmt.Sprintf("repair-shadow-only-%d", unique))
	_, err = repairer.PrepareNext(
		ctx,
		shadowOnlyExecutionKey,
		tag,
		nonCanonicalHeight,
		nonCanonicalHeight+1,
		1,
		nil,
	)
	require.ErrorContains(err, "shadow-only consolidated object")
	_, err = db.ExecContext(ctx, `DELETE FROM block_consolidation_shadow WHERE block_metadata_id = $1`, nonCanonicalMetadataID)
	require.NoError(err)

	// Application writers acquire the tag lock before touching metadata rows.
	// Holding the tag lock must block persistence while leaving the row lock free.
	tagLockTx, err = db.BeginTx(ctx, nil)
	require.NoError(err)
	_, err = tagLockTx.ExecContext(
		ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 2))`,
		fmt.Sprintf("cscb_repair_tag/%d", tag),
	)
	require.NoError(err)
	metadataWrite := make(chan error, 1)
	go func() {
		metadataWrite <- meta.PersistBlockMetas(ctx, false, []*api.BlockMetadata{proto.Clone(activeClean).(*api.BlockMetadata)}, nil)
	}()
	select {
	case writeErr := <-metadataWrite:
		require.Failf("metadata writer bypassed repair tag lock", "error=%v", writeErr)
	case <-time.After(100 * time.Millisecond):
	}
	var lockedMetadataID int64
	require.NoError(tagLockTx.QueryRowContext(
		ctx,
		`SELECT id FROM block_metadata WHERE id = $1 FOR UPDATE NOWAIT`,
		blockMetadataID,
	).Scan(&lockedMetadataID))
	require.Equal(blockMetadataID, lockedMetadataID)
	require.NoError(tagLockTx.Commit())
	require.NoError(<-metadataWrite)

	noCandidateExecutionKey := repairSHA256(fmt.Sprintf("repair-no-candidate-%d", unique))
	defer cleanupRepairExecution(t, db, noCandidateExecutionKey)
	tagLockTx, err = db.BeginTx(ctx, nil)
	require.NoError(err)
	_, err = tagLockTx.ExecContext(
		ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 2))`,
		fmt.Sprintf("cscb_repair_tag/%d", tag),
	)
	require.NoError(err)
	type candidateResult struct {
		manifest *cscbrepair.Manifest
		err      error
	}
	noCandidateResult := make(chan candidateResult, 1)
	go func() {
		next, prepareErr := repairer.PrepareNext(
			ctx,
			noCandidateExecutionKey,
			tag,
			height,
			height+1,
			1,
			nil,
		)
		noCandidateResult <- candidateResult{manifest: next, err: prepareErr}
	}()
	select {
	case result := <-noCandidateResult:
		require.Failf("terminal no-candidate binding bypassed repair tag lock", "manifest=%v error=%v", result.manifest, result.err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(tagLockTx.Commit())
	result := <-noCandidateResult
	require.NoError(result.err)
	require.Nil(result.manifest)
	next, err := repairer.PrepareNext(ctx, noCandidateExecutionKey, tag, height, height+1, 1, nil)
	require.NoError(err)
	require.Nil(next)

	// Skipped canonical slots inside a CSCB's height span have no payload row.
	// They must not be mistaken for unrelated canonical block data during restore.
	gapStartHeight := height + 100
	gapHeights := []uint64{gapStartHeight, gapStartHeight + 2}
	gapBlocks := make([]*api.Block, 0, len(gapHeights))
	gapSingleBlockKeys := make([]string, 0, len(gapHeights))
	for i, gapHeight := range gapHeights {
		gapBlock := storageutils.CloneBlockWithoutStoragePlacement(block)
		gapBlock.Metadata.Height = gapHeight
		gapBlock.Metadata.Hash = fmt.Sprintf("repair-gap-hash-%d-%d", unique, gapHeight)
		if i == 0 {
			gapBlock.Metadata.ParentHeight = gapHeight - 1
			gapBlock.Metadata.ParentHash = fmt.Sprintf("repair-gap-parent-%d", unique)
		} else {
			gapBlock.Metadata.ParentHeight = gapHeights[i-1]
			gapBlock.Metadata.ParentHash = gapBlocks[i-1].Metadata.Hash
		}
		gapSingleBlockKey, uploadErr := singleBlockUploader.Upload(ctx, gapBlock, api.Compression_GZIP)
		require.NoError(uploadErr)
		gapBlock.Metadata.ObjectKeyMain = gapSingleBlockKey
		gapBlocks = append(gapBlocks, gapBlock)
		gapSingleBlockKeys = append(gapSingleBlockKeys, gapSingleBlockKey)
	}
	skippedMetadata := &api.BlockMetadata{
		Tag:     tag,
		Height:  gapStartHeight + 1,
		Skipped: true,
	}
	require.NoError(meta.PersistBlockMetas(ctx, true, []*api.BlockMetadata{
		gapBlocks[0].Metadata,
		skippedMetadata,
		gapBlocks[1].Metadata,
	}, nil))
	gapRecords, err := meta.GetBlocksMissingConsolidationShadow(ctx, tag, gapStartHeight, gapStartHeight+3, 3)
	require.NoError(err)
	require.Len(gapRecords, 2)
	var skippedMetadataID int64
	require.NoError(db.QueryRowContext(ctx, `
		SELECT id FROM block_metadata
		WHERE tag = $1 AND height = $2 AND skipped = TRUE`,
		tag,
		skippedMetadata.Height,
	).Scan(&skippedMetadataID))
	var gapRepairID int64
	defer cleanupRepairMetadata(t, db, gapRecords[0].ID, nil)
	defer cleanupRepairMetadata(t, db, gapRecords[1].ID, nil)
	defer cleanupRepairMetadata(t, db, skippedMetadataID, nil)
	defer cleanupRepairMetadata(t, db, 0, &gapRepairID)

	gapPayloads := make([]blobstorage.ConsolidatedBlockPayload, 0, len(gapRecords))
	for i, record := range gapRecords {
		dirtyGapBlock := proto.Clone(gapBlocks[i]).(*api.Block)
		dirtyGapBlock.Metadata.ObjectKeyMain = gapSingleBlockKeys[i]
		dirtyGapBlock.Metadata.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK
		require.True(storageutils.HasBlockStoragePlacement(dirtyGapBlock))
		dirtyGapPayload, marshalErr := proto.Marshal(dirtyGapBlock)
		require.NoError(marshalErr)
		gapPayloads = append(gapPayloads, blobstorage.ConsolidatedBlockPayload{
			Metadata:           record.Metadata,
			MetadataID:         record.ID,
			RawBlockPayload:    blobstorage.BytesPayloadSource(dirtyGapPayload),
			UncompressedLength: uint64(len(dirtyGapPayload)),
		})
	}
	gapDirtyKey, gapPlacements, err := blob.UploadConsolidated(ctx, gapPayloads)
	require.NoError(err)
	require.Len(gapPlacements, 2)
	gapShadows := make([]*metastorage.ConsolidationShadowPlacement, 0, len(gapRecords))
	for i, record := range gapRecords {
		gapShadows = append(gapShadows, &metastorage.ConsolidationShadowPlacement{
			BlockMetadataID:           record.ID,
			Tag:                       tag,
			Height:                    record.Metadata.Height,
			Hash:                      record.Metadata.Hash,
			SingleBlockObjectKeyMain:  gapSingleBlockKeys[i],
			ConsolidatedObjectKeyMain: gapDirtyKey,
			ObjectFormat:              gapPlacements[i].ObjectFormat,
			ByteOffset:                gapPlacements[i].ByteOffset,
			ByteLength:                gapPlacements[i].ByteLength,
			UncompressedLength:        gapPlacements[i].UncompressedLength,
		})
	}
	require.NoError(meta.PersistBlockConsolidationShadows(ctx, gapShadows))
	promotion, err = meta.PromoteBlockConsolidationShadows(ctx, tag, gapStartHeight, gapStartHeight+3, 3, 72*time.Hour)
	require.NoError(err)
	require.Equal(uint64(2), promotion.Blocks)

	gapExecutionKey := repairSHA256(fmt.Sprintf("repair-skipped-gap-%d", unique))
	gapManifest, err := repairer.PrepareObject(
		ctx,
		gapExecutionKey,
		tag,
		gapStartHeight,
		gapStartHeight+3,
		3,
		gapDirtyKey,
		nil,
	)
	require.NoError(err)
	gapRepairID = gapManifest.ID
	require.Equal(gapStartHeight, gapManifest.StartHeight)
	require.Equal(gapStartHeight+3, gapManifest.EndHeight)
	require.Equal(uint64(2), gapManifest.CanonicalBlockCount)

	_, err = db.ExecContext(ctx, `UPDATE block_metadata SET skipped = FALSE WHERE id = $1`, skippedMetadataID)
	require.NoError(err)
	_, err = repairer.Restore(ctx, gapManifest.ID, nil)
	require.ErrorContains(err, "CSCB repair range contains unrelated canonical metadata_id=")
	_, err = db.ExecContext(ctx, `UPDATE block_metadata SET skipped = TRUE WHERE id = $1`, skippedMetadataID)
	require.NoError(err)
	gapManifest, err = repairer.Restore(ctx, gapManifest.ID, nil)
	require.NoError(err)
	require.Equal(cscbrepair.StateRestored, gapManifest.State)
	for i, gapHeight := range gapHeights {
		activeGapBlock, getErr := meta.GetBlockByHeight(ctx, tag, gapHeight)
		require.NoError(getErr)
		require.Equal(gapSingleBlockKeys[i], activeGapBlock.ObjectKeyMain)
		require.Equal(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK, activeGapBlock.ObjectFormat)
	}

	// Pinned-key fencing is based on exact manifest identity, not a path naming
	// convention or a caller-supplied CSCB format value.
	nonstandardOldKey := fmt.Sprintf("custom-layout/dirty-object-%d", unique)
	var nonstandardRepairID int64
	err = db.QueryRowContext(ctx, `
		INSERT INTO cscb_repair_manifest (
			tag, state, bucket, old_consolidated_object_key_main,
			old_consolidated_object_version_id, old_consolidated_object_etag,
			old_consolidated_object_bytes, start_height, end_height,
			canonical_block_count, total_block_count, row_set_sha256
		) VALUES ($1, 'prepared', $2, $3, 'version', 'etag', 1, $4, $5, 0, 0, $6)
		RETURNING id`,
		tag,
		bucket,
		nonstandardOldKey,
		height,
		height+1,
		strings.Repeat("0", 64),
	).Scan(&nonstandardRepairID)
	require.NoError(err)
	defer cleanupRepairMetadata(t, db, 0, &nonstandardRepairID)
	_, err = db.ExecContext(ctx, `
		UPDATE block_metadata
		SET object_key_main = $2, object_format = $3
		WHERE id = $1`,
		replacementMetadataID,
		nonstandardOldKey,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK,
	)
	require.ErrorContains(err, "cannot reference a pinned old CSCB object")
}

func configureRepairTestEnvironment(t *testing.T, postgres *config.PostgresConfig) string {
	t.Helper()
	switch postgres.Host {
	case "localhost", "127.0.0.1", "::1":
		postgres.Port = 5433
		postgres.Database = "chainstorage_solana_mainnet"
		postgres.User = "cs_solana_mainnet_worker"
		postgres.Password = "worker_password"
		postgres.SSLMode = "require"
		return "http://localhost:4566"
	case "postgres":
		return "http://localstack:4566"
	default:
		t.Fatalf("refusing to run CSCB repair lifecycle test against PostgreSQL host %q", postgres.Host)
		return ""
	}
}

func newRepairLocalStackConfig(ctx context.Context, endpoint string) aws.Config {
	cfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awsconfig.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(string, string, ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint, HostnameImmutable: true}, nil
			}),
		),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to configure LocalStack: %v", err))
	}
	return cfg
}

func repairSHA256(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func repairRetentionSafeBucketPolicy(bucket string) string {
	return fmt.Sprintf(`{
		"Version":"2012-10-17",
		"Statement":[
			{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::%[1]s/*/consolidated/*","Condition":{"Null":{"s3:if-none-match":"true"}}},
			{"Effect":"Deny","Principal":"*","Action":["s3:DeleteObject","s3:DeleteObjectVersion","s3:ReplicateObject","s3:ReplicateDelete"],"Resource":"arn:aws:s3:::%[1]s/*/consolidated/*"},
			{"Effect":"Deny","Principal":"*","Action":"s3:PutLifecycleConfiguration","Resource":"arn:aws:s3:::%[1]s"}
		]
	}`, bucket)
}

func requireOldCSCBUnreferenced(t *testing.T, db *sql.DB, objectKey string) {
	t.Helper()
	var references uint64
	err := db.QueryRowContext(context.Background(), `
		SELECT
			(SELECT COUNT(*) FROM block_metadata WHERE object_key_main = $1)
			+
			(SELECT COUNT(*) FROM block_consolidation_shadow WHERE consolidated_object_key_main = $1)`,
		objectKey,
	).Scan(&references)
	require.NoError(t, err)
	require.Zero(t, references)
}

func openRepairDB(ctx context.Context, cfg *config.PostgresConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.Host,
		cfg.Port,
		cfg.Database,
		cfg.User,
		cfg.Password,
		cfg.SSLMode,
	)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func cleanupRepairMetadata(t *testing.T, db *sql.DB, blockMetadataID int64, repairID *int64) {
	t.Helper()
	ctx := context.Background()
	if repairID != nil && *repairID != 0 {
		_, _ = db.ExecContext(ctx, `DELETE FROM cscb_retirement_safety_observation WHERE bucket = (SELECT bucket FROM cscb_repair_manifest WHERE id = $1)`, *repairID)
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_execution DISABLE TRIGGER cscb_repair_execution_delete_trigger`)
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_block DISABLE TRIGGER cscb_repair_block_delete_trigger`)
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_manifest DISABLE TRIGGER cscb_repair_manifest_delete_trigger`)
		_, _ = db.ExecContext(ctx, `DELETE FROM cscb_repair_execution WHERE repair_id = $1`, *repairID)
		_, _ = db.ExecContext(ctx, `DELETE FROM cscb_repair_block WHERE repair_id = $1`, *repairID)
		_, _ = db.ExecContext(ctx, `DELETE FROM cscb_repair_manifest WHERE id = $1`, *repairID)
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_manifest ENABLE TRIGGER cscb_repair_manifest_delete_trigger`)
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_block ENABLE TRIGGER cscb_repair_block_delete_trigger`)
		_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_execution ENABLE TRIGGER cscb_repair_execution_delete_trigger`)
	}
	_, _ = db.ExecContext(ctx, `ALTER TABLE block_single_block_retention DISABLE TRIGGER block_single_block_retention_delete_trigger`)
	_, _ = db.ExecContext(ctx, `DELETE FROM block_single_block_retention WHERE block_metadata_id = $1`, blockMetadataID)
	_, _ = db.ExecContext(ctx, `ALTER TABLE block_single_block_retention ENABLE TRIGGER block_single_block_retention_delete_trigger`)
	_, _ = db.ExecContext(ctx, `DELETE FROM block_consolidation_shadow WHERE block_metadata_id = $1`, blockMetadataID)
	_, _ = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE block_metadata_id = $1`, blockMetadataID)
	_, _ = db.ExecContext(ctx, `DELETE FROM block_metadata WHERE id = $1`, blockMetadataID)
}

func cleanupRepairExecution(t *testing.T, db *sql.DB, executionKey string) {
	t.Helper()
	ctx := context.Background()
	_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_execution DISABLE TRIGGER cscb_repair_execution_delete_trigger`)
	_, _ = db.ExecContext(ctx, `DELETE FROM cscb_repair_execution WHERE execution_key = $1`, executionKey)
	_, _ = db.ExecContext(ctx, `ALTER TABLE cscb_repair_execution ENABLE TRIGGER cscb_repair_execution_delete_trigger`)
}

func cleanupRepairBucket(t *testing.T, client *awss3.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	_, _ = client.DeleteBucketPolicy(ctx, &awss3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)})
	versions, err := client.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	if err == nil {
		for _, version := range versions.Versions {
			_, _ = client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: version.Key, VersionId: version.VersionId})
		}
		for _, marker := range versions.DeleteMarkers {
			_, _ = client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: marker.Key, VersionId: marker.VersionId})
		}
	}
	_, _ = client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(bucket)})
}
