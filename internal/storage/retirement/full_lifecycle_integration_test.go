package retirement_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
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
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metapostgres "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	storageutils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestIntegrationRetirementFullStorageLifecycle(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()
	unique := time.Now().UTC().UnixNano()
	tag := uint32(1_500_000_000 + unique%100_000_000)
	height := uint64(9_000_000_000 + unique%100_000_000)
	bucket := fmt.Sprintf("chainstorage-retirement-lifecycle-%d", unique)

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
	localStackEndpoint := retirementLocalStackEndpoint(t, cfg.AWS.Postgres)
	cfg.Chain.BlockStartHeight = height

	db, err := openLifecycleDB(ctx, cfg.AWS.Postgres)
	if err != nil {
		t.Skipf("Postgres integration database is unavailable: %v", err)
	}
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
		fx.Replace(newRetirementLocalStackConfig(ctx, localStackEndpoint)),
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
	defer cleanupLifecycleBucket(t, rawS3, bucket)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          tag,
			Height:       height,
			Hash:         fmt.Sprintf("solana-hash-%d", unique),
			ParentHash:   fmt.Sprintf("solana-parent-%d", unique),
			ParentHeight: height - 1,
			Timestamp:    timestamppb.New(time.Now().UTC().Truncate(time.Second)),
		},
		Blobdata: &api.Block_Solana{Solana: &api.SolanaBlobdata{Header: []byte(`{"slot":9000000000,"transactions":["one","two"]}`)}},
	}

	singleBlockKey, err := singleBlockUploader.Upload(ctx, block, api.Compression_GZIP)
	require.NoError(err)
	require.NotEmpty(singleBlockKey)
	singleBlockObject, err := rawS3.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(singleBlockKey),
	})
	require.NoError(err)
	singleBlockBytes, err := io.ReadAll(singleBlockObject.Body)
	require.NoError(err)
	require.NoError(singleBlockObject.Body.Close())
	block.Metadata.ObjectKeyMain = singleBlockKey
	require.NoError(meta.PersistBlockMetas(ctx, true, []*api.BlockMetadata{block.Metadata}, nil))

	records, err := meta.GetBlocksMissingConsolidationShadow(ctx, tag, height, height+1, 1)
	require.NoError(err)
	require.Len(records, 1)
	blockMetadataID := records[0].ID
	defer cleanupLifecycleMetadata(t, db, blockMetadataID, bucket)

	downloadedSingleBlock, err := blob.Download(ctx, records[0].Metadata)
	require.NoError(err)
	require.True(proto.Equal(block, downloadedSingleBlock))
	consolidatedPayload, err := proto.Marshal(storageutils.CloneBlockWithoutStoragePlacement(downloadedSingleBlock))
	require.NoError(err)
	consolidatedKey, placements, err := blob.UploadConsolidated(ctx, []blobstorage.ConsolidatedBlockPayload{{
		Metadata:           records[0].Metadata,
		MetadataID:         blockMetadataID,
		RawBlockPayload:    blobstorage.BytesPayloadSource(consolidatedPayload),
		UncompressedLength: uint64(len(consolidatedPayload)),
	}})
	require.NoError(err)
	require.Len(placements, 1)
	require.Contains(consolidatedKey, "/consolidated/")
	placement := placements[0]
	require.NoError(meta.PersistBlockConsolidationShadows(ctx, []*metastorage.ConsolidationShadowPlacement{{
		BlockMetadataID:           blockMetadataID,
		Tag:                       tag,
		Height:                    height,
		Hash:                      block.Metadata.Hash,
		SingleBlockObjectKeyMain:  singleBlockKey,
		ConsolidatedObjectKeyMain: consolidatedKey,
		ObjectFormat:              placement.ObjectFormat,
		ByteOffset:                placement.ByteOffset,
		ByteLength:                placement.ByteLength,
		UncompressedLength:        placement.UncompressedLength,
	}}))
	promotion, err := meta.PromoteBlockConsolidationShadows(ctx, tag, height, height+1, 1, 72*time.Hour)
	require.NoError(err)
	require.Equal(uint64(1), promotion.Blocks)

	var retiredAt, retireAfter time.Time
	require.NoError(db.QueryRowContext(ctx, `
		SELECT single_block_retention_started_at, single_block_delete_after
		FROM block_consolidation_shadow
		WHERE block_metadata_id = $1`, blockMetadataID).Scan(&retiredAt, &retireAfter))
	require.WithinDuration(retiredAt.Add(72*time.Hour), retireAfter, time.Microsecond)
	_, err = db.ExecContext(ctx, `
		UPDATE block_consolidation_shadow
		SET single_block_retention_started_at = clock_timestamp() - INTERVAL '96 hours',
			single_block_delete_after = clock_timestamp() - INTERVAL '24 hours'
		WHERE block_metadata_id = $1`, blockMetadataID)
	require.NoError(err)

	active, err := meta.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(consolidatedKey, active.ObjectKeyMain)
	require.Equal(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH, active.ObjectFormat)
	parsedCSCB, err := blob.Download(ctx, active)
	require.NoError(err)
	require.True(proto.Equal(storageutils.CloneBlockWithoutStoragePlacement(block), storageutils.CloneBlockWithoutStoragePlacement(parsedCSCB)))

	_, err = rawS3.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(singleBlockKey),
		Body:   bytes.NewReader(singleBlockBytes),
	})
	require.NoError(err)
	_, err = rawS3.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(singleBlockKey),
	})
	require.NoError(err)

	policy := retentionSafeBucketPolicy(bucket)
	_, err = rawS3.PutBucketPolicy(ctx, &awss3.PutBucketPolicyInput{Bucket: aws.String(bucket), Policy: aws.String(policy)})
	require.NoError(err)
	store := retirement.NewS3ObjectStore(rawS3)
	repo := retirement.NewPostgresRepository(db)
	planner := retirement.NewPlanner(repo, store)
	req := retirement.PlanRequest{
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
		ProductionDeleteEnabled:     false,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackErrorCount:          0,
		Approval: retirement.Approval{
			Chain:       "solana-mainnet",
			StartHeight: height,
			EndHeight:   height + 1,
		},
	}
	report, err := planner.Plan(ctx, req)
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(retirement.ActionDeleteObjectVersion, report.Items[0].Action)
	require.Empty(report.Items[0].SkipReason)
	require.Equal(2, report.Items[0].SingleBlockVersions)
	require.Equal(1, report.Items[0].DeleteMarkers)
	require.True(report.SafetyGates.CSCBWriteOncePolicyVerified)

	snapshot, err := store.InspectObjectRetentionSafety(ctx, bucket, consolidatedKey)
	require.NoError(err)
	_, err = db.ExecContext(ctx, `
		INSERT INTO cscb_retirement_safety_observation (
			bucket, consolidated_object_key_main, configuration_sha256,
			first_observed_at, last_observed_at
		) VALUES ($1, $2, $3, clock_timestamp() - INTERVAL '16 minutes', clock_timestamp() - INTERVAL '16 minutes')
		ON CONFLICT (bucket, consolidated_object_key_main) DO UPDATE SET
			configuration_sha256 = EXCLUDED.configuration_sha256,
			first_observed_at = EXCLUDED.first_observed_at,
			last_observed_at = EXCLUDED.last_observed_at`,
		bucket, consolidatedKey, snapshot.ConfigurationSHA256)
	require.NoError(err)

	require.NoError(planner.Apply(ctx, req, report))
	require.Equal(retirement.ActionDeletedVerified, report.Items[0].Action)
	require.Equal(retirement.RetirementStateDeletedVerified, report.Items[0].RetirementState)

	singleBlockTopology, err := store.ListObjectVersions(ctx, bucket, singleBlockKey)
	require.NoError(err)
	require.Empty(singleBlockTopology.Versions)
	require.Empty(singleBlockTopology.DeleteMarkers)
	cscbHead, err := store.HeadObject(ctx, bucket, consolidatedKey)
	require.NoError(err)
	require.True(cscbHead.Exists)

	var (
		shadowSingleBlockKey sql.NullString
		deletedAt            sql.NullTime
		state                string
		manifestKey          sql.NullString
		keyHash              string
	)
	require.NoError(db.QueryRowContext(ctx, `
		SELECT shadow.single_block_object_key_main, shadow.single_block_object_deleted_at,
			retirement.state, retirement.single_block_object_key_main, retirement.single_block_object_key_sha256
		FROM block_consolidation_shadow shadow
		JOIN block_single_block_retention retirement
			ON retirement.block_metadata_id = shadow.block_metadata_id
		WHERE shadow.block_metadata_id = $1`, blockMetadataID).Scan(
		&shadowSingleBlockKey, &deletedAt, &state, &manifestKey, &keyHash,
	))
	require.False(shadowSingleBlockKey.Valid)
	require.True(deletedAt.Valid)
	require.Equal(retirement.RetirementStateDeletedVerified, state)
	require.False(manifestKey.Valid)
	require.Len(keyHash, 64)

	activeAfterDelete, err := meta.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(consolidatedKey, activeAfterDelete.ObjectKeyMain)
	parsedAfterDelete, err := blob.Download(ctx, activeAfterDelete)
	require.NoError(err)
	require.True(proto.Equal(storageutils.CloneBlockWithoutStoragePlacement(block), storageutils.CloneBlockWithoutStoragePlacement(parsedAfterDelete)))
	rawCSCBBlock, err := readRawLifecycleCSCBBlock(ctx, rawS3, bucket, consolidatedKey, activeAfterDelete)
	require.NoError(err)
	require.False(storageutils.HasBlockStoragePlacement(rawCSCBBlock))
	require.True(proto.Equal(storageutils.CloneBlockWithoutStoragePlacement(block), rawCSCBBlock))
}

func readRawLifecycleCSCBBlock(
	ctx context.Context,
	client *awss3.Client,
	bucket string,
	key string,
	metadata *api.BlockMetadata,
) (*api.Block, error) {
	object, err := client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	defer func() { _ = object.Body.Close() }()
	data, err := io.ReadAll(object.Body)
	if err != nil {
		return nil, err
	}
	index, err := cscb.ParseIndex(data)
	if err != nil {
		return nil, err
	}
	block, chunk, err := index.LookupBlock(metadata)
	if err != nil {
		return nil, err
	}
	start := chunk.CompressedPayloadOffset
	end := start + chunk.CompressedLength
	if end < start || end > uint64(len(data)) {
		return nil, fmt.Errorf("CSCB chunk bounds are invalid: start=%d end=%d bytes=%d", start, end, len(data))
	}
	chunkPayload, err := cscb.DecodeChunkFrame(bytes.NewReader(data[start:end]), index.Header.Codec)
	if err != nil {
		return nil, err
	}
	if err := cscb.ValidateChunkPayload(chunkPayload, chunk); err != nil {
		return nil, err
	}
	payload, err := cscb.ExtractBlockPayload(chunkPayload, block)
	if err != nil {
		return nil, err
	}
	var result api.Block
	if err := proto.Unmarshal(payload, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func openLifecycleDB(ctx context.Context, cfg *config.PostgresConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s", cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.SSLMode)
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

func retirementLocalStackEndpoint(t *testing.T, postgres *config.PostgresConfig) string {
	t.Helper()
	switch postgres.Host {
	case "localhost", "127.0.0.1", "::1":
		return "http://localhost:4566"
	case "postgres":
		return "http://localstack:4566"
	default:
		t.Fatalf("refusing to run retirement lifecycle test against PostgreSQL host %q", postgres.Host)
		return ""
	}
}

func newRetirementLocalStackConfig(ctx context.Context, endpoint string) aws.Config {
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

func retentionSafeBucketPolicy(bucket string) string {
	return fmt.Sprintf(`{
		"Version":"2012-10-17",
		"Statement":[
			{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::%[1]s/*/consolidated/*","Condition":{"Null":{"s3:if-none-match":"true"}}},
			{"Effect":"Deny","Principal":"*","Action":["s3:DeleteObject","s3:DeleteObjectVersion","s3:ReplicateObject","s3:ReplicateDelete"],"Resource":"arn:aws:s3:::%[1]s/*/consolidated/*"},
			{"Effect":"Deny","Principal":"*","Action":"s3:PutLifecycleConfiguration","Resource":"arn:aws:s3:::%[1]s"}
		]
	}`, bucket)
}

func cleanupLifecycleMetadata(t *testing.T, db *sql.DB, blockMetadataID int64, bucket string) {
	t.Helper()
	ctx := context.Background()
	_, _ = db.ExecContext(ctx, `DELETE FROM cscb_retirement_safety_observation WHERE bucket = $1`, bucket)
	_, _ = db.ExecContext(ctx, `ALTER TABLE block_single_block_retention DISABLE TRIGGER block_single_block_retention_delete_trigger`)
	_, _ = db.ExecContext(ctx, `DELETE FROM block_single_block_retention WHERE block_metadata_id = $1`, blockMetadataID)
	_, _ = db.ExecContext(ctx, `ALTER TABLE block_single_block_retention ENABLE TRIGGER block_single_block_retention_delete_trigger`)
	_, _ = db.ExecContext(ctx, `DELETE FROM block_consolidation_shadow WHERE block_metadata_id = $1`, blockMetadataID)
	_, _ = db.ExecContext(ctx, `DELETE FROM canonical_blocks WHERE block_metadata_id = $1`, blockMetadataID)
	_, _ = db.ExecContext(ctx, `DELETE FROM block_metadata WHERE id = $1`, blockMetadataID)
}

func cleanupLifecycleBucket(t *testing.T, client *awss3.Client, bucket string) {
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
