package main

import (
	"context"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/aws"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage/generationrehome"
)

type fencedGenerationRehomeFlags struct {
	tag                     uint32
	copyLedger              string
	targetGeneration        string
	execute                 bool
	confirmProductionRehome bool
	approveChain            string
	approveObjects          uint64
	approveRows             uint64
	approveTransition       string
	reportFile              string
}

var fencedRehomeFlags fencedGenerationRehomeFlags

func newFencedGenerationRehomeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rehome-fenced-consolidated-generation",
		Short: "audit or execute an exact legacy-null to v2 CSCB metadata rehome",
		Long: `Verify copied, immutable source and destination S3 versions plus the exact Postgres
placement and retirement cohort before changing consolidated metadata from legacy SQL NULL to v2.

The command reads the append-only copy ledger and selects only audit objects with fenced rows. It is
dry-run by default. Execution uses one object-scoped transaction at a time, holds the CSCB repair tag
and object locks, writes an immutable database audit row, updates only primary and consolidated
storage generation columns, and revalidates before commit. The deleted single-block generation is
never changed. Production execution requires the explicit confirmation and exact cohort approvals.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFencedGenerationRehome(cmd.Context(), fencedRehomeFlags)
		},
	}
	cmd.Flags().Uint32Var(&fencedRehomeFlags.tag, "tag", 0, "block tag; default zero resolves to the configured stable tag")
	cmd.Flags().StringVar(&fencedRehomeFlags.copyLedger, "copy-ledger", "", "append-only legacy CSCB audit/copy progress JSONL")
	cmd.Flags().StringVar(&fencedRehomeFlags.targetGeneration, "target-generation", "v2", "configured destination storage generation; this exception is restricted to v2")
	cmd.Flags().BoolVar(&fencedRehomeFlags.execute, "execute", false, "execute guarded object-scoped metadata transitions")
	cmd.Flags().BoolVar(&fencedRehomeFlags.confirmProductionRehome, "confirm-production-rehome", false, "second explicit gate required with --execute in production")
	cmd.Flags().StringVar(&fencedRehomeFlags.approveChain, "approve-chain", "", "exact chain approval, e.g. solana-mainnet")
	cmd.Flags().Uint64Var(&fencedRehomeFlags.approveObjects, "approve-objects", 0, "exact fenced object count approval")
	cmd.Flags().Uint64Var(&fencedRehomeFlags.approveRows, "approve-rows", 0, "exact referenced row count approval")
	cmd.Flags().StringVar(&fencedRehomeFlags.approveTransition, "approve-transition", "", "exact transition approval: legacy-null-to-v2")
	cmd.Flags().StringVar(&fencedRehomeFlags.reportFile, "report-file", "", "write the final JSON report to this file instead of stdout")
	_ = cmd.MarkFlagRequired("copy-ledger")
	return cmd
}

func runFencedGenerationRehome(ctx context.Context, flags fencedGenerationRehomeFlags) error {
	if flags.execute && isProductionEnvironment(commonFlags.env) && !flags.confirmProductionRehome {
		return xerrors.New("production execution requires --execute and --confirm-production-rehome")
	}
	ledger, err := os.Open(flags.copyLedger)
	if err != nil {
		return xerrors.Errorf("failed to open copy ledger %s: %w", flags.copyLedger, err)
	}
	objects, err := generationrehome.LoadFencedCopyLedger(ledger)
	_ = ledger.Close()
	if err != nil {
		return xerrors.Errorf("failed to load fenced copy ledger: %w", err)
	}

	var deps struct {
		fx.In
		S3Client s3.Client
	}
	app := startApp(aws.Module, s3.Module, fx.Populate(&deps))
	defer app.Close()
	cfg := app.Config()
	if cfg.StorageType.MetaStorageType != config.MetaStorageType_POSTGRES || cfg.AWS.Postgres == nil {
		return xerrors.New("fenced storage generation rehome requires Postgres meta storage")
	}
	if cfg.StorageType.BlobStorageType != config.BlobStorageType_UNSPECIFIED && cfg.StorageType.BlobStorageType != config.BlobStorageType_S3 {
		return xerrors.Errorf("fenced storage generation rehome requires S3 blob storage, got %v", cfg.StorageType.BlobStorageType)
	}

	writeGeneration, err := cfg.WriteBlockStorageGeneration()
	if err != nil {
		return xerrors.Errorf("failed to resolve write block storage generation: %w", err)
	}
	if writeGeneration != flags.targetGeneration {
		return xerrors.Errorf("target generation %q is not the active write generation %q", flags.targetGeneration, writeGeneration)
	}
	sourceBucket, err := cfg.ResolveBlockStorageBucket("")
	if err != nil {
		return xerrors.Errorf("failed to resolve legacy source bucket: %w", err)
	}
	destinationBucket, err := cfg.ResolveBlockStorageBucket(flags.targetGeneration)
	if err != nil {
		return xerrors.Errorf("failed to resolve destination bucket: %w", err)
	}
	tag := cfg.GetEffectiveBlockTag(flags.tag)
	chain := approvalChainFromFlags()

	logger.Info("running fenced consolidated storage generation rehome",
		zap.String("environment", string(cfg.Env())),
		zap.String("chain", chain),
		zap.Uint32("tag", tag),
		zap.String("source_bucket", sourceBucket),
		zap.String("destination_bucket", destinationBucket),
		zap.String("target_generation", flags.targetGeneration),
		zap.Int("objects", len(objects)),
		zap.Bool("execute", flags.execute),
	)

	db, err := openRetirementPostgres(ctx, cfg.AWS.Postgres, !flags.execute)
	if err != nil {
		return xerrors.Errorf("failed to open generation rehome postgres connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	service := generationrehome.NewService(
		generationrehome.NewPostgresRepository(db),
		generationrehome.NewS3ObjectStore(deps.S3Client),
	)
	report, runErr := service.Run(ctx, generationrehome.Request{
		Environment:       string(cfg.Env()),
		Chain:             chain,
		Tag:               tag,
		SourceBucket:      sourceBucket,
		DestinationBucket: destinationBucket,
		TargetGeneration:  flags.targetGeneration,
		Execute:           flags.execute,
		Approval: generationrehome.Approval{
			Chain:      flags.approveChain,
			Objects:    flags.approveObjects,
			Rows:       flags.approveRows,
			Transition: flags.approveTransition,
		},
		Progress: func(completed int, total int, item generationrehome.ReportItem) {
			logger.Info("fenced generation rehome progress",
				zap.Int("completed", completed),
				zap.Int("total", total),
				zap.String("action", item.Action),
				zap.String("object_key", item.ObjectKey),
			)
		},
	}, objects)
	if reportErr := writeGenerationRehomeReport(flags.reportFile, report); reportErr != nil {
		if runErr != nil {
			return xerrors.Errorf("generation rehome failed (%v) and report write failed: %w", runErr, reportErr)
		}
		return reportErr
	}
	if runErr != nil {
		return runErr
	}
	return nil
}

func writeGenerationRehomeReport(path string, report *generationrehome.Report) error {
	if path == "" {
		return generationrehome.WriteReportJSON(os.Stdout, report)
	}
	file, err := os.Create(path)
	if err != nil {
		return xerrors.Errorf("failed to create report file %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()
	if err := generationrehome.WriteReportJSON(file, report); err != nil {
		return xerrors.Errorf("failed to write report file %s: %w", path, err)
	}
	return nil
}
