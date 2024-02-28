package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/aws"
	blockchainModule "github.com/coinbase/chainstorage/internal/blockchain"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	"github.com/coinbase/chainstorage/internal/tracer"
	"github.com/coinbase/chainstorage/internal/workflow"
)

const (
	workflowFlagName       = "workflow"
	workflowIDFlagName     = "workflowID"
	inputFlagName          = "input"
	defaultTerminateReason = "Terminated by workflow cmd"
)

var (
	cadenceHost = map[config.Env]string{
		config.EnvLocal:       "http://localhost:8080",
		config.EnvDevelopment: "https://temporal-dev.example.com",
		config.EnvProduction:  "https://temporal.example.com",
	}
)

type executors struct {
	fx.In
	Backfiller      *workflow.Backfiller
	Monitor         *workflow.Monitor
	Poller          *workflow.Poller
	Streamer        *workflow.Streamer
	Benchmarker     *workflow.Benchmarker
	CrossValidator  *workflow.CrossValidator
	EventBackfiller *workflow.EventBackfiller
	Replicator      *workflow.Replicator
}

var (
	workflowCmd = &cobra.Command{
		Use:   "workflow",
		Short: "tool for managing chainstorage workflows",
	}

	startWorkflowCmd = &cobra.Command{
		Use:   "start",
		Short: "start a workflow",
		RunE: func(cmd *cobra.Command, args []string) error {
			return startWorkflow()
		},
	}

	stopWorkflowCmd = &cobra.Command{
		Use:   "stop",
		Short: "stop a workflow",
		RunE: func(cmd *cobra.Command, args []string) error {
			return stopWorkflow()
		},
	}

	workflowFlags struct {
		workflow   string
		workflowID string
		input      string
	}
)

func init() {
	workflowCmd.PersistentFlags().StringVar(&workflowFlags.workflow, workflowFlagName, "", "Workflow name")
	workflowCmd.PersistentFlags().StringVar(&workflowFlags.workflowID, workflowIDFlagName, "", "Workflow identity")
	workflowCmd.PersistentFlags().StringVar(&workflowFlags.input, inputFlagName, "", "Input json string")

	workflowCmd.AddCommand(startWorkflowCmd)
	workflowCmd.AddCommand(stopWorkflowCmd)
	rootCmd.AddCommand(workflowCmd)
}

func startWorkflow() error {
	workflowIdentity := workflow.GetWorkflowIdentify(workflowFlags.workflow)
	if workflowIdentity == workflow.UnknownIdentity {
		return xerrors.Errorf("invalid workflow: %v", workflowFlags.workflow)
	}

	app, executors, err := initApp()
	if err != nil {
		return xerrors.Errorf("failed to init app: %w", err)
	}
	defer app.Close()

	ctx := context.Background()
	workflowIdentityString, err := workflowIdentity.String()
	if err != nil {
		return xerrors.Errorf("error parsing workflowIdentity: %w", err)
	}

	req, err := workflowIdentity.UnmarshalJsonStringToRequest(workflowFlags.input)
	if err != nil {
		return xerrors.Errorf("error converting input flag to request: %w", err)
	}

	if !confirmWorkflowOperation("start", workflowIdentityString, req) {
		return nil
	}

	var run client.WorkflowRun
	switch workflowIdentity {
	case workflow.BackfillerIdentity:
		request, ok := req.(workflow.BackfillerRequest)
		if !ok {
			return xerrors.Errorf("error converting to request type")
		}
		run, err = executors.Backfiller.Execute(ctx, &request)
	case workflow.MonitorIdentity:
		request, ok := req.(workflow.MonitorRequest)
		if !ok {
			return xerrors.Errorf("error converting to request type")
		}
		run, err = executors.Monitor.Execute(ctx, &request)
	case workflow.PollerIdentity:
		request, ok := req.(workflow.PollerRequest)
		if !ok {
			return xerrors.Errorf("error converting to request type")
		}
		run, err = executors.Poller.Execute(ctx, &request)
	case workflow.StreamerIdentity:
		request, ok := req.(workflow.StreamerRequest)
		if !ok {
			return xerrors.Errorf("error converting to request type")
		}
		run, err = executors.Streamer.Execute(ctx, &request)
	case workflow.BenchmarkerIdentity:
		request, ok := req.(workflow.BenchmarkerRequest)
		if !ok {
			return xerrors.Errorf("error converting to request type")
		}
		run, err = executors.Benchmarker.Execute(ctx, &request)
	case workflow.CrossValidatorIdentity:
		request, ok := req.(workflow.CrossValidatorRequest)
		if !ok {
			return xerrors.Errorf("error converting to request type")
		}
		run, err = executors.CrossValidator.Execute(ctx, &request)
	case workflow.EventBackfillerIdentity:
		request, ok := req.(workflow.EventBackfillerRequest)
		if !ok {
			return xerrors.Errorf("error converting to request type")
		}
		run, err = executors.EventBackfiller.Execute(ctx, &request)
	case workflow.ReplicatorIdentity:
		request, ok := req.(workflow.ReplicatorRequest)
		if !ok {
			return xerrors.Errorf("error converting to request type")
		}
		run, err = executors.Replicator.Execute(ctx, &request)
	default:
		return xerrors.Errorf("unsupported workflow identity: %v", workflowIdentity)
	}

	if err != nil {
		logger.Error("failed to start workflow",
			zap.String("workflowIdentity", workflowIdentityString),
		)
		return xerrors.Errorf("failed to start workflow: %w", err)
	}

	workflowURL := getWorkflowURL(app, run)
	logger.Info("started workflow",
		zap.String("workflowIdentity", workflowIdentityString),
		zap.String("workflowRunID", run.GetRunID()),
		zap.String("workflowID", run.GetID()),
		zap.String("workflowURL", workflowURL),
	)
	return nil
}

func stopWorkflow() error {
	var workflowIdentityString string
	var err error
	workflowIdentity := workflow.GetWorkflowIdentify(workflowFlags.workflow)
	if workflowIdentity == workflow.UnknownIdentity {
		return xerrors.Errorf("invalid workflow: %v", workflowFlags.workflow)
	}

	app, executors, err := initApp()
	if err != nil {
		return xerrors.Errorf("failed to init app: %w", err)
	}
	defer app.Close()

	if workflowFlags.workflowID != "" {
		workflowIdentityString = workflowFlags.workflowID
	} else {
		workflowIdentityString, err = workflowIdentity.String()
		if err != nil {
			return xerrors.Errorf("error parsing workflowIdentity: %w", err)
		}
	}
	if !confirmWorkflowOperation("stop", workflowIdentityString, nil) {
		return nil
	}

	reason := defaultTerminateReason
	if workflowFlags.input != "" {
		reason = workflowFlags.input
	}

	ctx := context.Background()
	switch workflowIdentity {
	case workflow.BackfillerIdentity:
		err = executors.Backfiller.StopWorkflow(ctx, workflowIdentityString, reason)
	case workflow.MonitorIdentity:
		err = executors.Monitor.StopWorkflow(ctx, workflowIdentityString, reason)
	case workflow.PollerIdentity:
		err = executors.Poller.StopWorkflow(ctx, workflowIdentityString, reason)
	case workflow.StreamerIdentity:
		err = executors.Streamer.StopWorkflow(ctx, workflowIdentityString, reason)
	case workflow.BenchmarkerIdentity:
		err = executors.Benchmarker.StopWorkflow(ctx, workflowIdentityString, reason)
	case workflow.CrossValidatorIdentity:
		err = executors.CrossValidator.StopWorkflow(ctx, workflowIdentityString, reason)
	case workflow.EventBackfillerIdentity:
		err = executors.EventBackfiller.StopWorkflow(ctx, workflowIdentityString, reason)
	case workflow.ReplicatorIdentity:
		err = executors.Replicator.StopWorkflow(ctx, workflowIdentityString, reason)
	default:
		return xerrors.Errorf("unsupported workflow identity: %v", workflowIdentity)
	}

	if err != nil {
		return xerrors.Errorf("failed to stop workflow for workflowID=%s: %w", workflowIdentityString, err)
	}

	logger.Info("stopped workflow",
		zap.String("workflowIdentity", workflowIdentityString),
	)

	return nil
}

func initApp() (CmdApp, *executors, error) {
	var executors executors
	app := startApp(
		cadence.Module,
		blockchainModule.Module,
		s3.Module,
		downloader.Module,
		storage.Module,
		workflow.Module,
		aws.Module,
		tracer.Module,
		fx.Populate(&executors),
	)

	return app, &executors, nil
}

func confirmWorkflowOperation(operation string, workflowIdentity string, input any) bool {
	logger.Info(
		"workflow arguments",
		zap.String("workflow", workflowIdentity),
		zap.String("env", string(env)),
		zap.String("blockchain", blockchain.GetName()),
		zap.String("network", network.GetName()),
		zap.String("sidechain", sidechain.GetName()),
		zap.Reflect("input", input),
	)

	chainInfo := fmt.Sprintf("%v-%v", commonFlags.blockchain, commonFlags.network)
	if commonFlags.sidechain != "" {
		chainInfo = fmt.Sprintf("%v-%v", chainInfo, commonFlags.sidechain)
	}

	prompt := fmt.Sprintf(
		"%v%v%v%v%v",
		color.CyanString(fmt.Sprintf("Are you sure you want to %v ", operation)),
		color.MagentaString(fmt.Sprintf("\"%v\" ", workflowIdentity)),
		color.CyanString("in "),
		color.MagentaString(fmt.Sprintf("%v::%v", env, chainInfo)),
		color.CyanString("? (y/N) "),
	)

	return confirm(prompt)
}

func getWorkflowURL(app CmdApp, run client.WorkflowRun) string {
	return fmt.Sprintf(
		"%s/namespaces/%s/workflows/%s/%s/history",
		cadenceHost[env],
		app.Config().Cadence.Domain,
		url.PathEscape(run.GetID()),
		run.GetRunID(),
	)
}
