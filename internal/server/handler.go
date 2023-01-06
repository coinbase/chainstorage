package server

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/cenkalti/backoff"
	"github.com/uber-go/tally"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/services"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/consts"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Server struct {
		config         *config.Config
		logger         *zap.Logger
		metaStorage    metastorage.MetaStorage
		blobStorage    blobstorage.BlobStorage
		s3Client       s3.Client
		parser         parser.Parser
		metrics        *serverMetrics
		streamDone     chan struct{}
		maxNoEventTime time.Duration
	}

	ServerParams struct {
		fx.In
		fxparams.Params
		MetaStorage metastorage.MetaStorage
		BlobStorage blobstorage.BlobStorage
		S3Client    s3.Client
		Parser      parser.Parser
		Lifecycle   fx.Lifecycle
	}

	RegisterParams struct {
		fx.In
		fxparams.Params
		Manager services.SystemManager
		Server  *Server
	}

	serverMetrics struct {
		scope tally.Scope
	}

	requestByRange interface {
		GetTag() uint32
		GetStartHeight() uint64
		GetEndHeight() uint64
	}

	requestByID interface {
		GetTag() uint32
		GetHeight() uint64
		GetHash() string
	}

	parseChainEventsRequestInput interface {
		// Deprecated: Use GetSequenceNum instead.
		GetSequence() string
		GetSequenceNum() int64
		GetInitialPositionInStream() string
		GetEventTag() uint32
	}
)

const (
	keepAliveTime    = 5 * time.Second
	keepAliveTimeout = 5 * time.Second
)

const (
	scopeName = "server"

	blocksServedCounter = "blocks_served"
	formatTag           = "format"
	formatFile          = "file"
	formatRaw           = "raw"
	formatNative        = "native"
	formatRosetta       = "rosetta"

	eventsServedCounter   = "events_served"
	eventTypeTag          = "event_type"
	eventTypeBlockAdded   = "block_added"
	eventTypeBlockRemoved = "block_removed"

	errorCounter = "error"
	serviceTag   = "service"
	methodTag    = "method"
	statusTag    = "status"

	requestCounter = "request"
)

const (
	streamingShortWaitTime              = time.Millisecond * 10
	streamingBackoffMaxInterval         = time.Minute
	streamingBackoffMultiplier          = 1.5
	streamingBackoffRandomizationFactor = 0.5
	streamingBackoffStop                = backoff.Stop
)

var (
	InitialPositionLatest   = api.InitialPosition_LATEST.String()
	InitialPositionEarliest = api.InitialPosition_EARLIEST.String()

	errServerShutDown       = xerrors.New("sever is shutting down")
	errNoNewEventForTooLong = xerrors.New("there was no new event for quite a while")

	// The method the interceptor is given is of the form /coinbase.chainstorage.ChainStorage/GetNativeBlock
	// This regex matches that and extracts the service and method name into
	// separate capture groups.
	methodRegex = regexp.MustCompile(`\/(.+)\/(.+)$`)
)

var registerServerOnce sync.Once
var registerServerError error

func NewServer(params ServerParams) *Server {
	s := &Server{
		config:         params.Config,
		logger:         log.WithPackage(params.Logger),
		metaStorage:    params.MetaStorage,
		blobStorage:    params.BlobStorage,
		s3Client:       params.S3Client,
		parser:         params.Parser,
		metrics:        newServerMetrics(params.Metrics),
		streamDone:     make(chan struct{}),
		maxNoEventTime: params.Config.Api.StreamingMaxNoEventTime,
	}
	params.Lifecycle.Append(fx.Hook{
		OnStart: s.onStart,
		OnStop:  s.onStop,
	})

	return s
}

func newServerMetrics(scope tally.Scope) *serverMetrics {
	scope = scope.SubScope(scopeName)
	return &serverMetrics{
		scope: scope,
	}
}

func Register(params RegisterParams) error {
	registerServerOnce.Do(func() {
		manager := params.Manager
		server := params.Server
		config := params.Config

		unaryInterceptor := grpc.ChainUnaryInterceptor(
			// XXX: Add your own interceptors here.
			server.unaryRequestInterceptor,
			server.unaryErrorInterceptor,
		)

		streamInterceptr := grpc.ChainStreamInterceptor(
			// XXX: Add your own interceptors here.
			server.streamRequestInterceptor,
			server.streamErrorInterceptor,
		)

		gs := grpc.NewServer(
			unaryInterceptor,
			streamInterceptr,
			grpc.KeepaliveParams(keepalive.ServerParameters{
				Time:    keepAliveTime,
				Timeout: keepAliveTimeout,
			}),
		)
		api.RegisterChainStorageServer(gs, server)
		reflection.Register(gs)
		daemonizeServer(manager, gs, config)
	})

	return registerServerError
}

func daemonizeServer(
	manager services.SystemManager,
	gs *grpc.Server,
	cfg *config.Config,
) {
	bindAddress := cfg.Server.BindAddress
	runGRPCServer := func(ctx context.Context) (services.ShutdownFunction, chan error) {
		return startServer(manager.Logger(), bindAddress, gs)
	}
	manager.ServiceWaitGroup().Add(1)
	go func() {
		defer manager.ServiceWaitGroup().Done()
		services.Daemonize(manager, runGRPCServer, "GRPC Server")
	}()
}

func startServer(
	logger *zap.Logger,
	bindAddress string,
	gs *grpc.Server,
) (services.ShutdownFunction, chan error) {
	errorChannel := make(chan error)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered", r)
			}
		}()
		logger.Info("Listening", zap.String("bindAddress", bindAddress))
		listener, err := net.Listen("tcp", bindAddress)
		if err != nil {
			logger.Error("Failed to listen", zap.Error(err))
			errorChannel <- err
			return
		}
		if err := gs.Serve(listener); err != nil {
			logger.Error("Failed to serve", zap.Error(err))
			errorChannel <- err
			return
		}
	}()
	return func(_ context.Context) error {
		gs.GracefulStop()
		<-done
		return nil
	}, errorChannel
}

func (s *Server) emitBlocksMetric(format string, count int64) {
	s.metrics.scope.Tagged(map[string]string{formatTag: format}).Counter(blocksServedCounter).Inc(count)
}

func (s *Server) emitEventsMetric(eventType string, count int64) {
	s.metrics.scope.Tagged(map[string]string{eventTypeTag: eventType}).Counter(eventsServedCounter).Inc(count)
}

func (s *Server) GetLatestBlock(ctx context.Context, req *api.GetLatestBlockRequest) (*api.GetLatestBlockResponse, error) {
	tag := s.config.GetEffectiveBlockTag(req.GetTag())
	if err := s.validateTag(tag); err != nil {
		return nil, xerrors.Errorf("failed to validate tag: %w", err)
	}

	block, err := s.metaStorage.GetLatestBlock(ctx, tag)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest block: %w", err)
	}

	return &api.GetLatestBlockResponse{
		Tag:        block.Tag,
		Hash:       block.Hash,
		ParentHash: block.ParentHash,
		Height:     block.Height,
		Timestamp:  block.Timestamp,
	}, nil
}

func (s *Server) GetBlockFile(ctx context.Context, req *api.GetBlockFileRequest) (*api.GetBlockFileResponse, error) {
	block, err := s.getBlockFromMetaStorage(ctx, req)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block from meta storage: %w", err)
	}

	blockFile, err := s.newBlockFile(block)
	if err != nil {
		return nil, xerrors.Errorf("failed to prepare block file: %w", err)
	}

	s.emitBlocksMetric(formatFile, 1)

	return &api.GetBlockFileResponse{
		File: blockFile,
	}, nil
}

func (s *Server) GetBlockFilesByRange(ctx context.Context, req *api.GetBlockFilesByRangeRequest) (*api.GetBlockFilesByRangeResponse, error) {
	blocks, err := s.getBlocksFromMetaStorage(ctx, req, s.config.Api.MaxNumBlockFiles)
	if err != nil {
		return nil, xerrors.Errorf("failed to get blocks from meta storage: %w", err)
	}

	blockFiles := make([]*api.BlockFile, len(blocks))
	for i := 0; i < len(blocks); i++ {
		blockFile, err := s.newBlockFile(blocks[i])
		if err != nil {
			return nil, xerrors.Errorf("newBlockFile error: %w", err)
		}

		blockFiles[i] = blockFile
	}

	s.emitBlocksMetric(formatFile, int64(len(blockFiles)))

	return &api.GetBlockFilesByRangeResponse{Files: blockFiles}, nil
}

func (s *Server) GetRawBlock(ctx context.Context, req *api.GetRawBlockRequest) (*api.GetRawBlockResponse, error) {
	block, err := s.getBlockFromMetaStorage(ctx, req)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block from meta storage: %w", err)
	}

	rawBlock, err := s.getBlockFromBlobStorage(ctx, block)
	if err != nil {
		return nil, xerrors.Errorf("failed to get raw blocks: %w", err)
	}

	s.emitBlocksMetric(formatRaw, 1)

	return &api.GetRawBlockResponse{
		Block: rawBlock,
	}, nil
}

func (s *Server) GetRawBlocksByRange(ctx context.Context, req *api.GetRawBlocksByRangeRequest) (*api.GetRawBlocksByRangeResponse, error) {
	blocks, err := s.getBlocksFromMetaStorage(ctx, req, s.config.Api.MaxNumBlocks)
	if err != nil {
		return nil, xerrors.Errorf("failed to get blocks from meta storage: %w", err)
	}

	rawBlocks, err := s.getBlocksFromBlobStorage(ctx, blocks)
	if err != nil {
		return nil, xerrors.Errorf("failed to get raw blocks: %w", err)
	}

	s.emitBlocksMetric(formatRaw, int64(len(rawBlocks)))

	return &api.GetRawBlocksByRangeResponse{
		Blocks: rawBlocks,
	}, nil
}

func (s *Server) GetNativeBlock(ctx context.Context, req *api.GetNativeBlockRequest) (*api.GetNativeBlockResponse, error) {
	block, err := s.getBlockFromMetaStorage(ctx, req)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block from meta storage: %w", err)
	}

	rawBlock, err := s.getBlockFromBlobStorage(ctx, block)
	if err != nil {
		return nil, xerrors.Errorf("failed to get raw blocks: %w", err)
	}

	nativeBlock, err := s.parser.ParseNativeBlock(ctx, rawBlock)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block: %w", err)
	}

	s.emitBlocksMetric(formatNative, 1)

	return &api.GetNativeBlockResponse{
		Block: nativeBlock,
	}, nil
}

func (s *Server) GetNativeBlocksByRange(ctx context.Context, req *api.GetNativeBlocksByRangeRequest) (*api.GetNativeBlocksByRangeResponse, error) {
	blocks, err := s.getBlocksFromMetaStorage(ctx, req, s.config.Api.MaxNumBlocks)
	if err != nil {
		return nil, xerrors.Errorf("failed to get blocks from meta storage: %w", err)
	}

	rawBlocks, err := s.getBlocksFromBlobStorage(ctx, blocks)
	if err != nil {
		return nil, xerrors.Errorf("failed to get raw blocks: %w", err)
	}

	nativeBlocks := make([]*api.NativeBlock, len(rawBlocks))
	for i := 0; i < len(nativeBlocks); i++ {
		nativeBlock, err := s.parser.ParseNativeBlock(ctx, rawBlocks[i])
		if err != nil {
			return nil, xerrors.Errorf("failed to parse block: %w", err)
		}

		nativeBlocks[i] = nativeBlock
	}

	s.emitBlocksMetric(formatNative, int64(len(nativeBlocks)))

	return &api.GetNativeBlocksByRangeResponse{
		Blocks: nativeBlocks,
	}, nil
}

func (s *Server) GetRosettaBlock(ctx context.Context, req *api.GetRosettaBlockRequest) (*api.GetRosettaBlockResponse, error) {
	block, err := s.getBlockFromMetaStorage(ctx, req)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block from meta storage: %w", err)
	}

	rawBlock, err := s.getBlockFromBlobStorage(ctx, block)
	if err != nil {
		return nil, xerrors.Errorf("failed to get raw blocks: %w", err)
	}

	rosettaBlock, err := s.parser.ParseRosettaBlock(ctx, rawBlock)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block: %w", err)
	}

	s.emitBlocksMetric(formatRosetta, 1)

	return &api.GetRosettaBlockResponse{
		Block: rosettaBlock,
	}, nil
}

func (s *Server) GetRosettaBlocksByRange(ctx context.Context, req *api.GetRosettaBlocksByRangeRequest) (*api.GetRosettaBlocksByRangeResponse, error) {
	blocks, err := s.getBlocksFromMetaStorage(ctx, req, s.config.Api.MaxNumBlocks)
	if err != nil {
		return nil, xerrors.Errorf("failed to get blocks from meta storage: %w", err)
	}

	rawBlocks, err := s.getBlocksFromBlobStorage(ctx, blocks)
	if err != nil {
		return nil, xerrors.Errorf("failed to get raw blocks: %w", err)
	}

	rosettaBlocks := make([]*api.RosettaBlock, len(rawBlocks))
	for i := 0; i < len(rosettaBlocks); i++ {
		rosettaBlock, err := s.parser.ParseRosettaBlock(ctx, rawBlocks[i])
		if err != nil {
			return nil, xerrors.Errorf("failed to parse block: %w", err)
		}

		rosettaBlocks[i] = rosettaBlock
	}

	s.emitBlocksMetric(formatRosetta, int64(len(rosettaBlocks)))

	return &api.GetRosettaBlocksByRangeResponse{
		Blocks: rosettaBlocks,
	}, nil
}

func (s *Server) newBlockFile(block *api.BlockMetadata) (*api.BlockFile, error) {
	if block.Skipped {
		return &api.BlockFile{
			Tag:     block.Tag,
			Height:  block.Height,
			Skipped: true,
		}, nil
	}

	key := block.GetObjectKeyMain()
	compression := storage_utils.GetCompressionType(key)
	getObjectReq, _ := s.s3Client.GetObjectRequest(&awss3.GetObjectInput{
		Bucket: aws.String(s.config.AWS.Bucket),
		Key:    aws.String(key),
	})
	fileUrl, err := getObjectReq.Presign(s.config.AWS.PresignedUrlExpiration)
	if err != nil {
		s.logger.Error("block file s3 presign error", zap.Reflect("block", block), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "internal block file url generation error: %+v", err)
	}

	return &api.BlockFile{
		Tag:          block.Tag,
		Hash:         block.Hash,
		ParentHash:   block.ParentHash,
		Height:       block.Height,
		ParentHeight: block.ParentHeight,
		FileUrl:      fileUrl,
		Compression:  compression,
	}, nil
}

func (s *Server) validateTag(tag uint32) error {
	if latestTag := s.config.GetLatestBlockTag(); tag > latestTag {
		return status.Errorf(codes.InvalidArgument, "requested tag is unavailable: latest tag is %v", latestTag)
	}

	return nil
}

func (s *Server) validateBlockRange(startHeight uint64, endHeight uint64, maxNumBlocks uint64) error {
	if startHeight >= endHeight {
		return status.Error(codes.InvalidArgument, "invalid range: start_height must be less than end_height")
	}

	if numBlocks := endHeight - startHeight; numBlocks > maxNumBlocks {
		return status.Errorf(codes.InvalidArgument, "block range size exceeded limit of %d", maxNumBlocks)
	}

	return nil
}

func (s *Server) getBlockFromMetaStorage(ctx context.Context, req requestByID) (*api.BlockMetadata, error) {
	tag := s.config.GetEffectiveBlockTag(req.GetTag())
	height := req.GetHeight()
	hash := req.GetHash()

	if err := s.validateTag(tag); err != nil {
		return nil, err
	}

	block, err := s.metaStorage.GetBlockByHash(ctx, tag, height, hash)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block by hash (tag=%v, height=%v, hash=%v): %w", tag, height, hash, err)
	}

	return block, nil
}

func (s *Server) getBlocksFromMetaStorage(ctx context.Context, req requestByRange, maxNumBlocks uint64) ([]*api.BlockMetadata, error) {
	tag := s.config.GetEffectiveBlockTag(req.GetTag())
	startHeight := req.GetStartHeight()
	endHeight := req.GetEndHeight()

	if endHeight == 0 {
		endHeight = startHeight + 1
	}

	if err := s.validateTag(tag); err != nil {
		return nil, err
	}

	if err := s.validateBlockRange(startHeight, endHeight, maxNumBlocks); err != nil {
		return nil, err
	}

	blocks, err := s.metaStorage.GetBlocksByHeightRange(ctx, tag, startHeight, endHeight)
	if err != nil {
		return nil, xerrors.Errorf("internal dynamodb error: %w", err)
	}

	// A chain reorg may happen after calling GetBlocksByHeightRange
	// Validate requests do not go beyond the latest watermark
	latestBlock, err := s.metaStorage.GetLatestBlock(ctx, tag)
	if err != nil {
		return nil, xerrors.Errorf("internal dynamodb error: %w", err)
	}

	latest := latestBlock.Height
	if endHeight-1 > latest {
		// Possibly caused by chain reorg.
		// Return a special error code so that client can retry the request.
		return nil, status.Errorf(codes.FailedPrecondition, "block end height exceeded latest watermark %d", latest)
	}

	return blocks, nil
}

func (s *Server) getBlockFromBlobStorage(ctx context.Context, block *api.BlockMetadata) (*api.Block, error) {
	output, err := s.blobStorage.Download(ctx, block)
	if err != nil {
		return nil, xerrors.Errorf("failed to download from blob storage (input={%+v}): %w", block, err)
	}

	return output, nil
}

func (s *Server) getBlocksFromBlobStorage(ctx context.Context, blocks []*api.BlockMetadata) ([]*api.Block, error) {
	result := make([]*api.Block, len(blocks))
	group, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(int(s.config.Api.NumWorkers)))
	for i := range blocks {
		i := i
		group.Go(func() error {
			input := blocks[i]
			output, err := s.blobStorage.Download(ctx, input)
			if err != nil {
				return xerrors.Errorf("failed to download from blob storage (input={%+v}): %w", input, err)
			}

			result[i] = output
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, xerrors.Errorf("failed to download blocks from blob storage: %w", err)
	}

	return result, nil
}

// getServiceAndMethod extracts the service and method name.
func getServiceAndMethod(fullMethod string) (service, method string) {
	methodParts := methodRegex.FindStringSubmatch(fullMethod)
	if len(methodParts) > 0 {
		service = methodParts[1]
		method = methodParts[2]
	} else {
		method = fullMethod
	}
	return
}

func (s *Server) unaryRequestInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	service, method := getServiceAndMethod(info.FullMethod)

	resp, err := handler(ctx, req)

	status := status.Convert(err).Code().String()
	s.metrics.scope.Tagged(map[string]string{
		serviceTag: service,
		methodTag:  method,
		statusTag:  status,
	}).Counter(requestCounter).Inc(1)
	s.logger.Debug(
		"handler.request",
		zap.String(methodTag, method),
		zap.String(statusTag, status),
	)
	return resp, err
}

func (s *Server) streamRequestInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	service, method := getServiceAndMethod(info.FullMethod)

	err := handler(srv, stream)

	status := status.Convert(err).Code().String()
	s.metrics.scope.Tagged(map[string]string{
		methodTag: method,
		statusTag: status,
	}).Counter(requestCounter).Inc(1)
	s.logger.Debug(
		"handler.stream.request",
		zap.String(serviceTag, service),
		zap.String(methodTag, method),
		zap.String(statusTag, status),
	)
	return err
}

// unaryErrorInterceptor is responsible for instrumenting the errors returned by unary methods.
func (s *Server) unaryErrorInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	return resp, s.mapToGrpcError(err, info.FullMethod, req)
}

// streamErrorInterceptor is responsible for instrumenting the errors returned by stream methods.
func (s *Server) streamErrorInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, stream)
	return s.mapToGrpcError(err, info.FullMethod, nil)
}

func (s *Server) mapToGrpcError(err error, fullMethod string, request interface{}) error {
	if err == nil {
		return nil
	}

	service, method := getServiceAndMethod(fullMethod)
	if service != consts.FullServiceName {
		// Don't touch the error if the service name doesn't match.
		// e.g. calls for the "grpc.reflection.v1alpha.ServerReflection" service are skipped.
		return err
	}

	description := "internal error"
	code := codes.Internal

	var grpcErr gateway.GrpcError
	if xerrors.As(err, &grpcErr) {
		// If the error is already a grpc error, use the given code.
		code = grpcErr.GRPCStatus().Code()
		description = code.String()
	} else if xerrors.Is(err, storage.ErrItemNotFound) {
		description = "block not found"
		code = codes.NotFound
	} else if xerrors.Is(err, storage.ErrNoEventHistory) {
		description = "no event history available"
		code = codes.InvalidArgument
	} else if xerrors.Is(err, storage.ErrInvalidEventId) {
		description = "invalid event id"
		code = codes.InvalidArgument
	} else if xerrors.Is(err, storage.ErrOutOfRange) || xerrors.Is(err, storage.ErrInvalidHeight) {
		description = "invalid height or out of range"
		code = codes.InvalidArgument
	} else if xerrors.Is(err, parser.ErrInvalidChain) {
		// Possibly caused by chain reorg.
		description = "invalid chain"
		code = codes.FailedPrecondition
	} else if xerrors.Is(err, storage.ErrRequestCanceled) {
		description = "storage request canceled"
		code = codes.Canceled
	} else if xerrors.Is(err, parser.ErrNotImplemented) {
		description = "parser method not implemented"
		code = codes.Unimplemented
	} else if xerrors.Is(err, context.Canceled) {
		description = "context canceled"
		code = codes.Canceled
	} else if xerrors.Is(err, context.DeadlineExceeded) {
		description = "context deadline exceeded"
		code = codes.DeadlineExceeded
	} else if xerrors.Is(err, errNoNewEventForTooLong) || xerrors.Is(err, errServerShutDown) {
		description = "please retry after a moment"
		code = codes.Aborted
	}

	s.metrics.scope.Tagged(map[string]string{
		methodTag: method,
		statusTag: code.String(),
	}).Counter(errorCounter).Inc(1)

	logFn := s.logger.Warn
	if code == codes.Internal {
		logFn = s.logger.Error
	}

	logFn(
		"server.error",
		zap.String("method", method),
		zap.String("status", code.String()),
		zap.String("description", description),
		zap.Reflect("request", request),
		zap.Error(err),
	)

	return status.Errorf(code, "%v: %+v", description, err)
}

func encodeEventIdToSequence(eventId int64) string {
	return strconv.FormatInt(eventId, 10)
}

func decodeSequenceToEventId(sequence string) (int64, error) {
	return strconv.ParseInt(sequence, 10, 64)
}

func (s *Server) StreamChainEvents(request *api.ChainEventsRequest, stream api.ChainStorage_StreamChainEventsServer) error {
	ctx := stream.Context()
	eventTag := request.EventTag
	lastSentEventId, err := s.parseChainEventsRequest(ctx, request)
	if err != nil {
		return xerrors.Errorf("failed to parse chain events request: %w", err)
	}

	tick := time.NewTicker(s.config.Api.StreamingInterval)
	defer tick.Stop()

	backoff := s.newStreamingBackoff()
	for {
		events, err := s.metaStorage.GetEventsAfterEventId(ctx, eventTag, lastSentEventId, s.config.Api.StreamingBatchSize)
		if err != nil {
			return xerrors.Errorf("failed to retrieve events: %w", err)
		}

		if len(events) > 0 {
			backoff.Reset()
			tick.Reset(streamingShortWaitTime)
		} else {
			waitTime := backoff.NextBackOff()
			if waitTime == streamingBackoffStop {
				return xerrors.Errorf("max wait time exceeded: %w", errNoNewEventForTooLong)
			}
			tick.Reset(waitTime)
		}

		for _, e := range events {
			event := &api.BlockchainEvent{
				Sequence:    encodeEventIdToSequence(e.EventId),
				SequenceNum: e.EventId,
				Type:        e.EventType,
				Block: &api.BlockIdentifier{
					Tag:       e.Tag,
					Hash:      e.BlockHash,
					Height:    e.BlockHeight,
					Skipped:   e.BlockSkipped,
					Timestamp: utils.ToTimestamp(e.BlockTimestamp),
				},
				EventTag: e.EventTag,
			}
			if e.EventType == api.BlockchainEvent_BLOCK_ADDED {
				s.emitEventsMetric(eventTypeBlockAdded, 1)
			} else if e.EventType == api.BlockchainEvent_BLOCK_REMOVED {
				s.emitEventsMetric(eventTypeBlockRemoved, 1)
			}

			res := &api.ChainEventsResponse{
				Event: event,
			}
			if err := stream.Send(res); err != nil {
				if code := status.Code(err); code == codes.Unavailable {
					// The client's transport is closing. Close the stream now.
					s.logger.Debug("client's transport is closing", zap.Error(err))
					return nil
				}
				return xerrors.Errorf("failed to stream event to client: %w", err)
			}

			lastSentEventId = e.EventId
		}

		select {
		case <-tick.C:
		case <-s.streamDone:
			return xerrors.Errorf("server is being redeployed: %w", errServerShutDown)
		case <-ctx.Done():
			// The client is canceled. Close the stream now.
			s.logger.Debug("client is canceled", zap.Error(err))
			return nil
		}
	}
}

func (s *Server) newStreamingBackoff() backoff.BackOff {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     s.config.Api.StreamingInterval,
		MaxElapsedTime:      s.maxNoEventTime,
		RandomizationFactor: streamingBackoffRandomizationFactor,
		Multiplier:          streamingBackoffMultiplier,
		MaxInterval:         streamingBackoffMaxInterval,
		Clock:               backoff.SystemClock,
	}
	b.Reset()
	return b
}

func (s *Server) GetChainEvents(ctx context.Context, req *api.GetChainEventsRequest) (*api.GetChainEventsResponse, error) {
	if req.MaxNumEvents == 0 {
		req.MaxNumEvents = uint64(1)
	}
	eventTag := req.GetEventTag()
	lastSentEventId, err := s.parseChainEventsRequest(ctx, req)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse chain events request: %w", err)
	}
	events, err := s.metaStorage.GetEventsAfterEventId(ctx, eventTag, lastSentEventId, req.GetMaxNumEvents())
	if err != nil {
		return nil, xerrors.Errorf("failed to get events (req={%+v}): %w", req, err)
	}

	blockchainEvents := make([]*api.BlockchainEvent, 0, len(events))
	var numBlockAddedEvents, numBlockRemovedEvents int64
	for _, e := range events {
		blockchainEvents = append(blockchainEvents, &api.BlockchainEvent{
			Sequence:    encodeEventIdToSequence(e.EventId),
			SequenceNum: e.EventId,
			Type:        e.EventType,
			Block: &api.BlockIdentifier{
				Tag:       e.Tag,
				Hash:      e.BlockHash,
				Height:    e.BlockHeight,
				Skipped:   e.BlockSkipped,
				Timestamp: utils.ToTimestamp(e.BlockTimestamp),
			},
			EventTag: e.EventTag,
		})

		if e.EventType == api.BlockchainEvent_BLOCK_ADDED {
			numBlockAddedEvents += 1
		} else if e.EventType == api.BlockchainEvent_BLOCK_REMOVED {
			numBlockRemovedEvents += 1
		}
	}

	if numBlockAddedEvents > 0 {
		s.emitEventsMetric(eventTypeBlockAdded, numBlockAddedEvents)
	}

	if numBlockRemovedEvents > 0 {
		s.emitEventsMetric(eventTypeBlockRemoved, numBlockRemovedEvents)
	}

	return &api.GetChainEventsResponse{Events: blockchainEvents}, nil
}

func (s *Server) parseChainEventsRequest(ctx context.Context, input parseChainEventsRequestInput) (int64, error) {
	sequence := input.GetSequence()
	sequenceNum := input.GetSequenceNum()
	initialPositionInStream := input.GetInitialPositionInStream()
	eventTag := input.GetEventTag()
	latestEventTag := s.config.GetLatestEventTag()

	if eventTag > latestEventTag {
		return 0, status.Errorf(codes.InvalidArgument, "do not support eventTag=%d, latestEventTag=%d", eventTag, latestEventTag)
	}

	var lastSentEventId int64
	if sequence != "" {
		// Though this field is deprecated, use it in favor of initialPositionInStream and sequenceNum
		// for backward compatibility.
		decodedEventId, err := decodeSequenceToEventId(sequence)
		if err != nil {
			return 0, status.Errorf(codes.InvalidArgument, "invalid sequence: failed to decode sequence (%s) to event id: %+v", sequence, err)
		}
		lastSentEventId = decodedEventId
	} else if initialPositionInStream != "" {
		// if initialPositionInStream is set, use it to determine the "last sent event id", a.k.a cursor
		switch initialPositionInStream {
		case InitialPositionLatest:
			// if start from latest, assume last sent event id is max event id - 1
			latestEventId, err := s.metaStorage.GetMaxEventId(ctx, eventTag)
			if err != nil {
				return 0, xerrors.Errorf("failed to retrieve max event id for eventTag=%d: %w", eventTag, err)
			}
			lastSentEventId = latestEventId - 1
		case InitialPositionEarliest:
			// if start from earliest, assume last sent event id is EventIdStartValue - 1 such that we start sending from EventIdStartValue
			lastSentEventId = metastorage.EventIdStartValue - 1
		default:
			// if start from a specific height, first find the first event associated with that height, then move cursor to event id - 1
			decodedHeight, err := strconv.ParseUint(initialPositionInStream, 10, 64)
			if err != nil {
				return 0, status.Errorf(codes.InvalidArgument, "invalid initial position in stream (%s): %+v", initialPositionInStream, err)
			}
			eventId, err := s.metaStorage.GetFirstEventIdByBlockHeight(ctx, eventTag, decodedHeight)
			if err != nil {
				// if no such event under the given block height, metaStorage will throw ErrItemNotFound
				return 0, xerrors.Errorf("failed to retrieve first event id by block height: %w", err)
			}
			lastSentEventId = eventId - 1
		}
	} else {
		// Use sequenceNum if sequence and initialPositionInStream are empty.
		lastSentEventId = sequenceNum
	}

	return lastSentEventId, nil
}

func (s *Server) GetChainMetadata(ctx context.Context, req *api.GetChainMetadataRequest) (*api.GetChainMetadataResponse, error) {
	return &api.GetChainMetadataResponse{
		LatestBlockTag:       s.config.GetLatestBlockTag(),
		StableBlockTag:       s.config.GetStableBlockTag(),
		LatestEventTag:       s.config.GetLatestEventTag(),
		StableEventTag:       s.config.GetStableEventTag(),
		BlockStartHeight:     s.config.Chain.BlockStartHeight,
		IrreversibleDistance: s.config.Chain.IrreversibleDistance,
		BlockTime:            s.config.Chain.BlockTime.String(),
	}, nil
}

func (s *Server) GetVersionedChainEvent(ctx context.Context, req *api.GetVersionedChainEventRequest) (*api.GetVersionedChainEventResponse, error) {
	fromEventTag := req.GetFromEventTag()
	toEventTag := req.GetToEventTag()

	fromEventId := req.GetFromSequenceNum()
	if req.GetFromSequence() != "" {
		// TODO: deprecate this field.
		var err error
		fromEventId, err = decodeSequenceToEventId(req.GetFromSequence())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid sequence: failed to decode sequence (%s) to event id", req.GetFromSequence())
		}
	}

	fromEvent, err := s.metaStorage.GetEventByEventId(ctx, fromEventTag, fromEventId)
	if err != nil {
		return nil, xerrors.Errorf("failed to get event for eventTag=%v, sequence=%v: %w", fromEventTag, fromEventId, err)
	}
	blockHeight := fromEvent.BlockHeight

	events, err := s.metaStorage.GetEventsByBlockHeight(ctx, toEventTag, blockHeight)
	if err != nil {
		return nil, xerrors.Errorf("failed to get events for fromEventTag=%v, toEventTag=%v, sequence=%v: %w", fromEventTag, toEventTag, fromEventId, err)
	}

	// return the event with the largest eventId if there are multiple matches
	// e.g. +h1 -> +h2 -> +h3 -> -h3 -> +h3 -> ...
	// should return the second +h3 when finding a matching event for +h3
	var matchedEvent *model.EventDDBEntry
	for _, event := range events {
		if event.BlockHash == fromEvent.BlockHash &&
			event.ParentHash == fromEvent.ParentHash &&
			event.EventType == fromEvent.EventType &&
			event.BlockSkipped == fromEvent.BlockSkipped &&
			event.Tag == fromEvent.Tag {
			if matchedEvent == nil || event.EventId > matchedEvent.EventId {
				matchedEvent = event
			}
		}
	}

	if matchedEvent == nil {
		return nil, xerrors.Errorf("cannot find matching event for fromEventTag=%v, toEventTag=%v, sequence=%v. please use another event.", fromEventTag, toEventTag, fromEventId)
	}

	return &api.GetVersionedChainEventResponse{
		Event: &api.BlockchainEvent{
			Sequence:    encodeEventIdToSequence(matchedEvent.EventId),
			SequenceNum: matchedEvent.EventId,
			Type:        matchedEvent.EventType,
			Block: &api.BlockIdentifier{
				Tag:       matchedEvent.Tag,
				Hash:      matchedEvent.BlockHash,
				Height:    matchedEvent.BlockHeight,
				Skipped:   matchedEvent.BlockSkipped,
				Timestamp: utils.ToTimestamp(matchedEvent.BlockTimestamp),
			},
			EventTag: matchedEvent.EventTag,
		},
	}, nil
}

func (s *Server) onStart(ctx context.Context) error {
	s.logger.Info(
		"starting server",
		zap.String("namespace", s.config.Namespace()),
		zap.String("env", string(s.config.Env())),
		zap.String("blockchain", s.config.Blockchain().GetName()),
		zap.String("network", s.config.Network().GetName()),
	)
	return nil
}

// onStop will terminate all open rpc streams in order to allow a graceful shutdown
// of the server. All non-streaming requests will be allowed to complete before shutdown.
func (s *Server) onStop(ctx context.Context) error {
	s.logger.Info("stopping server")
	close(s.streamDone)
	return nil
}
