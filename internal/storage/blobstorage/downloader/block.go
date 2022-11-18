package downloader

import (
	"context"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	tracehttp "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/finalizer"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	BlockDownloader interface {
		Download(ctx context.Context, blockFile *api.BlockFile) (*api.Block, error)
	}

	BlockDownloaderParams struct {
		fx.In
		fxparams.Params
		HttpClient HTTPClient
	}

	HTTPClient interface {
		Do(req *http.Request) (*http.Response, error)
	}

	blockDownloaderImpl struct {
		config     *config.Config
		logger     *zap.Logger
		httpClient HTTPClient
		retry      retry.Retry
	}
)

const (
	timeout = time.Second * 30
)

func NewBlockDownloader(params BlockDownloaderParams) BlockDownloader {
	logger := log.WithPackage(params.Logger)
	return &blockDownloaderImpl{
		config:     params.Config,
		logger:     logger,
		httpClient: params.HttpClient,
		retry:      retry.New(retry.WithLogger(logger)),
	}
}

func NewHTTPClient() HTTPClient {
	httpClient := &http.Client{
		Timeout: timeout,
	}
	httpClient = tracehttp.WrapClient(httpClient, tracehttp.RTWithResourceNamer(func(req *http.Request) string {
		return "/chainstorage/blobstorage/downloader"
	}))
	return httpClient
}

func (d *blockDownloaderImpl) Download(ctx context.Context, blockFile *api.BlockFile) (*api.Block, error) {
	if blockFile.Skipped {
		// No blob data is available when the block is skipped.
		return &api.Block{
			Blockchain: d.config.Chain.Blockchain,
			Network:    d.config.Chain.Network,
			Metadata: &api.BlockMetadata{
				Tag:     blockFile.Tag,
				Height:  blockFile.Height,
				Skipped: true,
			},
			Blobdata: nil,
		}, nil
	}

	defer d.logDuration(time.Now())
	var block *api.Block
	if err := d.retry.Retry(ctx, func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, blockFile.FileUrl, nil)
		if err != nil {
			return xerrors.Errorf("failed to create download request: %w", err)
		}

		httpResp, err := d.httpClient.Do(req)
		if err != nil {
			return retry.Retryable(xerrors.Errorf("failed to download block file: %w", err))
		}

		finalizer := finalizer.WithCloser(httpResp.Body)
		defer finalizer.Finalize()

		if statusCode := httpResp.StatusCode; statusCode != http.StatusOK {
			if statusCode == http.StatusRequestTimeout ||
				statusCode == http.StatusTooManyRequests ||
				statusCode >= http.StatusInternalServerError {
				return retry.Retryable(xerrors.Errorf("received %d status code: %w", statusCode, errors.ErrDownloadFailure))
			} else {
				return xerrors.Errorf("received non-retryable %d status code: %w", statusCode, errors.ErrDownloadFailure)
			}
		}

		bodyBytes, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			return retry.Retryable(xerrors.Errorf("failed to read body: %w", err))
		}

		block = new(api.Block)
		blockData, err := storage_utils.Decompress(bodyBytes, blockFile.Compression)
		if err != nil {
			return xerrors.Errorf("failed to decompress block data with type %v: %w", blockFile.Compression.String(), err)
		}

		if err := proto.Unmarshal(blockData, block); err != nil {
			return xerrors.Errorf("failed to unmarshal file contents: %w", err)
		}
		return finalizer.Close()
	}); err != nil {
		return nil, err
	}

	return block, nil
}

func (c *blockDownloaderImpl) logDuration(start time.Time) {
	c.logger.Debug(
		"downloader.request",
		zap.Duration("duration", time.Since(start)),
	)
}
