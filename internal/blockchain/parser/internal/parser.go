package internal

import (
	"context"
	"encoding/json"
	"io"
	"iter"
	"os"

	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Parser interface {
		ParseNativeBlock(ctx context.Context, rawBlock *api.Block, opts ...ParseOption) (*api.NativeBlock, error)
		GetNativeTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error)
		ParseRosettaBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error)
		CompareNativeBlocks(ctx context.Context, height uint64, expectedBlock, actualBlock *api.NativeBlock) error
		// ValidateBlock Given a native block, validates whether the block data is cryptographically correct.
		ValidateBlock(ctx context.Context, nativeBlock *api.NativeBlock) error
		// ValidateAccountState Given an account's state verification request and the target block, verifies that the account state is valid. If successful, return the stored account state. Otherwise, return error.
		ValidateAccountState(ctx context.Context, req *api.ValidateAccountStateRequest) (*api.ValidateAccountStateResponse, error)
		// ValidateRosettaBlock Given other block source (native, etc), validates whether transaction operations show the correct balance transfer.
		ValidateRosettaBlock(ctx context.Context, req *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error
		// ParseStreamNative returns a chain-agnostic NativeStreamedBlock
		// over the spooled bytes. Dispatches per chain internally:
		// for bitcoin-family chains the returned block's GetBitcoin()
		// is populated; for other (or unimplemented) chains all
		// Get<Chain>() accessors return nil and callers should fall
		// back to GetBlock + ParseNativeBlock.
		//
		// Ownership of the SpooledBlock transfers to the returned
		// stream — calling stream.Close() also closes the spool. On
		// error, the caller retains ownership and must close the
		// spool itself.
		ParseStreamNative(ctx context.Context, spooled *downloader.SpooledBlock, opts ...ParseOption) (NativeStreamedBlock, error)
	}

	NativeParser interface {
		ParseBlock(ctx context.Context, rawBlock *api.Block, opts ...ParseOption) (*api.NativeBlock, error)
		GetTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error)
	}

	RosettaParser interface {
		ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error)
	}

	// TrustlessValidator validates the block payload and account state using cryptographic algorithms.
	// The goal is to have the level of cryptographic security that is comparable to the native blockchain.
	TrustlessValidator interface {
		ValidateBlock(ctx context.Context, nativeBlock *api.NativeBlock) error
		ValidateAccountState(ctx context.Context, req *api.ValidateAccountStateRequest) (*api.ValidateAccountStateResponse, error)
	}

	Params struct {
		fx.In
		fxparams.Params
		Aleo            ParserFactory `name:"aleo" optional:"true"`
		Bitcoin         ParserFactory `name:"bitcoin" optional:"true"`
		Bsc             ParserFactory `name:"bsc" optional:"true"`
		Ethereum        ParserFactory `name:"ethereum" optional:"true"`
		Rosetta         ParserFactory `name:"rosetta" optional:"true"`
		Solana          ParserFactory `name:"solana" optional:"true"`
		Polygon         ParserFactory `name:"polygon" optional:"true"`
		Avacchain       ParserFactory `name:"avacchain" optional:"true"`
		Arbitrum        ParserFactory `name:"arbitrum" optional:"true"`
		Optimism        ParserFactory `name:"optimism" optional:"true"`
		Fantom          ParserFactory `name:"fantom" optional:"true"`
		Base            ParserFactory `name:"base" optional:"true"`
		Aptos           ParserFactory `name:"aptos" optional:"true"`
		EthereumBeacon  ParserFactory `name:"ethereum/beacon" optional:"true"`
		CosmosStaking   ParserFactory `name:"cosmos/staking" optional:"true"`
		CardanoStaking  ParserFactory `name:"cardano/staking" optional:"true"`
		Tron            ParserFactory `name:"tron" optional:"true"`
		Story           ParserFactory `name:"story" optional:"true"`
		EthereumClassic ParserFactory `name:"ethereumclassic" optional:"true"`
		Plasma          ParserFactory `name:"plasma" optional:"true"`
		Monad           ParserFactory `name:"monad" optional:"true"`
		Abstract        ParserFactory `name:"abstract" optional:"true"`
		Megaeth         ParserFactory `name:"megaeth" optional:"true"`
		Seismic         ParserFactory `name:"seismic" optional:"true"`
		Dash            ParserFactory `name:"dash" optional:"true"`
		Zcash           ParserFactory `name:"zcash" optional:"true"`
		Tempo           ParserFactory `name:"tempo" optional:"true"`
		Robinhood       ParserFactory `name:"robinhood" optional:"true"`
		Arc             ParserFactory `name:"arc" optional:"true"`
	}

	ParserParams struct {
		fx.In
		fxparams.Params
	}

	parserImpl struct {
		nativeParser  NativeParser
		rosettaParser RosettaParser
		checker       Checker
		validator     TrustlessValidator
	}
)

func NewParser(params Params) (Parser, error) {
	var factory ParserFactory
	blockchain := params.Config.Chain.Blockchain
	sidechain := params.Config.Chain.Sidechain
	if sidechain == api.SideChain_SIDECHAIN_NONE {
		switch blockchain {
		case common.Blockchain_BLOCKCHAIN_BITCOIN, common.Blockchain_BLOCKCHAIN_BITCOINCASH, common.Blockchain_BLOCKCHAIN_LITECOIN, common.Blockchain_BLOCKCHAIN_DOGECOIN:
			factory = params.Bitcoin
		case common.Blockchain_BLOCKCHAIN_ZCASH:
			factory = params.Zcash
		case common.Blockchain_BLOCKCHAIN_DASH:
			factory = params.Dash
		case common.Blockchain_BLOCKCHAIN_BSC:
			factory = params.Bsc
		case common.Blockchain_BLOCKCHAIN_ETHEREUM:
			factory = params.Ethereum
		case common.Blockchain_BLOCKCHAIN_SOLANA:
			factory = params.Solana
		case common.Blockchain_BLOCKCHAIN_POLYGON:
			factory = params.Polygon
		case common.Blockchain_BLOCKCHAIN_AVACCHAIN:
			factory = params.Avacchain
		case common.Blockchain_BLOCKCHAIN_ARBITRUM:
			factory = params.Arbitrum
		case common.Blockchain_BLOCKCHAIN_OPTIMISM:
			factory = params.Optimism
		case common.Blockchain_BLOCKCHAIN_BASE:
			factory = params.Base
		case common.Blockchain_BLOCKCHAIN_FANTOM:
			factory = params.Fantom
		case common.Blockchain_BLOCKCHAIN_APTOS:
			factory = params.Aptos
		case common.Blockchain_BLOCKCHAIN_TRON:
			factory = params.Tron
		case common.Blockchain_BLOCKCHAIN_STORY:
			factory = params.Story
		case common.Blockchain_BLOCKCHAIN_ETHEREUMCLASSIC:
			factory = params.EthereumClassic
		case common.Blockchain_BLOCKCHAIN_PLASMA:
			factory = params.Plasma
		case common.Blockchain_BLOCKCHAIN_MONAD:
			factory = params.Monad
		case common.Blockchain_BLOCKCHAIN_ABSTRACT:
			factory = params.Abstract
		case common.Blockchain_BLOCKCHAIN_MEGAETH:
			factory = params.Megaeth
		case common.Blockchain_BLOCKCHAIN_SEISMIC:
			factory = params.Seismic
		case common.Blockchain_BLOCKCHAIN_TEMPO:
			factory = params.Tempo
		case common.Blockchain_BLOCKCHAIN_ROBINHOOD:
			factory = params.Robinhood
		case common.Blockchain_BLOCKCHAIN_ARC:
			factory = params.Arc
		default:
			if params.Config.IsRosetta() {
				factory = params.Rosetta
			}
		}
	} else {
		switch sidechain {
		case api.SideChain_SIDECHAIN_ETHEREUM_MAINNET_BEACON, api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON:
			factory = params.EthereumBeacon
		}
	}

	if factory == nil {
		return nil, xerrors.Errorf("parser is not implemented: blockchain(%v)-sidechain(%v)", blockchain, sidechain)
	}

	parser, err := factory.NewParser()
	if err != nil {
		return nil, xerrors.Errorf("failed to create parser: %w", err)
	}

	scope := params.Metrics
	logger := log.WithPackage(params.Logger)
	parser = WithInstrumentInterceptor(parser, scope, logger)
	return parser, nil
}

func (p *parserImpl) ParseNativeBlock(ctx context.Context, rawBlock *api.Block, opts ...ParseOption) (*api.NativeBlock, error) {
	return p.nativeParser.ParseBlock(ctx, rawBlock, opts...)
}

func (p *parserImpl) GetNativeTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error) {
	return p.nativeParser.GetTransaction(ctx, nativeBlock, transactionHash)
}

func (p *parserImpl) ParseRosettaBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	return p.rosettaParser.ParseBlock(ctx, rawBlock)
}

func (p *parserImpl) CompareNativeBlocks(ctx context.Context, height uint64, expectedBlock, actualBlock *api.NativeBlock) error {
	return p.checker.CompareNativeBlocks(ctx, height, expectedBlock, actualBlock)
}

func (p *parserImpl) ValidateBlock(ctx context.Context, nativeBlock *api.NativeBlock) error {
	return p.validator.ValidateBlock(ctx, nativeBlock)
}

func (p *parserImpl) ValidateAccountState(ctx context.Context, req *api.ValidateAccountStateRequest) (*api.ValidateAccountStateResponse, error) {
	return p.validator.ValidateAccountState(ctx, req)
}

func (p *parserImpl) ValidateRosettaBlock(ctx context.Context, req *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error {
	return p.checker.ValidateRosettaBlock(ctx, req, actualRosettaBlock)
}

func (p *parserImpl) ParseStreamNative(ctx context.Context, spooled *downloader.SpooledBlock, opts ...ParseOption) (NativeStreamedBlock, error) {
	if spooled == nil {
		return nil, xerrors.New("nil spooled block")
	}

	// Skipped blocks have no payload. Return an empty native stream
	// carrying just the metadata synthesized from BlockFile. If the
	// underlying chain is bitcoin-family, the skipped-bitcoin inner
	// stream yields no transactions; otherwise GetBitcoin() is nil.
	if spooled.BlockFile.GetSkipped() {
		metadata := &api.BlockMetadata{
			Tag:     spooled.BlockFile.Tag,
			Height:  spooled.BlockFile.Height,
			Skipped: true,
		}
		wrapper := &nativeStreamedBlock{metadata: metadata, spooled: spooled}
		if _, ok := p.nativeParser.(BitcoinStreamer); ok {
			wrapper.bitcoin = skippedBitcoinNativeStream{}
		}
		if _, ok := p.nativeParser.(SolanaStreamer); ok {
			wrapper.solana = skippedSolanaNativeStream{}
		}
		return wrapper, nil
	}

	// Bitcoin-family: walk the envelope, wire loaders, delegate to
	// the native parser's StreamBlockIter.
	if streamer, ok := p.nativeParser.(BitcoinStreamer); ok {
		return buildBitcoinNativeStreamedBlock(ctx, spooled, streamer, opts...)
	}

	// Solana: walk the envelope, stream the getBlock JSON from the
	// spool, delegate to the native parser's StreamBlockIter.
	if streamer, ok := p.nativeParser.(SolanaStreamer); ok {
		return buildSolanaNativeStreamedBlock(ctx, spooled, streamer, opts...)
	}

	// No chain-specific streaming available. Return metadata-only
	// wrapper; caller falls back to GetBlock + ParseNativeBlock.
	r, err := spooled.Open()
	if err != nil {
		return nil, xerrors.Errorf("open spool for metadata walk: %w", err)
	}
	defer func() { _ = r.Close() }()
	block, err := api.WalkBlockEnvelope(r)
	if err != nil {
		return nil, xerrors.Errorf("walk block envelope: %w", err)
	}
	return &nativeStreamedBlock{
		metadata: block.GetMetadata(),
		spooled:  spooled,
	}, nil
}

// nativeStreamedBlock is the chain-agnostic NativeStreamedBlock
// implementation. Per-chain inner streams are populated when the
// configured chain's native parser supports streaming; otherwise the
// corresponding Get<Chain>() returns nil.
type nativeStreamedBlock struct {
	metadata *api.BlockMetadata
	spooled  *downloader.SpooledBlock
	// spoolHandle is a cached file handle over spooled used by the
	// bitcoin loaders' ReadAt calls (so per-tx seek+read doesn't
	// pay an Open syscall). Closed on Close() alongside the spool.
	// Populated only for bitcoin-family streams.
	spoolHandle io.Closer
	bitcoin     BitcoinNativeStream
	// ethereum is always nil today; added when the ethereum streaming
	// walker lands.
	ethereum EthereumNativeStream
	// solana is populated only for Solana streams.
	solana SolanaNativeStream
}

func (n *nativeStreamedBlock) GetMetadata() *api.BlockMetadata { return n.metadata }
func (n *nativeStreamedBlock) Close() error {
	if n.spoolHandle != nil {
		_ = n.spoolHandle.Close()
	}
	return n.spooled.Close()
}
func (n *nativeStreamedBlock) GetBitcoin() BitcoinNativeStream {
	if n == nil {
		return nil
	}
	return n.bitcoin
}
func (n *nativeStreamedBlock) GetEthereum() EthereumNativeStream {
	if n == nil {
		return nil
	}
	return n.ethereum
}
func (n *nativeStreamedBlock) GetSolana() SolanaNativeStream {
	if n == nil {
		return nil
	}
	return n.solana
}

// skippedBitcoinNativeStream is the bitcoin inner stream returned for
// skipped bitcoin-family blocks. Yields no transactions and reports
// "no header" on Header().
type skippedBitcoinNativeStream struct{}

func (skippedBitcoinNativeStream) Transactions() iter.Seq2[*api.BitcoinTransaction, error] {
	return func(yield func(*api.BitcoinTransaction, error) bool) {}
}
func (skippedBitcoinNativeStream) Header() (*api.BitcoinHeader, error) {
	return nil, xerrors.New("skipped block has no header")
}

// buildBitcoinNativeStreamedBlock walks the bitcoin proto envelope,
// wires lazy seek-based loaders over the spool, and delegates to the
// native parser's StreamBlockIter. The returned NativeStreamedBlock
// has GetBitcoin() populated and GetEthereum() nil.
func buildBitcoinNativeStreamedBlock(ctx context.Context, spooled *downloader.SpooledBlock, streamer BitcoinStreamer, opts ...ParseOption) (NativeStreamedBlock, error) {
	r, err := spooled.Open()
	if err != nil {
		return nil, xerrors.Errorf("open spool for walk: %w", err)
	}
	block, chunks, walkErr := api.WalkBitcoinEnvelope(r)
	_ = r.Close()
	if walkErr != nil {
		return nil, xerrors.Errorf("walk bitcoin envelope: %w", walkErr)
	}
	if chunks.Header.Length == 0 && chunks.Header.Offset == 0 {
		return nil, xerrors.Errorf("bitcoin block at height %d has no header", spooled.BlockFile.GetHeight())
	}

	// Cache a single *os.File handle over the spool so header and
	// per-tx loaders use pread (ReadAt) instead of repeated
	// Open+Seek+Read+Close. For a many-tx block this cuts ~4
	// syscalls per tx × N txs into one open + N preads, making a
	// meaningful difference under concurrent workloads.
	handle, err := spooled.Open()
	if err != nil {
		return nil, xerrors.Errorf("open spool for loaders: %w", err)
	}
	readerAt, ok := handle.(io.ReaderAt)
	if !ok {
		_ = handle.Close()
		return nil, xerrors.Errorf("spool reader does not support ReadAt")
	}

	openHeader := func() (io.ReadCloser, error) {
		return io.NopCloser(io.NewSectionReader(readerAt, chunks.Header.Offset, chunks.Header.Length)), nil
	}

	groups := chunks.InputTransactionsGroups
	loadGroup := func(i int) (*api.RepeatedBytes, error) {
		if i < 0 || i >= len(groups) {
			return nil, nil
		}
		ref := groups[i]
		b := make([]byte, ref.Length)
		if _, err := readerAt.ReadAt(b, ref.Offset); err != nil && err != io.EOF {
			return nil, xerrors.Errorf("read input tx group [%d]: %w", i, err)
		}
		rb := &api.RepeatedBytes{}
		if err := proto.Unmarshal(b, rb); err != nil {
			return nil, xerrors.Errorf("unmarshal input tx group [%d]: %w", i, err)
		}
		return rb, nil
	}

	inner := streamer.StreamBlockIter(ctx, openHeader, loadGroup, opts...)
	return &nativeStreamedBlock{
		metadata:    block.GetMetadata(),
		spooled:     spooled,
		spoolHandle: handle,
		bitcoin:     inner,
	}, nil
}

// skippedSolanaNativeStream is the Solana inner stream returned for
// skipped slots. Skipped slots are routine on Solana (the leader
// produced no block), so a range walker sees an empty transaction
// stream and an empty rewards list rather than a nil accessor; only
// Header() reports the absence, matching the bitcoin precedent.
type skippedSolanaNativeStream struct{}

func (skippedSolanaNativeStream) Transactions() iter.Seq2[*api.SolanaTransactionV2, error] {
	return func(yield func(*api.SolanaTransactionV2, error) bool) {}
}
func (skippedSolanaNativeStream) RawTransactions() iter.Seq2[json.RawMessage, error] {
	return func(yield func(json.RawMessage, error) bool) {}
}
func (skippedSolanaNativeStream) Header() (*api.SolanaHeader, error) {
	return nil, xerrors.New("skipped block has no header")
}
func (skippedSolanaNativeStream) Rewards() ([]*api.SolanaReward, error) {
	return nil, nil
}

// buildSolanaNativeStreamedBlock walks the Solana proto envelope, wires
// a section reader over the header's byte range in the spool, and
// delegates to the native parser's StreamBlockIter. The returned
// NativeStreamedBlock has GetSolana() populated and the other accessors
// nil.
func buildSolanaNativeStreamedBlock(ctx context.Context, spooled *downloader.SpooledBlock, streamer SolanaStreamer, opts ...ParseOption) (NativeStreamedBlock, error) {
	r, err := spooled.Open()
	if err != nil {
		return nil, xerrors.Errorf("open spool for walk: %w", err)
	}
	block, chunks, walkErr := api.WalkSolanaEnvelope(r)
	_ = r.Close()
	if walkErr != nil {
		return nil, xerrors.Errorf("walk solana envelope: %w", walkErr)
	}
	if chunks.Header.Length == 0 && chunks.Header.Offset == 0 {
		return nil, xerrors.Errorf("solana block at height %d has no header", spooled.BlockFile.GetHeight())
	}
	// ParseBlock only understands the tag-2 (jsonParsed) getBlock
	// layout; keep the streaming path on the same contract.
	if block.GetMetadata().GetTag() < 2 {
		return nil, ErrNotImplemented
	}

	// One cached handle over the spool so the header section reader
	// uses pread instead of Open+Seek+Read+Close per pass.
	handle, err := spooled.Open()
	if err != nil {
		return nil, xerrors.Errorf("open spool for header reader: %w", err)
	}
	readerAt, ok := handle.(io.ReaderAt)
	if !ok {
		_ = handle.Close()
		return nil, xerrors.Errorf("spool reader does not support ReadAt")
	}
	openHeader := func() (io.ReadCloser, error) {
		return io.NopCloser(io.NewSectionReader(readerAt, chunks.Header.Offset, chunks.Header.Length)), nil
	}

	inner := streamer.StreamBlockIter(ctx, openHeader, block.GetMetadata().GetHeight(), opts...)
	return &nativeStreamedBlock{
		metadata:    block.GetMetadata(),
		spooled:     spooled,
		spoolHandle: handle,
		solana:      inner,
	}, nil
}

// seekedHeaderReader is an io.ReadCloser that reads up to `limit`
// bytes from `f` (an io.ReadCloser) and closes `f` on Close.
type seekedHeaderReader struct {
	f     io.ReadCloser
	limit io.Reader
}

func (r *seekedHeaderReader) Read(p []byte) (int, error) { return r.limit.Read(p) }
func (r *seekedHeaderReader) Close() error               { return r.f.Close() }

// Ensure os.File satisfies the reader contract we seek against.
var _ io.ReadSeekCloser = (*os.File)(nil)
