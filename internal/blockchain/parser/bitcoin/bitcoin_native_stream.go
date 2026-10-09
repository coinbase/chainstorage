package bitcoin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"sync"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// InputTxGroupLoader returns the prev-output transactions
// corresponding to the inputs of the i-th block transaction. The
// returned RepeatedBytes may be nil for txs with no inputs (e.g.
// coinbase) or when no input_transactions are available for the
// block.
//
// Callers may implement this against in-memory data (adapting
// *api.BitcoinBlobdata.InputTransactions) or against a spooled copy
// of the proto bytes (per-group seek+proto.Unmarshal). The lazy
// spool-based implementation is what drops parser peak RAM from
// ~O(block) to ~O(largest tx's input_transactions) on zcash.
//
// Alias of internal.BitcoinInputTxGroupLoader so the streamer
// interface and its bitcoin-family implementation share the same
// underlying type.
type InputTxGroupLoader = internal.BitcoinInputTxGroupLoader

// NewInMemoryInputTxGroupLoader adapts the legacy in-memory
// []*api.RepeatedBytes layout to the loader interface. Memory
// characteristics are unchanged vs directly passing the blobdata.
func NewInMemoryInputTxGroupLoader(groups []*api.RepeatedBytes) InputTxGroupLoader {
	return func(i int) (*api.RepeatedBytes, error) {
		if i < 0 || i >= len(groups) {
			return nil, nil
		}
		return groups[i], nil
	}
}

// BlockStream is a bitcoin-package alias for internal.BitcoinNativeStream.
// The stream is an iterator-based view over a bitcoin-family block.
//
// Ownership of backing resources (spool file, decompressors) lives
// above this type — see internal.NativeStreamedBlock (returned by
// Parser.ParseStreamNative) for the full contract that includes Close.
//
// # Header() and the "free after iteration" optimization
//
// Header() can be called before, during, or after Transactions()
// iteration. The cost depends on the order of operations:
//
//   - Call Header() AFTER Transactions() has been fully consumed: free.
//     The header is captured as a side effect of the streaming parse
//     (we already had to reconstitute it to return from the underlying
//     decode pass). Subsequent Header() calls return the cached value.
//
//   - Call Header() BEFORE (or WITHOUT) iterating: a dedicated
//     header-only pass over the block's JSON is performed once. The
//     pass walks top-level fields and skips the "tx" array via
//     token-depth tracking without decoding its elements, then
//     reconstitutes a BitcoinHeader from the remaining fields. The
//     result is cached.
//
//   - Call Header() DURING iteration (e.g. mid-range inside the loop):
//     treated as "before" — the dedicated pass runs. Avoid this if
//     possible.
//
// Callers that need both transactions and header should iterate first
// and read the header after the range loop to avoid the extra pass.
//
// BlockStream is not safe for concurrent use across goroutines. The
// typical single-goroutine range-loop pattern is always safe.
type BlockStream = internal.BitcoinNativeStream

// errIterStopped is an internal sentinel used to unwind the decode pass
// when the iterator consumer breaks. It is not exposed to callers.
var errIterStopped = errors.New("bitcoin: block stream iteration stopped by consumer")

type bitcoinBlockStream struct {
	ctx        context.Context
	parser     *bitcoinNativeParserImpl
	loadGroup  InputTxGroupLoader
	opts       []internal.ParseOption
	openReader func() (io.ReadCloser, error)

	mu        sync.Mutex
	header    *api.BitcoinHeader
	headerErr error
	headerSet bool
}

// StreamBlockIter returns a BlockStream that consumes block JSON via
// the supplied reader factory. The factory may be called more than
// once — once for each Transactions() iteration and at most once more
// for an early Header() call. It must return a freshly-positioned
// reader on each call.
//
// loadGroup resolves prev-output transactions for each block tx. It
// is invoked once per tx during iteration; callers that populate
// InputTransactions in memory can use NewInMemoryInputTxGroupLoader,
// while the disk-spooled DownloadStreamBitcoin path hands in a
// loader that seeks into the spool file on demand. The loader's
// return value for each call is allowed to be GC'd as soon as the
// tx is yielded, so peak parser memory is bounded by the largest
// single tx's prev-output bytes.
func (b *bitcoinNativeParserImpl) StreamBlockIter(
	ctx context.Context,
	openReader func() (io.ReadCloser, error),
	loadGroup InputTxGroupLoader,
	opts ...internal.ParseOption,
) BlockStream {
	return &bitcoinBlockStream{
		ctx:        ctx,
		parser:     b,
		loadGroup:  loadGroup,
		opts:       opts,
		openReader: openReader,
	}
}

func (s *bitcoinBlockStream) Transactions() iter.Seq2[*api.BitcoinTransaction, error] {
	return func(yield func(*api.BitcoinTransaction, error) bool) {
		if s.openReader == nil {
			yield(nil, xerrors.New("nil openReader"))
			return
		}

		r, err := s.openReader()
		if err != nil {
			yield(nil, xerrors.Errorf("failed to open block reader: %w", err))
			return
		}
		defer func() { _ = r.Close() }()

		emit := func(tx *api.BitcoinTransaction) error {
			if !yield(tx, nil) {
				return errIterStopped
			}
			return nil
		}

		hdr, err := s.parser.decodeBlockStream(s.ctx, r, s.loadGroup, emit, s.opts...)
		switch {
		case errors.Is(err, errIterStopped):
			// Consumer broke the range. Nothing to report; defers clean up.
		case err != nil:
			yield(nil, err)
		default:
			// Full consumption: cache header for a free subsequent Header() call.
			s.mu.Lock()
			if !s.headerSet {
				s.header = hdr
				s.headerSet = true
			}
			s.mu.Unlock()
		}
	}
}

func (s *bitcoinBlockStream) Header() (*api.BitcoinHeader, error) {
	s.mu.Lock()
	if s.headerSet {
		defer s.mu.Unlock()
		return s.header, s.headerErr
	}
	s.mu.Unlock()

	// Slow path: dedicated header-only scan.
	r, err := s.openReader()
	if err != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.headerSet = true
		s.headerErr = xerrors.Errorf("failed to open block reader for header scan: %w", err)
		return nil, s.headerErr
	}
	defer func() { _ = r.Close() }()

	hdr, err := parseBitcoinHeaderSkippingTx(r)

	s.mu.Lock()
	defer s.mu.Unlock()
	// Another caller may have raced to populate the header via iteration.
	// In that case keep the iteration-derived value (it is equivalent).
	if !s.headerSet {
		s.header = hdr
		s.headerErr = err
		s.headerSet = true
	}
	return s.header, s.headerErr
}

// decodeBlockStream decodes a bitcoin-family block JSON from r, invoking
// emit once per transaction as the stream progresses. Only a single
// BitcoinTransaction is held in memory at a time (plus a small buffer of
// the non-"tx" header fields), so peak parser memory is
// O(largest_tx + header) rather than O(block).
//
// The returned header is available only after all transactions are
// emitted, because tx may precede or follow header fields in JSON.
func (b *bitcoinNativeParserImpl) decodeBlockStream(
	ctx context.Context,
	r io.Reader,
	loadGroup InputTxGroupLoader,
	emit func(*api.BitcoinTransaction) error,
	opts ...internal.ParseOption,
) (*api.BitcoinHeader, error) {
	optView := internal.ResolveParseOptions(opts)
	// Caller-supplied WithTransactionFilter, applied after the chain's own
	// txFilter (Zcash shielded drop) on the same raw element. Honored here
	// so the option is not a silent no-op on bitcoin-family chains.
	optFilter := optView.TransactionFilter()

	dec := json.NewDecoder(r)
	openTok, err := dec.Token()
	if err != nil {
		return nil, xerrors.Errorf("failed to read opening token: %w", err)
	}
	if d, ok := openTok.(json.Delim); !ok || d != '{' {
		return nil, xerrors.Errorf("expected '{' at block start, got %v", openTok)
	}

	headerFields := make(map[string]json.RawMessage)
	txIdx := 0

	for dec.More() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keyTok, err := dec.Token()
		if err != nil {
			return nil, xerrors.Errorf("failed to read key token: %w", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, xerrors.Errorf("expected string key, got %v", keyTok)
		}

		if key == "tx" {
			openArr, err := dec.Token()
			if err != nil {
				return nil, xerrors.Errorf("failed to read tx opening: %w", err)
			}
			if d, ok := openArr.(json.Delim); !ok || d != '[' {
				return nil, xerrors.Errorf("expected '[' for tx, got %v", openArr)
			}
			for dec.More() {
				// When a chain-specific txFilter is set (e.g. Zcash
				// shielded-tx drop), decode as raw JSON first so
				// the filter can inspect fields not present in the
				// shared BitcoinTransaction struct. Otherwise decode
				// directly into BitcoinTransaction to avoid a
				// two-stage decode.
				//
				// txIdx is always incremented (including for
				// filtered txs) to preserve the source position as
				// the tx's index, matching ParseBlock.
				var rawTx BitcoinTransaction
				if b.txFilter != nil || optFilter != nil {
					var rawMsg json.RawMessage
					if err := dec.Decode(&rawMsg); err != nil {
						return nil, xerrors.Errorf("failed to decode tx[%d]: %w", txIdx, err)
					}
					keep := true
					if b.txFilter != nil {
						var err error
						if keep, err = b.txFilter(rawMsg); err != nil {
							return nil, xerrors.Errorf("tx filter failed at [%d]: %w", txIdx, err)
						}
					}
					if keep && optFilter != nil {
						var err error
						if keep, err = optFilter(rawMsg); err != nil {
							return nil, xerrors.Errorf("transaction filter failed at [%d]: %w", txIdx, err)
						}
					}
					if !keep {
						txIdx++
						continue
					}
					if err := json.Unmarshal(rawMsg, &rawTx); err != nil {
						return nil, xerrors.Errorf("failed to unmarshal tx[%d]: %w", txIdx, err)
					}
				} else {
					if err := dec.Decode(&rawTx); err != nil {
						return nil, xerrors.Errorf("failed to decode tx[%d]: %w", txIdx, err)
					}
				}

				// Chain-specific normalization (e.g. Dash/Zcash
				// hash backfill) before conversion.
				if b.preprocessTx != nil {
					b.preprocessTx(&rawTx)
				}

				// Lazy: load only this tx's prev-output group,
				// build a per-tx metadata map, and let it go
				// out of scope immediately after parsing.
				var metadataMap map[string][]*api.BitcoinTransactionOutput
				if loadGroup != nil {
					group, err := loadGroup(txIdx)
					if err != nil {
						return nil, xerrors.Errorf("load input tx group [%d]: %w", txIdx, err)
					}
					metadataMap, err = b.buildMetadataForGroup(group, optView)
					if err != nil {
						return nil, xerrors.Errorf("build metadata for tx[%d]: %w", txIdx, err)
					}
				}

				apiTx, err := rawTx.ToApiBitcoinTransaction(txIdx, metadataMap, b.p2pkhVersionByte, optView)
				if err != nil {
					return nil, xerrors.Errorf("failed to convert tx[%d]: %w", txIdx, err)
				}
				if err := emit(apiTx); err != nil {
					return nil, err
				}
				txIdx++
			}
			if _, err := dec.Token(); err != nil {
				return nil, xerrors.Errorf("failed to read tx closing: %w", err)
			}
			continue
		}

		// Header fields are small; buffer as json.RawMessage and reassemble
		// into a BitcoinBlock at the end. This is well under 1 KB total.
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, xerrors.Errorf("failed to decode header field %q: %w", key, err)
		}
		headerFields[key] = raw
	}
	if _, err := dec.Token(); err != nil {
		return nil, xerrors.Errorf("failed to read block closing: %w", err)
	}

	// Reconstitute the header. Remove "tx" so validation does not require it
	// to be present here; transactions were already streamed.
	delete(headerFields, "tx")
	headerJSON, err := json.Marshal(headerFields)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal header fields: %w", err)
	}
	var block BitcoinBlock
	if err := json.Unmarshal(headerJSON, &block); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal header: %w", err)
	}
	return block.GetApiBitcoinHeader(), nil
}

// parseBitcoinHeaderSkippingTx performs a one-pass JSON scan over r,
// buffering every top-level field except "tx" (which is skipped via
// token-depth tracking — no transaction values are allocated). The
// remaining fields are reassembled into a BitcoinBlock struct and the
// derived header is returned.
func parseBitcoinHeaderSkippingTx(r io.Reader) (*api.BitcoinHeader, error) {
	dec := json.NewDecoder(r)
	openTok, err := dec.Token()
	if err != nil {
		return nil, xerrors.Errorf("read opening token: %w", err)
	}
	if d, ok := openTok.(json.Delim); !ok || d != '{' {
		return nil, xerrors.Errorf("expected '{' at block start, got %v", openTok)
	}

	headerFields := make(map[string]json.RawMessage)
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, xerrors.Errorf("read key token: %w", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, xerrors.Errorf("expected string key, got %v", keyTok)
		}
		if key == "tx" {
			if err := skipJSONValue(dec); err != nil {
				return nil, xerrors.Errorf("skip tx array: %w", err)
			}
			continue
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, xerrors.Errorf("decode header field %q: %w", key, err)
		}
		headerFields[key] = raw
	}
	if _, err := dec.Token(); err != nil {
		return nil, xerrors.Errorf("read closing token: %w", err)
	}

	headerJSON, err := json.Marshal(headerFields)
	if err != nil {
		return nil, xerrors.Errorf("marshal header fields: %w", err)
	}
	var block BitcoinBlock
	if err := json.Unmarshal(headerJSON, &block); err != nil {
		return nil, xerrors.Errorf("unmarshal header: %w", err)
	}
	return block.GetApiBitcoinHeader(), nil
}

// skipJSONValue consumes exactly one JSON value (scalar or container)
// from dec's current position, without allocating any sub-values.
func skipJSONValue(dec *json.Decoder) error {
	depth := 0
	for {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		d, ok := tok.(json.Delim)
		if !ok {
			if depth == 0 {
				// Scalar at the top level — we're done.
				return nil
			}
			continue
		}
		switch d {
		case '[', '{':
			depth++
		case ']', '}':
			depth--
			if depth == 0 {
				return nil
			}
		}
	}
}
