package solana

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

// Top-level keys of the getBlock JSON that are not header fields.
const (
	blockJSONKeyTransactions = "transactions"
	blockJSONKeyRewards      = "rewards"
)

// BlockStream is a solana-package alias for internal.SolanaNativeStream:
// an iterator-based view over one slot's getBlock JSON.
//
// Ownership of backing resources (spool file) lives above this type —
// see internal.NativeStreamedBlock (returned by Parser.ParseStreamNative)
// for the full contract that includes Close.
//
// # Header(), Rewards() and the "free after iteration" optimization
//
// The header fields and the block-level rewards array are captured as a
// side effect of a full Transactions() pass, so calling Header() or
// Rewards() AFTER the range loop is free. Calling either BEFORE (or
// without) iterating runs one dedicated pass that skips the transaction
// array via token-depth tracking without decoding its elements; the
// result is cached. Calling them DURING iteration is treated as
// "before". Rewards are buffered as raw JSON during the transaction
// pass (a few hundred KB on epoch-boundary slots, bytes otherwise), so
// Transactions() stays O(largest transaction).
//
// BlockStream is not safe for concurrent use across goroutines. The
// typical single-goroutine range-loop pattern is always safe.
type BlockStream = internal.SolanaNativeStream

// errIterStopped is an internal sentinel used to unwind the decode pass
// when the iterator consumer breaks. It is not exposed to callers.
var errIterStopped = errors.New("solana: block stream iteration stopped by consumer")

// blockTail is everything a pass over the block JSON produces besides
// the transactions: the reconstituted header and the block rewards.
type blockTail struct {
	header  *api.SolanaHeader
	rewards []*api.SolanaReward
}

type solanaBlockStream struct {
	ctx        context.Context
	parser     *solanaNativeParserImpl
	slot       uint64
	opts       []internal.ParseOption
	openReader func() (io.ReadCloser, error)

	mu      sync.Mutex
	tail    *blockTail
	tailErr error
	tailSet bool
}

// StreamBlockIter returns a BlockStream that consumes the slot's getBlock
// JSON via the supplied reader factory. The factory may be called more
// than once — once for each Transactions() iteration and at most once
// more for an early Header() or Rewards() call — and must return a
// freshly-positioned reader on each call.
//
// slot is the block height from the envelope metadata: the one header
// field the JSON does not carry (ParseBlock reads it from Block.Metadata
// the same way).
func (p *solanaNativeParserImpl) StreamBlockIter(
	ctx context.Context,
	openReader func() (io.ReadCloser, error),
	slot uint64,
	opts ...internal.ParseOption,
) BlockStream {
	return &solanaBlockStream{
		ctx:        ctx,
		parser:     p,
		slot:       slot,
		opts:       opts,
		openReader: openReader,
	}
}

func (s *solanaBlockStream) Transactions() iter.Seq2[*api.SolanaTransactionV2, error] {
	return func(yield func(*api.SolanaTransactionV2, error) bool) {
		emitTx := func(tx *api.SolanaTransactionV2) error {
			if !yield(tx, nil) {
				return errIterStopped
			}
			return nil
		}
		if err := s.run(nil, emitTx); err != nil {
			yield(nil, err)
		}
	}
}

func (s *solanaBlockStream) RawTransactions() iter.Seq2[json.RawMessage, error] {
	return func(yield func(json.RawMessage, error) bool) {
		emitRaw := func(raw json.RawMessage) error {
			if !yield(raw, nil) {
				return errIterStopped
			}
			return nil
		}
		if err := s.run(emitRaw, nil); err != nil {
			yield(nil, err)
		}
	}
}

// run opens the reader, performs one full walk with the given emitter,
// and caches the tail on full consumption. It returns nil when the
// consumer stopped early (nothing to report) and the walk error
// otherwise.
func (s *solanaBlockStream) run(emitRaw func(json.RawMessage) error, emitTx func(*api.SolanaTransactionV2) error) error {
	if s.openReader == nil {
		return xerrors.New("nil openReader")
	}

	r, err := s.openReader()
	if err != nil {
		return xerrors.Errorf("failed to open block reader: %w", err)
	}
	defer func() { _ = r.Close() }()

	tail, err := s.parser.walkBlockStream(s.ctx, r, s.slot, s.opts, emitRaw, emitTx)
	switch {
	case errors.Is(err, errIterStopped):
		// Consumer broke the range. Nothing to report; defers clean up.
		return nil
	case err != nil:
		return err
	}

	// Full consumption: cache the tail for free Header()/Rewards().
	s.mu.Lock()
	if !s.tailSet {
		s.tail = tail
		s.tailSet = true
	}
	s.mu.Unlock()
	return nil
}

func (s *solanaBlockStream) Header() (*api.SolanaHeader, error) {
	tail, err := s.getTail()
	if err != nil {
		return nil, err
	}
	return tail.header, nil
}

func (s *solanaBlockStream) Rewards() ([]*api.SolanaReward, error) {
	tail, err := s.getTail()
	if err != nil {
		return nil, err
	}
	return tail.rewards, nil
}

// getTail returns the cached tail, running the dedicated
// transactions-skipping pass once if no full iteration has populated it.
func (s *solanaBlockStream) getTail() (*blockTail, error) {
	s.mu.Lock()
	if s.tailSet {
		defer s.mu.Unlock()
		return s.tail, s.tailErr
	}
	s.mu.Unlock()

	// Slow path: dedicated header + rewards scan.
	var (
		tail *blockTail
		err  error
	)
	if s.openReader == nil {
		err = xerrors.New("nil openReader")
	} else {
		var r io.ReadCloser
		r, err = s.openReader()
		if err != nil {
			err = xerrors.Errorf("failed to open block reader for tail scan: %w", err)
		} else {
			tail, err = s.parser.decodeBlockTail(r, s.slot)
			_ = r.Close()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// Another caller may have raced to populate the tail via iteration.
	// In that case keep the iteration-derived value (it is equivalent).
	if !s.tailSet {
		s.tail = tail
		s.tailErr = err
		s.tailSet = true
	}
	return s.tail, s.tailErr
}

// decodeBlockStream is the native-decoding walk used by Transactions().
// Kept as a named entry point for tests; see walkBlockStream.
func (p *solanaNativeParserImpl) decodeBlockStream(
	ctx context.Context,
	r io.Reader,
	slot uint64,
	emit func(*api.SolanaTransactionV2) error,
	opts ...internal.ParseOption,
) (*blockTail, error) {
	return p.walkBlockStream(ctx, r, slot, opts, nil, emit)
}

// walkBlockStream token-walks a getBlock JSON object from r, invoking
// exactly one of emitRaw / emitTx once per kept transaction in source
// order. Only one transaction is held in memory at a time (plus the
// small non-array header fields and the raw rewards array), so peak
// parser memory is O(largest transaction) rather than O(block).
//
// With emitRaw set, kept transactions are yielded as their verbatim JSON
// element and never natively decoded. Otherwise each kept transaction
// goes through parseTransactionV2, the same function ParseBlock uses, so
// the two paths stay in parity (enforced by solana_native_stream_test).
func (p *solanaNativeParserImpl) walkBlockStream(
	ctx context.Context,
	r io.Reader,
	slot uint64,
	opts []internal.ParseOption,
	emitRaw func(json.RawMessage) error,
	emitTx func(*api.SolanaTransactionV2) error,
) (*blockTail, error) {
	if (emitRaw == nil) == (emitTx == nil) {
		return nil, xerrors.New("walkBlockStream needs exactly one emitter")
	}
	filter := internal.ResolveParseOptions(opts).TransactionFilter()
	// The raw path is needed whenever the element must be inspected or
	// yielded as bytes; the direct decode is only for the unfiltered
	// native iteration, where it saves one copy per transaction.
	useRaw := filter != nil || emitRaw != nil

	dec := json.NewDecoder(r)
	if err := expectDelim(dec, '{'); err != nil {
		return nil, xerrors.Errorf("block start: %w", err)
	}

	headerFields := make(map[string]json.RawMessage)
	var rewardsRaw json.RawMessage
	txIdx := 0

	for dec.More() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key, err := readKey(dec)
		if err != nil {
			return nil, err
		}

		switch key {
		case blockJSONKeyTransactions:
			if err := expectDelim(dec, '['); err != nil {
				return nil, xerrors.Errorf("transactions start: %w", err)
			}
			for dec.More() {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				// On the raw path the element is decoded as raw JSON first so
				// the filter can inspect any field (account keys, program
				// ids, meta.err) without paying for the native conversion;
				// dropped transactions never reach parseTransactionV2. txIdx
				// is always incremented so error messages name the source
				// position, filtered or not.
				var tx SolanaTransactionV2
				if useRaw {
					var raw json.RawMessage
					if err := dec.Decode(&raw); err != nil {
						return nil, xerrors.Errorf("failed to decode transaction[%d]: %w", txIdx, err)
					}
					if filter != nil {
						keep, err := filter(raw)
						if err != nil {
							return nil, xerrors.Errorf("transaction filter failed at [%d]: %w", txIdx, err)
						}
						if !keep {
							txIdx++
							continue
						}
					}
					if emitRaw != nil {
						if err := emitRaw(raw); err != nil {
							return nil, err
						}
						txIdx++
						continue
					}
					if err := json.Unmarshal(raw, &tx); err != nil {
						return nil, xerrors.Errorf("failed to unmarshal transaction[%d]: %w", txIdx, err)
					}
				} else if err := dec.Decode(&tx); err != nil {
					return nil, xerrors.Errorf("failed to decode transaction[%d]: %w", txIdx, err)
				}

				apiTx, err := p.parseTransactionV2(&tx)
				if err != nil {
					return nil, xerrors.Errorf("failed to parse transaction[%d]: %w", txIdx, err)
				}
				if err := emitTx(apiTx); err != nil {
					return nil, err
				}
				txIdx++
			}
			if err := expectDelim(dec, ']'); err != nil {
				return nil, xerrors.Errorf("transactions end: %w", err)
			}

		case blockJSONKeyRewards:
			if err := dec.Decode(&rewardsRaw); err != nil {
				return nil, xerrors.Errorf("failed to decode rewards: %w", err)
			}

		default:
			// Header fields are small scalars; buffer as json.RawMessage
			// and reassemble into a SolanaBlockV2 at the end.
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return nil, xerrors.Errorf("failed to decode header field %q: %w", key, err)
			}
			headerFields[key] = raw
		}
	}
	if err := expectDelim(dec, '}'); err != nil {
		return nil, xerrors.Errorf("block end: %w", err)
	}

	return p.buildBlockTail(slot, headerFields, rewardsRaw)
}

// decodeBlockTail performs a one-pass scan over r that buffers every
// top-level field except "transactions", which is skipped via
// token-depth tracking without allocating any transaction values.
func (p *solanaNativeParserImpl) decodeBlockTail(r io.Reader, slot uint64) (*blockTail, error) {
	dec := json.NewDecoder(r)
	if err := expectDelim(dec, '{'); err != nil {
		return nil, xerrors.Errorf("block start: %w", err)
	}

	headerFields := make(map[string]json.RawMessage)
	var rewardsRaw json.RawMessage
	for dec.More() {
		key, err := readKey(dec)
		if err != nil {
			return nil, err
		}
		switch key {
		case blockJSONKeyTransactions:
			if err := skipJSONValue(dec); err != nil {
				return nil, xerrors.Errorf("skip transactions array: %w", err)
			}
		case blockJSONKeyRewards:
			if err := dec.Decode(&rewardsRaw); err != nil {
				return nil, xerrors.Errorf("failed to decode rewards: %w", err)
			}
		default:
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return nil, xerrors.Errorf("failed to decode header field %q: %w", key, err)
			}
			headerFields[key] = raw
		}
	}
	if err := expectDelim(dec, '}'); err != nil {
		return nil, xerrors.Errorf("block end: %w", err)
	}

	return p.buildBlockTail(slot, headerFields, rewardsRaw)
}

// buildBlockTail reassembles the buffered header fields into a
// SolanaBlockV2 and runs the same parseHeaderV2 / parseRewards that
// ParseBlock uses.
func (p *solanaNativeParserImpl) buildBlockTail(slot uint64, headerFields map[string]json.RawMessage, rewardsRaw json.RawMessage) (*blockTail, error) {
	headerJSON, err := json.Marshal(headerFields)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal header fields: %w", err)
	}
	var block SolanaBlockV2
	if err := json.Unmarshal(headerJSON, &block); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal header: %w", err)
	}
	header, err := p.parseHeaderV2(slot, &block)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse header: %w", err)
	}

	var rewards []SolanaReward
	if len(rewardsRaw) > 0 {
		if err := json.Unmarshal(rewardsRaw, &rewards); err != nil {
			return nil, xerrors.Errorf("failed to unmarshal rewards: %w", err)
		}
	}
	apiRewards, err := p.parseRewards(rewards)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse rewards: %w", err)
	}

	return &blockTail{header: header, rewards: apiRewards}, nil
}

// expectDelim consumes one token and requires it to be the given
// delimiter.
func expectDelim(dec *json.Decoder, want json.Delim) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if d, ok := tok.(json.Delim); !ok || d != want {
		return xerrors.Errorf("expected %q, got %v", want, tok)
	}
	return nil
}

// readKey consumes one token and requires it to be an object key.
func readKey(dec *json.Decoder) (string, error) {
	tok, err := dec.Token()
	if err != nil {
		return "", xerrors.Errorf("failed to read key token: %w", err)
	}
	key, ok := tok.(string)
	if !ok {
		return "", xerrors.Errorf("expected string key, got %v", tok)
	}
	return key, nil
}

// skipJSONValue consumes exactly one JSON value (scalar or container)
// from dec's current position, without allocating any sub-values.
// A local copy of the bitcoin walker's helper so the two chain walkers
// stay independent.
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
