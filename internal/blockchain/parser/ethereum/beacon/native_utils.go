package beacon

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/prysmaticlabs/prysm/v4/math"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

func (q *Quantity) UnmarshalJSON(input []byte) error {
	if len(input) == 0 {
		return xerrors.Errorf("input missing")
	}

	var str string
	if err := json.Unmarshal(input, &str); err != nil {
		return xerrors.Errorf("failed to unmarshal Quantity into string: %w", err)
	}

	if str == "" {
		return xerrors.Errorf("empty string")
	}

	val, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return xerrors.Errorf("invalid value %v: %w", str, err)
	}
	*q = Quantity(val)

	return nil
}

func (q Quantity) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%d"`, q)), nil
}

func (q Quantity) Value() uint64 {
	return uint64(q)
}

func (t *ExecutionTransaction) UnmarshalJSON(input []byte) error {
	if len(input) == 0 {
		return xerrors.Errorf("input missing")
	}

	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return xerrors.Errorf("failed to unmarshal ExecutionTransaction: %w", err)
	}

	if !internal.Has0xPrefix(s) {
		return xerrors.Errorf("missing 0x prefix")
	}

	*t = []byte(s)

	return nil
}

func (t ExecutionTransaction) MarshalJSON() ([]byte, error) {
	return t, nil
}

func (b *Blob) UnmarshalJSON(input []byte) error {
	if len(input) == 0 {
		return xerrors.Errorf("input missing")
	}

	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return xerrors.Errorf("failed to unmarshal Blob: %w", err)
	}

	if !internal.Has0xPrefix(s) {
		return xerrors.Errorf("missing 0x prefix")
	}

	*b = []byte(s)

	return nil
}

func (b Blob) MarshalJSON() ([]byte, error) {
	return b, nil
}

// calculateEpoch calculates the epoch for the given slot.
// https://github.com/prysmaticlabs/prysm/blob/2a067d5d038487bb9361ecaa6401ec4d8faae532/time/slots/slottime.go#L79
func calculateEpoch(slot uint64) (uint64, error) {
	epoch, err := math.Div64(slot, SlotsPerEpoch)
	if err != nil {
		return 0, xerrors.Errorf("failed to calculate epoch for slot=%d: %w", slot, err)
	}
	return epoch, nil
}
