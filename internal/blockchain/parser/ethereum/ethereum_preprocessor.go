package ethereum

import (
	"context"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/config"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	ethereumPreProcessor struct {
		config *config.Config
	}
	ethereumRosettaChecker struct{}
)

var (
	zeroTraceData = map[string]bool{
		"":    true,
		"0x":  true,
		"0x0": true,
	}
)

func NewEthereumChecker(params internal.ParserParams) (internal.Checker, error) {
	return internal.NewChecker(params, &ethereumPreProcessor{
		config: params.Config,
	}, &ethereumRosettaChecker{})
}

func (p *ethereumPreProcessor) PreProcessNativeBlock(expected, actual *api.NativeBlock) error {
	expectedBlock := expected.GetEthereum()
	actualBlock := actual.GetEthereum()
	if expectedBlock != nil && actualBlock != nil {
		if len(expectedBlock.Transactions) == len(actualBlock.Transactions) {
			for i := 0; i < len(expectedBlock.Transactions); i++ {
				expectedTransaction := expectedBlock.Transactions[i]
				actualTransaction := actualBlock.Transactions[i]
				if len(expectedTransaction.FlattenedTraces) == len(actualTransaction.FlattenedTraces) {
					for j := 0; j < len(expectedTransaction.FlattenedTraces); j++ {
						expectedTrace := expectedTransaction.FlattenedTraces[j]
						actualTrace := actualTransaction.FlattenedTraces[j]

						if zeroTraceData[expectedTrace.Input] && zeroTraceData[actualTrace.Input] {
							expectedTrace.Input = actualTrace.Input
						}

						if zeroTraceData[expectedTrace.Output] && zeroTraceData[actualTrace.Output] {
							expectedTrace.Output = actualTrace.Output
						}

						// Ignore differences in gas and gasUsed
						expectedTrace.Gas = actualTrace.Gas
						expectedTrace.GasUsed = actualTrace.GasUsed

						if expectedTrace.Type == "DELEGATECALL" {
							// DELEGATECALL, like CALL, CALLCODE and CREATE has a value associated with it;
							// however, that value simply reflects the parent CALL's value.
							// Unlike CALL and CREATE, there is no transfer associated with it;
							// therefore its value can be ignored.
							// Ref: https://github.com/openethereum/parity-ethereum/issues/2706
							expectedTrace.Value = actualTrace.Value
						}
					}
				}
			}
		}
	}

	return nil
}

func (c *ethereumRosettaChecker) ValidateRosettaBlock(ctx context.Context, request *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error {
	return internal.ErrNotImplemented
}
