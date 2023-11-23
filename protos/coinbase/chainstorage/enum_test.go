package chainstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSolanaProgramName(t *testing.T) {
	expectedNames := map[SolanaProgram]string{
		SolanaProgram_RAW:                          "raw",
		SolanaProgram_ADDRESS_LOOKUP_TABLE:         "address-lookup-table",
		SolanaProgram_BPF_Loader:                   "bpf-loader",
		SolanaProgram_BPF_UPGRADEABLE_Loader:       "bpf-upgradeable-loader",
		SolanaProgram_VOTE:                         "vote",
		SolanaProgram_SYSTEM:                       "system",
		SolanaProgram_STAKE:                        "stake",
		SolanaProgram_SPL_MEMO:                     "spl-memo",
		SolanaProgram_SPL_TOKEN:                    "spl-token",
		SolanaProgram_SPL_TOKEN_2022:               "spl-token-2022",
		SolanaProgram_SPL_ASSOCIATED_TOKEN_ACCOUNT: "spl-associated-token-account",
		SolanaProgram_UNPARSED:                     "unparsed",
	}

	for program, name := range expectedNames {
		assert.Equal(t, name, program.Name())
	}
}

func TestSolanaAddressLookupTableProgramInstructionName(t *testing.T) {
	expectedNames := map[SolanaAddressLookupTableProgram_InstructionType]string{
		SolanaAddressLookupTableProgram_UNKNOWN: "unknown",
	}

	for instruction, name := range expectedNames {
		assert.Equal(t, name, instruction.Name())
	}
}

func TestSolanaBpfLoaderProgramInstructionName(t *testing.T) {
	expectedNames := map[SolanaBpfLoaderProgram_InstructionType]string{
		SolanaBpfLoaderProgram_UNKNOWN: "unknown",
	}

	for instruction, name := range expectedNames {
		assert.Equal(t, name, instruction.Name())
	}
}

func TestSolanaBpfUpgradeableLoaderProgramInstructionName(t *testing.T) {
	expectedNames := map[SolanaBpfUpgradeableLoaderProgram_InstructionType]string{
		SolanaBpfUpgradeableLoaderProgram_UNKNOWN: "unknown",
	}

	for instruction, name := range expectedNames {
		assert.Equal(t, name, instruction.Name())
	}
}

func TestSolanaVoteProgramInstructionName(t *testing.T) {
	expectedNames := map[SolanaVoteProgram_InstructionType]string{
		SolanaVoteProgram_UNKNOWN:    "unknown",
		SolanaVoteProgram_INITIALIZE: "initialize",
		SolanaVoteProgram_VOTE:       "vote",
		SolanaVoteProgram_WITHDRAW:   "withdraw",
	}

	for instruction, name := range expectedNames {
		assert.Equal(t, name, instruction.Name())
	}
}

func TestSolanaSystemProgramInstructionName(t *testing.T) {
	expectedNames := map[SolanaSystemProgram_InstructionType]string{
		SolanaSystemProgram_UNKNOWN:                  "unknown",
		SolanaSystemProgram_CREATE_ACCOUNT:           "createAccount",
		SolanaSystemProgram_TRANSFER:                 "transfer",
		SolanaSystemProgram_CREATE_ACCOUNT_WITH_SEED: "createAccountWithSeed",
		SolanaSystemProgram_TRANSFER_WITH_SEED:       "transferWithSeed",
	}

	for instruction, name := range expectedNames {
		assert.Equal(t, name, instruction.Name())
	}
}

func TestSolanaStakeProgramInstructionName(t *testing.T) {
	expectedNames := map[SolanaStakeProgram_InstructionType]string{
		SolanaStakeProgram_UNKNOWN:    "unknown",
		SolanaStakeProgram_INITIALIZE: "initialize",
		SolanaStakeProgram_DELEGATE:   "delegate",
		SolanaStakeProgram_DEACTIVATE: "deactivate",
		SolanaStakeProgram_MERGE:      "merge",
		SolanaStakeProgram_SPLIT:      "split",
		SolanaStakeProgram_WITHDRAW:   "withdraw",
	}

	for instruction, name := range expectedNames {
		assert.Equal(t, name, instruction.Name())
	}
}

func TestSolanaSplMemoProgramInstructionName(t *testing.T) {
	expectedNames := map[SolanaSplMemoProgram_InstructionType]string{
		SolanaSplMemoProgram_SPL_MEMO: "splMemo",
	}

	for instruction, name := range expectedNames {
		assert.Equal(t, name, instruction.Name())
	}
}

func TestSolanaSplTokenProgramInstructionName(t *testing.T) {
	expectedNames := map[SolanaSplTokenProgram_InstructionType]string{
		SolanaSplTokenProgram_UNKNOWN:                    "unknown",
		SolanaSplTokenProgram_GET_ACCOUNT_DATA_SIZE:      "getAccountDataSize",
		SolanaSplTokenProgram_INITIALIZE_IMMUTABLE_OWNER: "initializeImmutableOwner",
		SolanaSplTokenProgram_TRANSFER:                   "transfer",
	}

	for instruction, name := range expectedNames {
		assert.Equal(t, name, instruction.Name())
	}
}

func TestSolanaSplToken2022ProgramInstructionName(t *testing.T) {
	expectedNames := map[SolanaSplToken2022Program_InstructionType]string{
		SolanaSplToken2022Program_UNKNOWN: "unknown",
	}

	for instruction, name := range expectedNames {
		assert.Equal(t, name, instruction.Name())
	}
}

func TestSolanaSplAssociatedTokenAccountProgramInstructionName(t *testing.T) {
	expectedNames := map[SolanaSplAssociatedTokenAccountProgram_InstructionType]string{
		SolanaSplAssociatedTokenAccountProgram_UNKNOWN: "unknown",
	}

	for instruction, name := range expectedNames {
		assert.Equal(t, name, instruction.Name())
	}
}
