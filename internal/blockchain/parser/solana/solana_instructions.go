package solana

// #nosec G101 These are not credentials
const (
	AddressLookupTableProgram        = "address-lookup-table"
	BpfLoaderProgram                 = "bpf-loader"
	BpfUpgradeableLoaderProgram      = "bpf-upgradeable-loader"
	VoteProgram                      = "vote"
	SystemProgram                    = "system"
	StakeProgram                     = "stake"
	SplMemoProgram                   = "spl-memo"
	SplTokenProgram                  = "spl-token"
	SplToken2022Program              = "spl-token-2022"
	SplAssociatedTokenAccountProgram = "spl-associated-token-account"
	UnparsedProgram                  = ""

	// Program: vote
	VoteInitializeInstruction  = "initialize"
	VoteVoteInstruction        = "vote"
	VoteWithdrawInstruction    = "withdraw"
	VoteCompactUpdateVoteState = "compactupdatevotestate"

	// Program: system
	SystemCreateAccountInstruction         = "createAccount"
	SystemTransferInstruction              = "transfer"
	SystemCreateAccountWithSeedInstruction = "createAccountWithSeed"
	SystemTransferWithSeedInstruction      = "transferWithSeed"

	// Program: stake
	StakeInitializeInstruction = "initialize"
	StakeDelegateInstruction   = "delegate"
	StakeDeactivateInstruction = "deactivate"
	StakeMergeInstruction      = "merge"
	StakeSplitInstruction      = "split"
	StakeWithdrawInstruction   = "withdraw"

	// Program: spl-token
	SplTokenGetAccountDataSizeInstruction       = "getAccountDataSize"
	SplTokenInitializeImmutableOwnerInstruction = "initializeImmutableOwner"
	SplTokenTransferInstruction                 = "transfer"
)

// Instruction reference: https://github.com/solana-labs/solana/blob/master/transaction-status/src/
type (
	// Generic
	UnknownInstructionInfo struct {
		Info []byte `json:"info"`
	}

	// Program: vote
	VoteInitializeInstructionInfo struct {
		VoteAccount          string `json:"voteAccount" validate:"required"`
		RentSysvar           string `json:"rentSysvar" validate:"required"`
		ClockSysvar          string `json:"clockSysvar" validate:"required"`
		Node                 string `json:"node" validate:"required"`
		AuthorizedVoter      string `json:"authorizedVoter" validate:"required"`
		AuthorizedWithdrawer string `json:"authorizedWithdrawer" validate:"required"`
		Commission           uint8  `json:"commission"`
	}

	VoteVoteInstructionInfo struct {
		VoteAccount      string `json:"voteAccount" validate:"required"`
		SlotHashesSysvar string `json:"slotHashesSysvar" validate:"required"`
		ClockSysvar      string `json:"clockSysvar" validate:"required"`
		VoteAuthority    string `json:"voteAuthority" validate:"required"`
		Vote             struct {
			Hash      string   `json:"hash" validate:"required"`
			Slots     []uint64 `json:"slots" validate:"required"`
			Timestamp int64    `json:"timestamp"`
		} `json:"vote" validate:"required"`
	}

	VoteWithdrawInstructionInfo struct {
		VoteAccount       string `json:"voteAccount" validate:"required"`
		Destination       string `json:"destination" validate:"required"`
		WithdrawAuthority string `json:"withdrawAuthority" validate:"required"`
		Lamports          uint64 `json:"lamports"`
	}

	VoteCompactUpdateVoteStateInstructionInfo struct {
		VoteAccount     string `json:"voteAccount" validate:"required"`
		VoteAuthority   string `json:"voteAuthority" validate:"required"`
		VoteStateUpdate struct {
			Lockouts []struct {
				ConfirmationCount uint64 `json:"confirmation_count"`
				Slot              uint64 `json:"slot"`
			} `json:"lockouts"`
			Hash      string `json:"hash"`
			Root      uint64 `json:"root"`
			Timestamp int64  `json:"timestamp"`
		} `json:"voteStateUpdate" validate:"required"`
	}

	// Program: system
	SystemCreateAccountInstructionInfo struct {
		Source     string `json:"source" validate:"required"`
		NewAccount string `json:"newAccount" validate:"required"`
		Lamports   uint64 `json:"lamports"`
		Space      uint64 `json:"space"`
		Owner      string `json:"owner" validate:"required"`
	}

	SystemTransferInstructionInfo struct {
		Source      string `json:"source" validate:"required"`
		Destination string `json:"destination" validate:"required"`
		Lamports    uint64 `json:"lamports"`
	}

	SystemCreateAccountWithSeedInstructionInfo struct {
		Source     string `json:"source" validate:"required"`
		NewAccount string `json:"newAccount" validate:"required"`
		Base       string `json:"base" validate:"required"`
		Seed       string `json:"seed"`
		Lamports   uint64 `json:"lamports"`
		Space      uint64 `json:"space"`
		Owner      string `json:"owner" validate:"required"`
	}

	SystemTransferWithSeedInstructionInfo struct {
		Source      string `json:"source" validate:"required"`
		SourceBase  string `json:"sourceBase" validate:"required"`
		Destination string `json:"destination" validate:"required"`
		Lamports    uint64 `json:"lamports"`
		SourceSeed  string `json:"sourceSeed"`
		SourceOwner string `json:"sourceOwner" validate:"required"`
	}

	// Program: stake
	StakeInitializeInstructionInfo struct {
		StakeAccount string `json:"stakeAcount"`
		RentSysvar   string `json:"rentSysvar" validate:"required"`

		Authorized struct {
			Staker     string `json:"staker" validate:"required"`
			Withdrawer string `json:"withdrawer" validate:"required"`
		} `json:"authorized"`

		Lockup struct {
			UnixTimestamp int64  `json:"unixTimestamp"`
			Epoch         uint64 `json:"epoch"`
			Custodian     string `json:"custodian" validate:"required"`
		} `json:"lockup" validate:"required"`
	}

	StakeDelegateInstructionInfo struct {
		StakeAccount       string `json:"stakeAcount"`
		VoteAccount        string `json:"voteAccount" validate:"required"`
		ClockSysvar        string `json:"clockSysvar" validate:"required"`
		StakeHistorySysvar string `json:"stakeHistorySysvar" validate:"required"`
		StakeConfigAccount string `json:"stakeConfigAccount" validate:"required"`
		StakeAuthority     string `json:"stakeAuthority" validate:"required"`
	}

	StakeDeactivateInstructionInfo struct {
		StakeAccount   string `json:"stakeAcount"`
		ClockSysvar    string `json:"clockSysvar" validate:"required"`
		StakeAuthority string `json:"stakeAuthority" validate:"required"`
	}

	StakeMergeInstructionInfo struct {
		Destination        string `json:"destination" validate:"required"`
		Source             string `json:"source" validate:"required"`
		ClockSysvar        string `json:"clockSysvar" validate:"required"`
		StakeHistorySysvar string `json:"stakeHistorySysvar" validate:"required"`
		StakeAuthority     string `json:"stakeAuthority" validate:"required"`
	}

	StakeSplitInstructionInfo struct {
		StakeAccount    string `json:"stakeAcount"`
		NewSplitAccount string `json:"newSplitAccount" validate:"required"`
		StakeAuthority  string `json:"stakeAuthority" validate:"required"`
		Lamports        uint64 `json:"lamports"`
	}

	StakeWithdrawInstructionInfo struct {
		StakeAccount       string `json:"stakeAcount"`
		Destination        string `json:"destination" validate:"required"`
		ClockSysvar        string `json:"clockSysvar" validate:"required"`
		StakeHistorySysvar string `json:"stakeHistorySysvar" validate:"required"`
		WithdrawAuthority  string `json:"withdrawAuthority" validate:"required"`
		Lamports           uint64 `json:"lamports"`
	}

	// Program: spl-token
	SplTokenGetAccountDataSizeInstructionInfo struct {
		Mint           string   `json:"mint" validate:"required"`
		ExtensionTypes []string `json:"extensionTypes"`
	}

	SplTokenInitializeImmutableOwnerInstructionInfo struct {
		Account string `json:"account" validate:"required"`
	}

	SplTokenTransferInstructionInfo struct {
		Source      string `json:"source" validate:"required"`
		Destination string `json:"destination" validate:"required"`
		Authority   string `json:"authority"`
		Amount      string `json:"amount" validate:"required"`
	}
)
