package xrp

import (
	"context"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
	"time"
)

type XRPBlock struct {
	LedgerHeader LedgerHeader         `json:"ledger"`
	Transactions []*LedgerTransaction `json:"transactions,omitempty"`
	LedgerIndex  uint32               `json:"ledger_index"`
	Validated    bool                 `json:"validated"`
}

type LedgerHeader struct {
	Closed              bool      `json:"closed"`
	LedgerHash          string    `json:"ledger_hash"`
	CloseTime           time.Time `json:"close_time"`
	CloseTimeResolution uint32    `json:"close_time_resolution"`
	PreviousLedgerHash  string    `json:"previous_ledger_hash"`
	LedgerIndex         uint32    `json:"ledger_index"`
	TotalCoins          uint64    `json:"total_coins"`
}

type LedgerTransaction struct {
	Account            string    `json:"Account"`            // The unique address of the account that initiated the transaction.
	Fee                string    `json:"Fee"`                // The amount of XRP, in drops, to be destroyed as a cost for distributing this transaction to the network.
	Flags              uint32    `json:"Flags"`              // The set of bit-flags for this transaction.
	LastLedgerSequence uint32    `json:"LastLedgerSequence"` // The highest ledger index this transaction can appear in.
	OfferSequence      uint32    `json:"OfferSequence"`      // The offer sequence number.
	Sequence           uint32    `json:"Sequence"`           // The sequence number of the account sending the transaction.
	SigningPubKey      string    `json:"SigningPubKey"`      // The hex representation of the public key that corresponds to the private key used to sign this transaction.
	TakerGets          TakerGets `json:"TakerGets"`          // The amount that the taker gets, specified in the offer.
	TakerPays          string    `json:"TakerPays"`          // The amount that the taker pays, specified in the offer.
	TransactionType    string    `json:"TransactionType"`    // The type of transaction, e.g., "OfferCreate".
	TxnSignature       string    `json:"TxnSignature"`       // The signature that verifies this transaction as originating from the account it says it is from.
	Hash               string    `json:"hash"`               // The hash of the transaction.
	Ctid               string    `json:"ctid"`               // The currency transaction ID.
	Meta               Meta      `json:"meta"`               // The metadata associated with the transaction.
	Validated          bool      `json:"validated"`          // Indicates whether the transaction is validated.
	Date               int64     `json:"date"`               // The timestamp of the transaction, represented as the number of seconds since the Unix epoch.
	LedgerIndex        uint32    `json:"ledger_index"`       // The index of the ledger that contains this transaction.
	InLedger           uint32    `json:"inLedger"`           // The ledger index where this transaction was included.
	Status             string    `json:"status"`             // The status of the transaction.
}

// TakerGets defines the structure for the amount that the taker gets in an offer.
type TakerGets struct {
	Currency string `json:"currency"` // The currency value in hexadecimal format.
	Issuer   string `json:"issuer"`   // The issuer of the currency, if applicable.
	Value    string `json:"value"`    // The actual value of the currency.
}

// Meta contains the metadata information related to a transaction.
type Meta struct {
	AffectedNodes     []AffectedNode `json:"AffectedNodes"`     // The list of nodes affected by the transaction.
	TransactionIndex  uint32         `json:"TransactionIndex"`  // The index of the transaction within the ledger.
	TransactionResult string         `json:"TransactionResult"` // The result of the transaction.
}

// AffectedNode represents a node that was affected by the transaction.
type AffectedNode struct {
	DeletedNode  *DeletedNode  `json:"DeletedNode"`  // Information about a node that was deleted.
	ModifiedNode *ModifiedNode `json:"ModifiedNode"` // Information about a node that was modified.
	CreatedNode  *CreatedNode  `json:"CreatedNode"`  // Information about a node that was created.
}

// DeletedNode contains information about a deleted node.
type DeletedNode struct {
	FinalFields struct {
		Account           string    `json:"Account"`           // The account related to the node.
		BookDirectory     string    `json:"BookDirectory"`     // The book directory of the node.
		BookNode          string    `json:"BookNode"`          // The book node of the node.
		Flags             uint32    `json:"Flags"`             // The flags associated with the node.
		OwnerNode         string    `json:"OwnerNode"`         // The owner node of the node.
		PreviousTxnID     string    `json:"PreviousTxnID"`     // The transaction ID of the previous transaction.
		PreviousTxnLgrSeq uint32    `json:"PreviousTxnLgrSeq"` // The ledger sequence of the previous transaction.
		Sequence          uint32    `json:"Sequence"`          // The sequence number of the node.
		TakerGets         TakerGets `json:"TakerGets"`         // The taker gets value of the node.
		TakerPays         string    `json:"TakerPays"`         // The taker pays value of the node.
	} `json:"FinalFields"`
	LedgerEntryType string `json:"LedgerEntryType"` // The type of ledger entry.
	LedgerIndex     string `json:"LedgerIndex"`     // The ledger index of the node.
}

// ModifiedNode contains information about a modified node.
type ModifiedNode struct {
	FinalFields struct {
		Flags         uint32 `json:"Flags"`         // The flags associated with the node.
		IndexNext     string `json:"IndexNext"`     // The next index of the node.
		IndexPrevious string `json:"IndexPrevious"` // The previous index of the node.
		Owner         string `json:"Owner"`         // The owner of the node.
		RootIndex     string `json:"RootIndex"`     // The root index of the node.
	} `json:"FinalFields"`
	LedgerEntryType string `json:"LedgerEntryType"` // The type of ledger entry.
	LedgerIndex     string `json:"LedgerIndex"`     // The ledger index of the node.
	PreviousFields  struct {
		Balance  string `json:"Balance"`  // The previous balance of the account.
		Sequence uint32 `json:"Sequence"` // The previous sequence number of the account.
	} `json:"PreviousFields"`
	PreviousTxnID     string `json:"PreviousTxnID"`     // The transaction ID of the previous transaction.
	PreviousTxnLgrSeq uint32 `json:"PreviousTxnLgrSeq"` // The ledger sequence of the previous transaction.
}

// CreatedNode contains information about a created node.
type CreatedNode struct {
	LedgerEntryType string `json:"LedgerEntryType"` // The type of ledger entry.
	LedgerIndex     string `json:"LedgerIndex"`     // The ledger index of the node.
	NewFields       struct {
		ExchangeRate      string `json:"ExchangeRate"`      // The exchange rate of the node.
		RootIndex         string `json:"RootIndex"`         // The root index of the node.
		TakerGetsCurrency string `json:"TakerGetsCurrency"` // The currency that the taker gets.
		TakerGetsIssuer   string `json:"TakerGetsIssuer"`   // The issuer of the currency that the taker gets.
		TakerPaysCurrency string `json:"TakerPaysCurrency"` // The currency that the taker pays.
		TakerPaysIssuer   string `json:"TakerPaysIssuer"`   // The issuer of the currency that the taker pays.
	} `json:"NewFields"`
}

// Warning represents a warning message associated with the transaction result.
type Warning struct {
	ID      string `json:"id"`      // The ID of the warning.
	Message string `json:"message"` // The warning message.
}

type (
	xrpNativeParserImpl struct {
		logger   *zap.Logger
		validate *validator.Validate
	}
)

var _ internal.NativeParser = (*xrpNativeParserImpl)(nil)

func NewXrpNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	v := validator.New()
	return &xrpNativeParserImpl{
		logger:   log.WithPackage(params.Logger),
		validate: v,
	}, nil
}

func (b *xrpNativeParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error) {
	return nil, internal.ErrNotImplemented
}

func (b *xrpNativeParserImpl) GetTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error) {
	return nil, internal.ErrNotImplemented
}
