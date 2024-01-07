package sdk

import (
	"time"

	"github.com/go-playground/validator/v10"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Config struct {
		Blockchain      common.Blockchain `mapstructure:"blockchain" validate:"required"`
		Network         common.Network    `mapstructure:"network" validate:"required"`
		Env             Env               `mapstructure:"env" validate:"required,oneof=production development local"`
		Sidechain       api.SideChain     `mapstructure:"side_chain"`
		Tag             uint32            `mapstructure:"tag"`
		ClientID        string            `mapstructure:"client_id"`
		ServerAddress   string            `mapstructure:"server_address"`
		ClientTimeout   time.Duration     `mapstructure:"client_timeout"`
		BlockValidation *bool             `mapstructure:"block_validation"`
	}

	Env = config.Env

	StreamingConfiguration struct {
		// See the proto for details.
		ChainEventsRequest *api.ChainEventsRequest `validate:"required"`

		// How many blocks to prefetch. If not specified. it defaults to 1.
		ChannelBufferCapacity uint64

		// Number of events to return from the stream. If not specified, streaming never ends.
		NumberOfEvents uint64

		// Not implemented.
		BlockConfirmationGap uint64

		// If specified, the Block field is omitted from ChainEventResult.
		EventOnly bool
	}
)

const (
	EnvProduction  = config.EnvProduction
	EnvDevelopment = config.EnvDevelopment
	EnvLocal       = config.EnvLocal
)

func (c *Config) validate() error {
	v := validator.New()
	return v.Struct(c)
}
