package blockchain

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
)

var Module = fx.Options(
	client.Module,
	endpoints.Module,
	jsonrpc.Module,
	parser.Module,
)
