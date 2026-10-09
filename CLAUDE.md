# ChainStorage Development Guide

This guide helps Claude understand the ChainStorage project structure and common development tasks.

## Project Overview

ChainStorage is a blockchain data storage and processing system that:
- Continuously replicates blockchain changes (new blocks)
- Acts as a distributed file system for blockchain data
- Stores raw data in horizontally-scalable storage (S3 + DynamoDB)
- Supports multiple blockchains: Ethereum, Bitcoin, Solana, Polygon, etc.
- Can serve up to 1,500 blocks per second in production

## Key Commands

### Testing
```bash
# Run all unit tests
make test

# Run specific package tests
make test TARGET=internal/blockchain/...

# Run integration tests
make integration TARGET=internal/storage/...

# Run functional tests (requires secrets.yml)
make functional TARGET=internal/workflow/...
```

### Linting and Type Checking
```bash
# Run linter (includes go vet, errcheck, ineffassign)
make lint
# Note: May encounter errors with Go versions > 1.22

# No separate typecheck command - type checking happens during build
```

### Building
```bash
# Initial setup (once)
make bootstrap

# Build everything
make build

# Generate protobuf files
make proto

# Generate configs from templates
make config
```

### Local Development
```bash
# Start local infrastructure (LocalStack)
make localstack

# Start server (Ethereum mainnet)
make server

# Start server (other networks)
make server CHAINSTORAGE_CONFIG=ethereum_goerli
```

## Project Structure

```
/cmd/              - Command line tools
  /admin/          - Admin CLI tool
  /api/            - API server
  /server/         - Main server
  /worker/         - Worker processes

/internal/         - Core implementation
  /blockchain/     - Blockchain clients and parsers
    /client/       - Chain-specific clients
    /parser/       - Chain-specific parsers
  /storage/        - Storage implementations
    /blobstorage/  - S3/GCS storage
    /metastorage/  - DynamoDB/Firestore storage
  /workflow/       - Temporal workflows
    /activity/     - Workflow activities

/config/           - Generated configurations
/config_templates/ - Configuration templates
/protos/           - Protocol buffer definitions
/sdk/              - Go SDK for consumers
```

## Key Workflows

1. **Backfiller**: Backfills historical blocks
2. **Poller**: Polls for new blocks continuously
3. **Streamer**: Streams blocks in real-time
4. **Monitor**: Monitors system health
5. **Benchmarker**: Benchmarks performance

## Environment Variables

- `CHAINSTORAGE_NAMESPACE`: Service namespace (default: chainstorage)
- `CHAINSTORAGE_CONFIG`: Format: `{blockchain}-{network}` (e.g., ethereum-mainnet)
- `CHAINSTORAGE_ENVIRONMENT`: Environment (local/development/production)

## Common Tasks

### Adding Support for New Blockchain
1. Create config templates in `/config_templates/chainstorage/{blockchain}/{network}/`
2. Implement client in `/internal/blockchain/client/{blockchain}/`
3. Implement parser in `/internal/blockchain/parser/{blockchain}/`
4. Run `make config` to generate configs
5. Add tests

### Debugging LocalStack Services
```bash
# Check S3 files
aws s3 --no-sign-request --region local --endpoint-url http://localhost:4566 ls --recursive example-chainstorage-ethereum-mainnet-dev/

# Check DynamoDB
aws dynamodb --no-sign-request --region local --endpoint-url http://localhost:4566 scan --table-name example_chainstorage_blocks_ethereum_mainnet

# Check SQS DLQ
aws sqs --no-sign-request --region local --endpoint-url http://localhost:4566 receive-message --queue-url "http://localhost:4566/000000000000/example_chainstorage_blocks_ethereum_mainnet_dlq"
```

### Working with Temporal Workflows
```bash
# Start backfiller
go run ./cmd/admin workflow start --workflow backfiller --input '{"StartHeight": 11000000, "EndHeight": 11000100}' --blockchain ethereum --network mainnet --env local

# Start poller
go run ./cmd/admin workflow start --workflow poller --input '{"Tag": 0, "MaxBlocksToSync": 100}' --blockchain ethereum --network mainnet --env local

# Check workflow status
tctl --address localhost:7233 --namespace chainstorage-ethereum-mainnet workflow show --workflow_id workflow.backfiller
```

## Important Notes

1. **Always run tests before committing**: Use `make test` and `make lint`
2. **Config generation**: After modifying templates, run `make config`
3. **Secrets**: Never commit `secrets.yml` files (used for endpoint configurations)
4. **Endpoint groups**: Master (sticky) for canonical chain, Slave (round-robin) for data ingestion
5. **Parser types**: Native (default), Mesh, or Raw

## Dependencies

- Go 1.22 (required - newer versions may cause lint errors)
- Protobuf 25.2
- Temporal (workflow engine)
- LocalStack (local AWS services)
- Docker & Docker Compose

## Architecture Insights

### Client Architecture
- **Multi-endpoint system**: Master (primary), Slave (load distribution), Validator, Consensus
- **Protocol support**: JSON-RPC (most chains) and REST API (Rosetta)
- **Shared implementations**: EVM chains (Polygon, BSC, Arbitrum) share Ethereum client code
- **Factory pattern**: Each blockchain has a client factory registered with dependency injection

### Key Design Patterns
1. **Interceptor Pattern**: Wraps clients for instrumentation and parsing
2. **Option Pattern**: Modifies client behavior (e.g., WithBestEffort())
3. **Batch Processing**: Configurable batch sizes for performance
4. **Error Handling**: Standardized errors with network-specific handling

### Supported Blockchains
- **EVM-based**: Ethereum, Polygon, BSC, Arbitrum, Optimism, Base, Fantom, Avalanche
- **Bitcoin-based**: Bitcoin, Bitcoin Cash, Dogecoin, Litecoin
- **Other**: Solana, Aptos, Tron
- **Special**: Ethereum Beacon Chain support