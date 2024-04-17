<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Command Line](#command-line)
  - [Block Command](#block-command)
  - [Backfill Command (development)](#backfill-command-development)
  - [Stream Command](#stream-command)
- [Testing](#testing)
  - [Unit Test](#unit-test)
  - [Integration Test](#integration-test)
  - [Functional Test](#functional-test)
- [Configuration](#configuration)
  - [Dependency overview](#dependency-overview)
  - [Template location and generated config](#template-location-and-generated-config)
  - [Environment Variables](#environment-variables)
  - [Creating New Configurations](#creating-new-configurations)
  - [Template Format and Inheritance](#template-format-and-inheritance)
  - [Endpoint Group](#endpoint-group)
  - [Overriding the Configuration](#overriding-the-configuration)
- [Development](#development)
  - [Running Server](#running-server)
  - [AWS localstack](#aws-localstack)
  - [Temporal Workflow](#temporal-workflow)
- [Failover](#failover)
  - [Nodes Failover](#nodes-failover)
  - [Failover in Workflows](#failover-in-workflows)
  - [Automatic Failover in Poller Workflow](#automatic-failover-in-poller-workflow)
  - [Checking Workflow Statuses](#checking-workflow-statuses)
  - [APIs](#apis)
  - [APIs List](#apis-list)
- [SDK](#sdk)
  - [Data Processing Pattern](#data-processing-pattern)
- [Examples](#examples)
  - [Batch](#batch)
  - [Stream](#stream)
  - [Unified](#unified)
- [Public APIs](#public-apis)
- [Contact Us](#contact-us)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Overview

ChainStorage is the foundational component of [ChainNode](https://github.com/coinbase/chainnode) and [Chainsformer](https://github.com/coinbase/chainsformer).
These projects represent the crypto data processing suites (code name ChainStack) widely adopted within Coinbase.

ChainStorage is inspired by the Change Data Capture paradigm, commonly used in the big data world. It continuously
replicates the changes (i.e. new blocks) on the blockchain, and acts like a distributed file system for the blockchain.

It aims to provide an efficient and flexible way to access the on-chain data:

- Efficiency is optimized by storing data in horizontally-scalable storage with a key-value schema. At Coinbase's production environment, ChainStorage can serve up to 1,500 blocks per second, enabling teams to build various indexers cost-effectively.
- Flexibility is improved by decoupling data interpretation from data ingestion. ChainStorage stores the raw data, and the parsing is deferred until the data is consumed. The parsers are shipped as part of the SDK and run on the consumer side. Thanks to the ELT (Extract, Load, Transform) architecture, we can quickly iterate on the parser without ingesting the data from the blockchain again.

## Quick Start

Make sure your local go version is 1.20 by running the following commands:
```shell
brew install go@1.20
brew unlink go
brew link go@1.20

brew install protobuf@25.2
brew unlink protobuf
brew link protobuf

```

To set up for the first time (only done once):
```shell
make bootstrap
```

Rebuild everything:
```shell
make build
```

Generate Protos:
```shell
make proto
```

## Command Line

the `cmd/admin` tool consists of multiple sub command.

```
admin is a utility for managing chainstorage

Usage:
  admin [command]

Available Commands:
  backfill    Backfill a block
  block       Fetch a block
  completion  Generate the autocompletion script for the specified shell
  event       tool for managing events storage
  help        Help about any command
  sdk
  validator
  workflow    tool for managing chainstorage workflows

Flags:
      --blockchain string   blockchain full name (e.g. ethereum)
      --env string          one of [local, development, production]
  -h, --help                help for admin
      --meta                output metadata only
      --network string      network name (e.g. mainnet)
      --out string          output filepath: default format is json; use a .pb extension for protobuf format
      --parser string       parser type: one of native, rosetta, or raw (default "native")

Use "admin [command] --help" for more information about a command.
```

All sub-commands require the `blockchain`, `env`, `network` flags.

### Block Command

Fetch a block from ethereum mainnet:
```shell
go run ./cmd/admin block --blockchain ethereum --network mainnet --env local --height 46147
```

Fetch a block from ethereum goerli:
```shell
go run ./cmd/admin block --blockchain ethereum --network goerli --env local --height 46147
```

### Backfill Command (development)

Backfill a block from BSC mainnet:
```
go run ./cmd/admin backfill --blockchain bsc --network mainnet --env development --start-height 10408613 --end-height 10408614
```

### Stream Command

Stream block events from a specific event sequence id:
```shell
go run ./cmd/admin sdk stream --blockchain ethereum --network mainnet --env development --sequence 2228575 --event-tag 1
```


## Testing
### Unit Test

```shell
# Run everything
make test

# Run the blockchain package only
make test TARGET=internal/blockchain/...
```

### Integration Test
```shell
# Run everything
make integration

# Run the storage package only
make integration TARGET=internal/storage/...

# If test class implemented with test suite, add suite name before the test name
make integration TARGET=internal/blockchain/... TEST_FILTER=TestIntegrationPolygonTestSuite/TestPolygonGetBlock
```

### Functional Test

Before running the functional test, you need to provide the endpoint group config by creating `secrets.yml`.
See [here](#endpoint-group) for more details.

```shell
# Run everything
make functional

# Run the workflow package only
make functional TARGET=internal/workflow/...

# Run TestIntegrationEthereumGetBlock only
make functional TARGET=internal/blockchain/... TEST_FILTER='TestIntegrationEthereumGetBlock$$'

# If test class implemented with test suite, add suite name before the test name
make functional TARGET=internal/blockchain/... TEST_FILTER=TestIntegrationPolygonTestSuite/TestPolygonGetBlock
```

## Configuration

### Dependency overview

To understand the structure and elements of ChainStorage's config, it's important to comprehend its dependencies.

- **Temporal**: Temporal is a workflow engine that orchestrates the data ingestion workflow. It calls ChainStorage service endpoint to complete various of tasks.
- **Blob storage** - current implementation is on AWS S3, and the local service is provied by localstack
- **Key value storage** - current implemnentation is based on dynamodb and the local service is provied by localstack
- **Dead Letter queue** - current implementation is on SQS and the local service is provied by localstack

### Template location and generated config

The config template directory is in `config_templates/config`, which `make config` reads this directory and generates the config into the `config/chainstorage` directory.

### Environment Variables

ChainStorage depends on the following environment variables to resolve the path of the configuration. 
The directory structure is as follows: `config/{namespace}/{blockchain}/{network}/{environment}.yml`.

- `CHAINSTORAGE_NAMESPACE`:
  A `{namespace}` is logical grouping of several services, each of which manages its own blockchain and network. The
  default namespace is [chainstorage](/config/chainstorage).
  To deploy a different namespace, set the env var to the name of a subdirectory of [./config](/config).
- `CHAINSTORAGE_CONFIG`:
  This env var, in the format of `{blockchain}-{network}`, determines the blockchain and network managed by the service.
  The naming is defined
  in [c3/common](/protos/coinbase/c3/common/common.proto).
- `CHAINSTORAGE_ENVIRONMENT`:
  This env var controls the `{environment}` in which the service is deployed. Possible values include `production`
  , `development`, and `local` (which is also the default value).

### Creating New Configurations

Every new asset in ChainStorage consists of ChainStorage configuration files.
These configuration files are generated from `.template.yml` template files using:

```shell
make config
```

these templates will be under a directory dedicated to storing the config templates
in a structure that mirrors the final config structure of the `config`
directories. All configurations from this directory will be generated within the final
respective config directories

### Template Format and Inheritance

Configuration templates are composable and inherit configuration properties from
"parent templates", which can be defined in `base.template.yml`, `local.template.yml`, `development.template.yml`,
and `production.template.yml`.
These parent templates are merged into the final blockchain and network specific `base.template.yml`,
`local.template.yml`, `development.template.yml`, `production.template.yml` configurations respectively.

In the following example, `config/chainstorage/ethereum/mainnet/base.yml`
inherits from `config_templates/base.template.yml` and `config_templates/chainstorage/ethereum/mainnet/base.template.yml`,
with the latter taking precedence over the former.

```
config
  chainstorage
    ethereum
      mainnet
        base.yml
        development.yml
        local.yml
        production.yml
config_templates
  chainstorage
    ethereum
      mainnet
        base.template.yml
        development.template.yml
        local.template.yml
        production.template.yml
    base.template.yml
    development.template.yml
    local.template.yml
    production.template.yml

```

The template language supports string substitution for the Config-Name and Environment using the `{{`, `}}` tags.

Example:

```yaml
foo: {{blockchain}}-{{network}}-{{environment}}
```

The blockchain, `{{blockchain}}`, network, `{{network}}`, and environment, `{{environment}}` template variables
are derived from the directory and file naming schemes associated with cloud and ChainStorage configurations.

### Endpoint Group

Endpoint group is an abstraction for one or more JSON-RPC endpoints.
[EndpointProvider](./internal/blockchain/endpoints/endpoint_provider.go) uses the `endpoint_group` config to implement
client-side routing to the node provider.

ChainStorage utilizes two endpoint groups to speed up data ingestion:
* master: This endpoint group is used to resolve the canonical chain and determine what blocks to ingest next.
  Typically, sticky session is turned on for this group to ensure stronger data consistency between the requests.
* slave: This endpoint group is used to ingest the data from the blockchain. During data ingestion, the new blocks are
  ingested in parallel and out of order. Typically, the endpoints are selected in a round-robin fashion, but you may
  increase the weights to send more traffic to certain endpoints.

If your node provider, e.g. QuickNode, already has built-in load balancing, your endpoint group may contain only one 
endpoint, as illustrated by the following configuration:
```yaml
chain:
  client:
    master:
      endpoint_group: |
        {
          "endpoints": [
            {
              "name": "quicknode-foo-bar-sticky",
              "url": "https://foo-bar.matic.quiknode.pro/****",
              "weight": 1
            }
          ],
          "sticky_session": {
            "header_hash": "x-session-hash"
          }
        }
    slave:
      endpoint_group: |
        {
          "endpoints": [
            {
              "name": "quicknode-foo-bar-round-robin",
              "url": "https://foo-bar.matic.quiknode.pro/****",
              "weight": 1
            }
          ]
        }
```

### Overriding the Configuration

You may override any configuration using an environment variable. The environment variable should be prefixed with
"CHAINSTORAGE_". For nested dictionary, use underscore to separate the keys.

For example, you may override the endpoint group config at runtime by injecting the following environment variables:
* master: CHAINSTORAGE_CHAIN_CLIENT_MASTER_ENDPOINT_GROUP
* slave: CHAINSTORAGE_CHAIN_CLIENT_SLAVE_ENDPOINT_GROUP

Alternatively, you may override the configuration by creating `secrets.yml` within the same directory. Its attributes
will be merged into the runtime configuration and take the highest precedence. Note that this file may contain
credentials and is excluded from check-in by `.gitignore`.

## Development

### Running Server
Start the dockers by the docker-compose file from project root folder:
```shell
make localstack
```
> If you have developed ChainStorage before locally with previous docker compose file and got below error message
> ```shell
> nc: bad address 'postgresql'
> ```
> Please remove existing ChainStorage container and reran `make localstack`

The next step is to start the server locally:

```shell
# Ethereum Mainnet
# Use aws local stack
make server

# If want to start testnet (goerli) server
# Use aws local stack
make server CHAINSTORAGE_CONFIG=ethereum_goerli
```

### AWS localstack

Check S3 files:
You can checkout the config from `config/chainstorage/{{Blockchain}}/{{network}}/{{evironment}}`  for the value of S3 bucket name, dynamoDB tables, and SQS name.
```shell
aws s3 --no-sign-request --region local --endpoint-url http://localhost:4566 ls --recursive example-chainstorage-ethereum-mainnet-dev/
```

Check DynamoDB rows:
```shell
aws dynamodb --no-sign-request --region local --endpoint-url http://localhost:4566 scan --table-name example_chainstorage_blocks_ethereum_mainnet
```

Check DLQ:
```shell
aws sqs --no-sign-request --region local --endpoint-url http://localhost:4566/000000000000/example_chainstorage_blocks_ethereum_mainnet_dlq receive-message --queue-url "http://localhost:4566/000000000000/example_chainstorage_blocks_ethereum_mainnet_dlq" --max-number-of-messages 10 --visibility-timeout 2
```

### Temporal Workflow

Open Temporal UI in a browser by entering the
URL: http://localhost:8088/namespaces/chainstorage-ethereum-mainnet/workflows

Start the backfill workflow:
```shell
go run ./cmd/admin workflow start --workflow backfiller --input '{"StartHeight": 11000000, "EndHeight": 11000100, "NumConcurrentExtractors": 1}' --blockchain ethereum --network mainnet --env local
```

Start the benchmarker workflow:
```shell
go run ./cmd/admin workflow start --workflow benchmarker --input '{"StartHeight": 1, "EndHeight": 12000000, "NumConcurrentExtractors": 24, "StepSize":1000000, "SamplesToTest":500}' --blockchain ethereum --network mainnet --env local
```

Start the monitor workflow:
```shell
go run ./cmd/admin workflow start --workflow monitor --blockchain ethereum --network mainnet --env local --input '{"StartHeight": 11000000}'
```

Start the poller workflow:
```shell
go run ./cmd/admin workflow start --workflow poller --input '{"Tag": 0, "MaxBlocksToSync": 100, "Parallelism":4}' --blockchain ethereum --network mainnet --env local
```
NOTE: the recommended value for "parallelism" depend on the capacity of your node provider. If you are not sure what
value should be used, just drop it from the command.

Start the streamer workflow:
```shell
go run ./cmd/admin workflow start --workflow streamer --input '{}' --blockchain ethereum --network goerli --env local
```

Stop the monitor workflow:
```shell
go run ./cmd/admin workflow stop --workflow monitor --blockchain ethereum --network mainnet --env local
```

Stop a versioned streamer workflow:
```shell
go run ./cmd/admin workflow stop --workflow streamer --blockchain ethereum --network mainnet --env local --workflowID {workflowID}
```

## Failover

### Nodes Failover
ChainStorage supports nodes failover feature to mitigate the issue from nodes which may impact our SLA.
When primary clusters are down, we can choose to switch over to failover clusters to mitigate incidents, instead of waiting for nodes get fully recovered.

Check [this comment](https://github.com/coinbase/chainstorage/blob/master/internal/blockchain/endpoints/endpoint_provider.go#L21-26) to get more details of the primary/secondary cluster definition.

To check if failover clusters are provided, please go to config service and check the endpoints configurations.


### Failover in Workflows
Both `Backfiller` and `Poller` workflows provide failover feature without updating configs in the config service which requires approvals as well as redeployment.

To use failover clusters for those two workflows, you simply need to set the `Failover` workflow param as `true` when you trigger it.

Note: By default, the `Failover` workflow param should be set as `false`, which means the primary clusters should always be first choice.
If you intend to use failover clusters, please update endpoints configs in config service instead.


### Automatic Failover in Poller Workflow
`Poller` workflow provides an automatic failover mechanism so that we don't need to manually restart workflows with `Failover` param.

This feature is guarded by the `failover_enabled` configuration.
Once this feature is enabled, when the workflow execution using primary clusters fails, it will automatically trigger a new workflow run with `Failover=true`.
[Implementation Code](https://github.com/coinbase/chainstorage/blob/master/internal/workflow/poller.go#L186).

Ref: [Temporal Continue-As-New](https://docs.temporal.io/concepts/what-is-continue-as-new/#:~:text=Continue%2DAs%2DNew%20is%20a,warn%20you%20every%2010%2C000%20Events.)


### Checking Workflow Statuses

Install tctl, it is a command-line tool that you can use to interact with a Temporal cluster. More info can be found
here: https://docs.temporal.io/tctl/

```shell
brew install tctl
```

### APIs

### APIs List
[Supported APIs List](https://github.com/coinbase/chainstorage/blob/master/protos/coinbase/chainstorage/api.proto)

```shell
# local
grpcurl --plaintext localhost:9090 coinbase.chainstorage.ChainStorage/GetLatestBlock
grpcurl --plaintext -d '{"start_height": 0, "end_height": 10}' localhost:9090 coinbase.chainstorage.ChainStorage/GetBlockFilesByRange
grpcurl --plaintext -d '{"sequence_num": 2223387}' localhost:9090 coinbase.chainstorage.ChainStorage/StreamChainEvents
grpcurl --plaintext -d '{"initial_position_in_stream": "EARLIEST"}' localhost:9090 coinbase.chainstorage.ChainStorage/StreamChainEvents
grpcurl --plaintext -d '{"initial_position_in_stream": "LATEST"}' localhost:9090 coinbase.chainstorage.ChainStorage/StreamChainEvents
grpcurl --plaintext -d '{"initial_position_in_stream": "13222054"}' localhost:9090 coinbase.chainstorage.ChainStorage/StreamChainEvents
```

## SDK
Chainstorage also provides SDK, and you can find supported
methods [here](https://github.com/coinbase/chainstorage/blob/master/sdk/client.go)

Note:
- `GetBlocksByRangeWithTag` is not equivalent to the batch version of `GetBlockWithTag` since you don't have a way to
  specify the block hash.
  So when you use `GetBlocksByRangeWithTag` and if it goes beyond the current tip of chain due to reorg, you'll get back
  the `FailedPrecondition` error because it exceeds the latest watermark.

  In conclusion, it's safe to use `GetBlocksByRangeWithTag` for backfilling since the reorg will not happen for past
  blocks, however, you'd be suggested to use `GetBlockWithTag` for recent blocks (e.g. streaming case).

### Data Processing Pattern
Below are several patterns you can choose for data processing.

1. If you want the most up-to-date blocks, you need to use the streaming APIs to handle the chain reorg events:
  1. Unified batch and streaming:
    - Download, let's say 10k events, using `GetChainEvents`.
    - Break down 10k events into small batches, e.g. 20 events/batch.
    - Process those batches in parallel.
    - For events in each batch, it can be processed either sequentially or in parallel using `GetBlockWithTag`.
    - Update watermark once all small batches have been processed.
    - Repeat above steps.

     With the above pattern, you can unify batch and streaming use cases. When your data pipeline is close to the tip,
     `GetChainEvents` will simply return all available blocks.
  2. Separate workflows for backfilling and live streaming:
     Use `GetBlocksByRangeWithTag` for backfilling and then switch over to `StreamChainEvents` for live streaming.
2. If you don't want to deal with chain reorg, you may use the batch APIs as follows:
  - Maintain a distance (`irreversibleDistance`) to the tip, the irreversible distance can be queried using `GetChainMetadata`.
  - Get the latest block height (`latest`) using `GetLatestBlock`.
  - Poll for new data from current watermark block to the block (`latest - irreversibleDistance`) using `GetBlocksByRangeWithTag`.
  - Repeat above steps periodically.

## Examples

See below for a few examples for implementing a simple indexer using the SDK.
Note that the examples are provided in increasing complexity.

### Batch

In [this example](/examples/batch/main.go), we use the blocks API to fetch the confirmed blocks as follows:
1. Fetch the maximum reorg distance (`irreversibleDistance`).
2. Fetch the latest block height (`latest`).
3. Poll for new blocks from the checkpoint up to the latest confirmed block  (`latest - irreversibleDistance`).
using `GetBlocksByRange`.
4. Update the checkpoint.
5. Repeat above steps periodically.

```shell
export CHAINSTORAGE_SDK_AUTH_HEADER=cb-nft-api-token
export CHAINSTORAGE_SDK_AUTH_TOKEN=****
go run ./examples/batch
```

### Stream

[This example](/examples/stream/main.go) demonstrates how to stream the latest blocks and handle chain reorgs.
The worker processes the events sequentially and relies on [BlockchainEvent_Type](/protos/coinbase/chainstorage/api.proto)
to construct the canonical chain.
For example, given `+1, +2, +3, -3, -2, +2', +3'` as the events, the canonical chain would be `+1, +2', +3'`.

```shell
export CHAINSTORAGE_SDK_AUTH_HEADER=cb-nft-api-token
export CHAINSTORAGE_SDK_AUTH_TOKEN=****
go run ./examples/stream
```

### Unified

The [last example](/examples/unified/main.go) showcases how to turn the data processing into an embarrassingly parallel
problem by leveraging the mono-increasing sequence number. In this example, though the events are processed in parallel
and out of order, the logical ordering guarantee is preserved.
1. Download, say 10k events, using `GetChainEvents`. Note that this API is non-blocking, and it returns all the
   available events if the requested amount is not available. This enables us to unify batch and stream processing.
2. Break down 10k events into small batches, e.g. 20 events/batch.
3. Distribute those batches to a number of workers for parallel processing.
   Note that this step is not part of the example.
4. For events in each batch, it can be processed either sequentially or in parallel using `GetBlockWithTag`.
5. Implement versioning using the mono-increasing sequence numbers provided by the events.
   See [here](https://aws.amazon.com/blogs/database/implementing-version-control-using-amazon-dynamodb/) for more details.
6. Update watermark once all the batches have been processed.
7. Repeat above steps.

```shell
export CHAINSTORAGE_SDK_AUTH_HEADER=cb-nft-api-token
export CHAINSTORAGE_SDK_AUTH_TOKEN=****
go run ./examples/unified
```

## Public APIs

The ChainStorage APIs are in beta preview. Note that the APIs are currently exposed as restful APIs through grpc
transcoding. Please refer to the [proto file](/protos/coinbase/chainstorage/api.proto) for the data schema.

See below for a few examples.

```shell
export CHAINSTORAGE_SDK_AUTH_TOKEN=****

curl -s -X POST \
  -H "content-type: application/json" \
  -H "x-apikey: ${CHAINSTORAGE_SDK_AUTH_TOKEN}" \  
  https://launchpad.coinbase.com/api/exp/chainstorage/ethereum/mainnet/v1/coinbase.chainstorage.ChainStorage/GetLatestBlock | jq

curl -s -X POST \
  -H "content-type: application/json" \
  -H "x-apikey: ${CHAINSTORAGE_SDK_AUTH_TOKEN}" \
  -d '{"height": 16000000}' \
  https://launchpad.coinbase.com/api/exp/chainstorage/ethereum/mainnet/v1/coinbase.chainstorage.ChainStorage/GetNativeBlock | jq

curl -s -X POST \
  -H "content-type: application/json" \
  -H "x-apikey: ${CHAINSTORAGE_SDK_AUTH_TOKEN}" \
  -d '{"start_height": 16000000, "end_height": 16000005}' \
  https://launchpad.coinbase.com/api/exp/chainstorage/ethereum/mainnet/v1/coinbase.chainstorage.ChainStorage/GetNativeBlocksByRange | jq
```

## Contact Us

We have set up a Discord server soon. Here is the link to join (limited 10) https://discord.com/channels/1079683467018764328/1079683467786334220.
