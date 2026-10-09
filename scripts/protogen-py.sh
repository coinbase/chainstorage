#!/usr/bin/env bash

set -eo pipefail

python -m grpc_tools.protoc \
    --python_out=gen/src/python \
    --grpc_python_out=gen/src/python \
    --proto_path=protos \
    protos/coinbase/chainstorage/*.proto \
    protos/coinbase/c3/common/*.proto \
    protos/coinbase/crypto/rosetta/types/*.proto
