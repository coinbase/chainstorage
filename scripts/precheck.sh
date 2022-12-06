#!/usr/bin/env bash

set -eo pipefail

# Check if protobuf is installed
if ! [ -x "$(command -v protoc)" ]; then
  echo 'Error: protobuf is not installed. Please refer https://grpc.io/docs/protoc-installation/ for installation instruction' >&2
  exit 1
fi
