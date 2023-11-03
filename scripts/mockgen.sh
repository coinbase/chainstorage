#!/usr/bin/env bash

set -eo pipefail

SCRIPTS_DIR=$(dirname "${BASH_SOURCE[0]}")
PATH=$SCRIPTS_DIR:$PATH

# Read the mocks.yaml file and run mockgen for each item
packges=$(yq.sh e ".gomocks[].package" mocks.yml)
for package in $packges; do
    interfaces=$(yq.sh e ".gomocks[] | select(.package == \"${package}\") | .interfaces | join(\",\")" mocks.yml)
    mockgen -destination "${package}/mocks/mocks.go" -package "${package##*/}mocks" "github.com/coinbase/chainstorage/${package}" "${interfaces}"
done
