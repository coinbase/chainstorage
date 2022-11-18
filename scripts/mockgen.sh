#!/usr/bin/env bash

set -eo pipefail

# Read the mocks.yaml file and run mockgen for each item
packges=$(yq e ".gomocks[].package" mocks.yml)
for package in $packges; do
    interfaces=$(yq e ".gomocks[] | select(.package == \"${package}\") | .interfaces | join(\",\")" mocks.yml)
    mockgen -destination "${package}/mocks/mocks.go" -package "${package##*/}mocks" "github.com/coinbase/chainstorage/${package}" "${interfaces}"
done
