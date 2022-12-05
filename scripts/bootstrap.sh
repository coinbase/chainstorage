#!/usr/bin/env bash

set -exo pipefail

source ./scripts/precheck.sh


go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.25
go install github.com/mikefarah/yq/v4@v4.27.5
go install github.com/golang/mock/mockgen@v1.6.0
go install github.com/gordonklaus/ineffassign@v0.0.0-20210914165742-4cc7213b9bc8
go install github.com/kisielk/errcheck@v1.6.0
go install github.com/go-bindata/go-bindata/go-bindata@v3.1.2
go install git.org/x/tools/cmd/goimports@v0.1.10
go mod download
go mod tidy
