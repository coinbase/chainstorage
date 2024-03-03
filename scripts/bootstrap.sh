#!/usr/bin/env bash

set -exo pipefail

source ./scripts/precheck.sh

go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.32.0
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
go install go.uber.org/mock/mockgen@v0.4.0
go install github.com/gordonklaus/ineffassign@v0.0.0-20231123203538-98f08ee05fe7
go install github.com/kisielk/errcheck@v1.7.0
go install golang.org/x/tools/cmd/goimports@v0.18.0
go mod download
go mod tidy
