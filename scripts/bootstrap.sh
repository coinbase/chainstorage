#!/usr/bin/env bash

set -exo pipefail

source ./scripts/precheck.sh


go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
go install github.com/mikefarah/yq/v4@v4.35.2
go install github.com/golang/mock/mockgen@v1.6.0
go install github.com/gordonklaus/ineffassign@v0.0.0-20230610083614-0e73809eb601
go install github.com/kisielk/errcheck@v1.6.3
go install github.com/go-bindata/go-bindata/go-bindata@v3.1.2
go install golang.org/x/tools/cmd/goimports@v0.13.0
go mod download
go mod tidy
