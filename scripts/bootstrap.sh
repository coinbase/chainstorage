#!/usr/bin/env bash

set -exo pipefail

PROTOC_VER=3.13.0
PROTOC_ZIP=protoc-${PROTOC_VER}-osx-x86_64.zip
curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VER}/${PROTOC_ZIP}
unzip -o ${PROTOC_ZIP} -d /usr/local bin/protoc
unzip -o ${PROTOC_ZIP} -d /usr/local 'include/*'
rm -f ${PROTOC_ZIP}

go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.25
go install github.com/mikefarah/yq/v4@v4.27.5
go install github.com/golang/mock/mockgen@v1.6.0
go install github.com/gordonklaus/ineffassign@v0.0.0-20210914165742-4cc7213b9bc8
go install github.com/kisielk/errcheck@v1.6.0
go install github.com/go-bindata/go-bindata/go-bindata@v3.1.2
go install golang.org/x/tools/cmd/goimports@v0.1.10
go mod download
go mod tidy
