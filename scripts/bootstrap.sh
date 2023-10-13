#!/usr/bin/env bash

set -exo pipefail

# Use asdf if it's available
if [ -x "$(command -v asdf)" ]; then
  if ! [ -x "$(command -v go)" ]; then
    if ! asdf plugin-list | grep -e '^golang$' &> /dev/null; then
      asdf plugin add golang https://github.com/asdf-community/asdf-golang.git
    fi
  fi
  if ! asdf plugin-list | grep -e '^yq$' &> /dev/null; then
    asdf plugin-add yq https://github.com/sudermanjr/asdf-yq.git
  fi
  if ! asdf plugin-list | grep -e '^protoc$' &> /dev/null; then
    asdf plugin-add protoc https://github.com/paxosglobal/asdf-protoc.git
  fi
  if ! asdf plugin-list | grep -e '^grpcurl$' &> /dev/null; then
    asdf plugin-add grpcurl https://github.com/asdf-community/asdf-grpcurl.git
  fi
  asdf install
else
  source ./scripts/precheck.sh
  go install github.com/mikefarah/yq/v4@v4.35.2
fi

go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
go install github.com/golang/mock/mockgen@v1.6.0
go install github.com/gordonklaus/ineffassign@v0.0.0-20230610083614-0e73809eb601
go install github.com/kisielk/errcheck@v1.6.3
go install github.com/go-bindata/go-bindata/go-bindata@v3.1.2
go install golang.org/x/tools/cmd/goimports@v0.13.0
go mod download
go mod tidy

if [ -x "$(command -v asdf)" ]; then
  asdf reshim
fi
