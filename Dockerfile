# Build stage
ARG BASE_REGISTRY=""
FROM ${BASE_REGISTRY}library/golang:1.26-trixie AS build

ENV GO111MODULE=on
# Build a fully static binary (no cgo). chainstorage has no cgo code, and the
# deploy stage runs on ubuntu:22.04 (glibc 2.35) -- older than any golang base
# image -- so a dynamically-linked binary would couple to the builder's glibc.
# CGO_ENABLED=0 removes the libc dependency entirely.
ENV CGO_ENABLED=0

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN make bin

# Deploy stage
FROM ${BASE_REGISTRY}library/ubuntu:22.04 AS deploy

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends ca-certificates && update-ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app/

COPY --from=build /app/bin/* ./
COPY --from=build /app/internal/storage/metastorage/postgres/db/migrations ./migrations

CMD ["/app/worker"]
