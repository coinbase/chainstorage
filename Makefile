TARGET ?= ...
TEST_FILTER ?= .
ifeq ($(TEST_FILTER),.)
	TEST_INTEGRATION_FILTER=TestIntegration
else
	TEST_INTEGRATION_FILTER=$(TEST_FILTER)
endif

SRCS=$(shell find . -name '*.go' -type f | grep -v -e ./protos -e /mocks -e '^./config/config.go')

.EXPORT_ALL_VARIABLES:
CHAINSTORAGE_CONFIG ?= ethereum-mainnet
GO111MODULE ?= on

ifeq ($(CI),)
define docker_compose_up
	docker-compose -f docker-compose-testing.yml up -d --force-recreate
	sleep 10
endef
define docker_compose_down
	docker-compose -f docker-compose-testing.yml down
endef
else
define docker_compose_up
endef
define docker_compose_down
endef
endif

.PHONY: build
build: config codegen fmt bin
	@echo "--- build"

.PHONY: bootstrap
bootstrap:
	@echo "--- bootstrap"
	scripts/bootstrap.sh

.PHONY: validate
build-validate: proto
	@echo "--- build-validate"
	git add -N . && git diff --exit-code

.PHONY: bin
bin:
	@echo "--- bin"
	mkdir -p bin
	go build -o bin ./$(TARGET)

.PHONY: docker-build
docker-build:
	@echo "--- docker-build"
	docker build -t coinbase/chainstorage .

.PHONY: validate-configs
validate-configs:
	@echo "--- validate-config"
	TEST_TYPE=unit go test ./internal/config -run=TestValidateConfigs$

.PHONY: test
test: fmt lint
	@echo "--- test"
	TEST_TYPE=unit go test ./$(TARGET) -run=$(TEST_FILTER)

.PHONY: lint
lint:
	@echo "--- lint"
	go vet -printfuncs=wrapf,statusf,warnf,infof,debugf,failf,equalf,containsf,fprintf,sprintf ./...
	errcheck -ignoretests -ignoregenerated ./...
	ineffassign ./...

.PHONY: integration
integration:
	@echo "--- integration"
	$(call docker_compose_up)
	TEST_TYPE=integration go test ./$(TARGET) -v -p=1 -parallel=1 -timeout=15m -failfast -run=$(TEST_INTEGRATION_FILTER)
	$(call docker_compose_down)

.PHONY: functional
functional:
	@echo "--- functional"
	$(call docker_compose_up)
	TEST_TYPE=functional go test ./$(TARGET) -v -p=1 -parallel=1 -timeout=45m -failfast -run=$(TEST_INTEGRATION_FILTER)
	$(call docker_compose_down)

.PHONY: benchmark
benchmark:
	@echo "--- benchmark"
	$(call docker_compose_up)
	go test -v ./internal/blockchain/integration_test -tags=integration -run=^_ -bench=$(TEST_FILTER) -benchtime=200x
	$(call docker_compose_down)

.PHONY: proto
proto:
	@echo "--- proto"
	./scripts/protogen.sh

.PHONY: codegen
codegen: proto
	@echo "--- codegen"
	./scripts/mockgen.sh

.PHONY: config
config:
	@echo "--- config"
	go run ./tools/config_gen ./config_templates/config .

.PHONY: fmt
fmt:
	@echo "--- fmt"
	@goimports -l -w -local github.com/coinbase/chainstorage $(SRCS)

.PHONY: server
server:
	go run ./cmd/server

.PHONY: cron
cron:
	go run ./cmd/cron

.PHONY: api
api:
	go run ./cmd/api

.PHONY: worker
worker:
	go run ./cmd/worker

.PHONY: localstack
localstack:
	docker-compose -f docker-compose-local.yml down -v && docker-compose -f docker-compose-local.yml up -d

.PHONY: docker-run
docker-run:
	docker run --rm --network host --name chainstorage coinbase/chainstorage
