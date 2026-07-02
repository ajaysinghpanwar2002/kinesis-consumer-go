GO ?= go
GOCACHE ?= /private/tmp/kinesis-consumer-go-build-cache
export GOCACHE
DOCKER ?= docker
DOCKER_IMAGE ?= kinesis-consumer-go-dev
DOCKER_GO_VERSION ?= 1.26.3

# All Go modules in the workspace. `go test`/`go build ./...` do not cross module
# boundaries, so each module must be run explicitly or the Valkey backend and the
# example are silently skipped.
MODULES ?= . pkg/backend/valkey examples/valkey
HOOKS_DIR ?= .githooks

.DEFAULT_GOAL := test

# Run the unit test suite across every module in the workspace.
.PHONY: test
test:
	@set -e; for m in $(MODULES); do \
		echo "==> go test $$m/..."; \
		( cd "$$m" && $(GO) test ./... ); \
	done

# Compile every package (including the example main) across all modules.
# Output goes to /dev/null so this is a pure compile check with no stray binaries.
.PHONY: build
build:
	@set -e; for m in $(MODULES); do \
		echo "==> go build $$m/..."; \
		( cd "$$m" && $(GO) build -o /dev/null ./... ); \
	done

# Run go vet across all modules.
.PHONY: vet
vet:
	@set -e; for m in $(MODULES); do \
		echo "==> go vet $$m/..."; \
		( cd "$$m" && $(GO) vet ./... ); \
	done

# Fail if any Go file is not gofmt-clean. gofmt is file-based, so one pass at the
# repo root covers every module.
.PHONY: fmt-check
fmt-check:
	@files=$$(gofmt -l .); \
	if [ -n "$$files" ]; then \
		echo "gofmt needed for:"; echo "$$files"; exit 1; \
	fi

# Tidy module files across all modules.
.PHONY: tidy
tidy:
	@set -e; for m in $(MODULES); do \
		echo "==> go mod tidy $$m"; \
		( cd "$$m" && $(GO) mod tidy ); \
	done

# Install the shared git hooks by pointing git at the in-repo hooks directory.
.PHONY: hooks
hooks:
	git config core.hooksPath $(HOOKS_DIR)
	@echo "git hooks installed (core.hooksPath=$(HOOKS_DIR))"

.PHONY: docker-build
docker-build:
	$(DOCKER) build --build-arg GO_VERSION=$(DOCKER_GO_VERSION) -t $(DOCKER_IMAGE) .

.PHONY: docker-test
docker-test: docker-build
	$(DOCKER) run --rm $(DOCKER_IMAGE)
