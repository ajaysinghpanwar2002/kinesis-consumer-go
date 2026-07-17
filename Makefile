GO ?= go
GOCACHE ?= /private/tmp/kinesis-consumer-go-build-cache
export GOCACHE
DOCKER ?= docker
DOCKER_IMAGE ?= kinesis-consumer-go-dev
DOCKER_GO_VERSION ?= 1.26

# All Go modules in the workspace. `go test`/`go build ./...` do not cross module
# boundaries, so each module must be run explicitly or the Valkey backend and the
# example are silently skipped.
MODULES ?= . pkg/backend/valkey examples/valkey
HOOKS_DIR ?= .githooks

# staticcheck is run through `go run @pinned` rather than a prebuilt binary so
# the analyzer is compiled with THIS repo's Go toolchain. A staticcheck binary
# built with an older Go refuses to analyze a newer module (e.g. one built with
# go1.24 cannot lint a `go 1.26` module), so pinning the source version keeps
# local and CI linting reproducible and toolchain-compatible.
STATICCHECK ?= $(GO) run honnef.co/go/tools/cmd/staticcheck@2025.1.1

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

# Static analysis across every module with staticcheck. test/integration is
# linted with its `integration` build tag so the tagged suite is analyzed too.
# Infra-free: staticcheck compiles but does not run the code.
.PHONY: lint
lint:
	@set -e; for m in $(MODULES); do \
		echo "==> staticcheck $$m/..."; \
		( cd "$$m" && $(STATICCHECK) ./... ); \
	done
	@echo "==> staticcheck $(INTEGRATION_DIR)/... (-tags integration)"
	@( cd $(INTEGRATION_DIR) && $(STATICCHECK) -tags integration ./... )

# Race-detector pass over the concurrency-heavy packages (worker/lease/drain
# orchestration in pkg/consumer, the fire-and-forget UDP reporter in
# pkg/metrics/statsd). Kept separate from `test`: the race runtime is several
# times slower, so the fast local hook stays snappy while CI always runs this.
RACE_PACKAGES ?= ./pkg/consumer ./pkg/metrics/statsd

.PHONY: test-race
test-race:
	@echo "==> go test -race $(RACE_PACKAGES)"
	@$(GO) test -race $(RACE_PACKAGES)

# The two modules third parties actually import — the vulnerability-scanning
# surface. examples/ and test/integration are never published, so a finding
# reachable only from them cannot affect a consumer.
PUBLISHED_MODULES ?= . pkg/backend/valkey

# govulncheck is run through `go run @pinned` rather than a prebuilt binary for
# the same reason as staticcheck above: the scanner must be built with THIS
# repo's Go toolchain to analyze a `go 1.26` module, and pinning keeps local
# and CI scans reproducible. Reports only findings reachable from the module's
# call graph and exits non-zero on any, so CI fails on a reachable
# vulnerability. Needs network access for the vulnerability DB (CI-only, like
# lint; not part of the offline pre-commit hook).
GOVULNCHECK ?= $(GO) run golang.org/x/vuln/cmd/govulncheck@v1.6.0

.PHONY: vulncheck
vulncheck:
	@set -e; for m in $(PUBLISHED_MODULES); do \
		echo "==> govulncheck $$m/..."; \
		( cd "$$m" && $(GOVULNCHECK) ./... ); \
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

INTEGRATION_DIR ?= test/integration
INTEGRATION_COMPOSE ?= $(INTEGRATION_DIR)/docker-compose.yml

# Compile-check the integration tests without bringing up any infrastructure.
# (go vet compiles test files; go build would skip them.) Not part of `test`.
.PHONY: integration-build
integration-build:
	( cd $(INTEGRATION_DIR) && $(GO) vet -tags integration ./... )

# Bring up LocalStack (Kinesis) + Valkey, run the integration suite behind the
# `integration` build tag, then tear the infrastructure down. Requires Docker.
.PHONY: integration
integration:
	$(DOCKER) compose -f $(INTEGRATION_COMPOSE) up -d --wait
	@status=0; \
	( cd $(INTEGRATION_DIR) && $(GO) test -tags integration -count=1 -timeout 600s ./... ) || status=$$?; \
	$(DOCKER) compose -f $(INTEGRATION_COMPOSE) down -v; \
	exit $$status

.PHONY: docker-build
docker-build:
	$(DOCKER) build --build-arg GO_VERSION=$(DOCKER_GO_VERSION) -t $(DOCKER_IMAGE) .

.PHONY: docker-test
docker-test: docker-build
	$(DOCKER) run --rm $(DOCKER_IMAGE)
