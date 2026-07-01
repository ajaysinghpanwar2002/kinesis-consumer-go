GO ?= go
GOCACHE ?= /private/tmp/kinesis-consumer-go-build-cache
DOCKER ?= docker
DOCKER_IMAGE ?= kinesis-consumer-go-dev
DOCKER_GO_VERSION ?= 1.26.3
PKGS ?= ./...

.DEFAULT_GOAL := test

.PHONY: test
test:
	GOCACHE=$(GOCACHE) $(GO) test $(PKGS)

.PHONY: build
build:
	GOCACHE=$(GOCACHE) $(GO) build $(PKGS)

.PHONY: tidy
tidy:
	$(GO) mod tidy

.PHONY: docker-build
docker-build:
	$(DOCKER) build --build-arg GO_VERSION=$(DOCKER_GO_VERSION) -t $(DOCKER_IMAGE) .

.PHONY: docker-test
docker-test: docker-build
	$(DOCKER) run --rm $(DOCKER_IMAGE)
