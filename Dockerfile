# syntax=docker/dockerfile:1

ARG GO_VERSION=1.26
FROM golang:${GO_VERSION}-alpine

WORKDIR /workspace

# Copy every module's go.mod/go.sum plus the workspace files BEFORE the source
# tree, so `go mod download` runs workspace-aware and this layer caches all
# dependency downloads: the test run below never fetches modules at runtime,
# and the layer is only invalidated by dependency changes, not source edits.
COPY go.work go.work.sum go.mod go.sum ./
COPY pkg/backend/valkey/go.mod pkg/backend/valkey/go.sum ./pkg/backend/valkey/
COPY examples/valkey/go.mod examples/valkey/go.sum ./examples/valkey/
COPY test/integration/go.mod test/integration/go.sum ./test/integration/
RUN go mod download

COPY . .

ENV GOCACHE=/tmp/kinesis-consumer-go-build-cache

CMD ["go", "test", "./..."]
