# syntax=docker/dockerfile:1

ARG GO_VERSION=1.26
FROM golang:${GO_VERSION}-alpine

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV GOCACHE=/tmp/kinesis-consumer-go-build-cache

CMD ["go", "test", "./..."]
