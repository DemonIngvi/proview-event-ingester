BIN      := event-ingester
IMAGE    ?= ghcr.io/demoningvi/proview-event-ingester
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
CHART    := charts/proview-event-ingester

.PHONY: all build test test-integration cover lint docker helm-lint helm-template clean

all: lint test build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=$(VERSION)" -o bin/$(BIN) ./cmd/$(BIN)

test:
	go test -race -count=1 ./...

test-integration:
	go test -race -count=1 -tags integration ./...

cover:
	go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | tail -1

lint:
	golangci-lint run ./...

docker:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

helm-lint:
	helm lint $(CHART) --strict

helm-template:
	helm template test $(CHART) | kubeconform -strict -ignore-missing-schemas -summary

clean:
	rm -rf bin coverage.out
