SHELL := /bin/bash
GO ?= go
BIN := bin

# The UI is embedded in the orchd binary, so it must be built first.
UI_DIST := internal/ui/dist/index.html

.PHONY: all build ui proto test test-integration lint clean dev-up dev-down demo db

all: build

## build: compile the UI, then all three binaries
build: ui
	@mkdir -p $(BIN)
	$(GO) build -o $(BIN)/orchd ./cmd/orchd
	$(GO) build -o $(BIN)/orchd-agent ./cmd/orchd-agent
	$(GO) build -o $(BIN)/orchctl ./cmd/orchctl
	@echo "built: $(BIN)/orchd $(BIN)/orchd-agent $(BIN)/orchctl"

## ui: build the React app into internal/ui/dist
ui: $(UI_DIST)

$(UI_DIST): $(shell find web/src web/index.html -type f 2>/dev/null) web/package.json
	cd web && npm install --no-audit --no-fund && npm run build

## proto: regenerate the ConnectRPC bindings
proto:
	buf lint
	buf generate

## test: unit tests (no database required)
test:
	$(GO) test ./...

## test-integration: unit tests plus the Postgres-backed store tests
test-integration:
	@test -n "$$ORCH_TEST_DSN" || { \
	  echo "set ORCH_TEST_DSN, e.g. postgres:///orch_test?host=/var/run/postgresql"; exit 1; }
	$(GO) test ./... -count=1

## test-ui: render every component against a running control plane
test-ui:
	cd web && node render-check.mjs $${ORCH_SERVER:-http://localhost:9443}

lint:
	$(GO) vet ./...
	gofmt -l cmd internal

## db: create the local development database
db:
	createdb orch 2>/dev/null || echo "database 'orch' already exists"

dev-up: build
	./scripts/dev-up.sh

dev-down:
	./scripts/dev-down.sh

## demo: run the §14 story end to end
demo:
	ORCHCTL=$(BIN)/orchctl ./scripts/demo.sh

clean:
	rm -rf $(BIN) internal/ui/dist .run
	@echo "note: internal/ui/dist is required to compile orchd; run 'make ui'"
