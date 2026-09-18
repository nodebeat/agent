GO ?= go
AGENT_PKG := ./cmd/nodebeat-agent
ONBOARD_PKG := ./cmd/nodebeat-onboard
BIN := bin/nodebeat-agent
ONBOARD_BIN := bin/nodebeat-onboard
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X github.com/nodebeat/agent/internal/version.Version=$(VERSION)

.PHONY: help build build-onboard test fmt vet tidy release-snapshot release

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## Build the agent binary
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN) $(AGENT_PKG)

build-onboard: ## Build the onboarding checklist binary
	$(GO) build -ldflags "$(LDFLAGS)" -o $(ONBOARD_BIN) $(ONBOARD_PKG)

test: ## Run unit tests
	$(GO) test ./...

fmt: ## Format Go sources
	$(GO) fmt ./...

vet: ## Run go vet
	$(GO) vet ./...

tidy: ## Tidy modules
	$(GO) mod tidy

release-snapshot: ## Snapshot release build (archives, checksums, SBOMs; no signing)
	goreleaser release --snapshot --clean --skip=sign

release: ## Cut a release from a tag (needs GITHUB_TOKEN; signs via CI OIDC)
	goreleaser release --clean
