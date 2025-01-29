GOCMD=go
GOTEST=$(GOCMD) test
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt
GOVET=$(GOCMD) vet

.PHONY: test run fmt vet tidy help

download: ## Download project dependencies
	$(GOMOD) download

test: ## Run tests
	$(GOTEST) -v ./...

fmt: ## Run go fmt
	$(GOFMT) ./...

vet: ## Run go vet
	$(GOVET) ./...

tidy: ## Tidy up module files
	$(GOMOD) tidy

help: ## Display this help message
	@cat $(MAKEFILE_LIST) | grep -e "^[a-zA-Z_-]*: *.*## *" | \
      awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help