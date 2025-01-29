GOCMD=go
GOTEST=$(GOCMD) test
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt
GOVET=$(GOCMD) vet

.PHONY: test run fmt vet tidy clean coverage help

ifeq ($(OS),Windows_NT)
    RM_FILE := del /f /q
else
    RM_FILE := rm -f
endif

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

clean: ## Clean build files
	$(RM_FILE) coverage.out

coverage: ## Run tests with coverage
	$(GOTEST) -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

help: ## Display this help message
	@cat $(MAKEFILE_LIST) | grep -e "^[a-zA-Z_-]*: *.*## *" | \
      awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help