GO      ?= go
BIN_DIR ?= bin

.PHONY: all build test vet fmt clean

all: fmt vet build test

build:
	$(GO) build -o $(BIN_DIR)/rl-agent ./agent/cmd/rl-agent
	$(GO) build -o $(BIN_DIR)/mock-controller ./controller/cmd/mock-controller

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

clean:
	rm -rf $(BIN_DIR)
