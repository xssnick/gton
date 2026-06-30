.PHONY: all build build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64 build-windows-amd64 prepare-build

BUILD_DIR := build
PACKAGE := ./cmd/node
BINARY := gton-node
GIT_COMMIT ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo unknown)
LDFLAGS := -w -s -X github.com/xssnick/gton/cmd/node/node.GitCommit=$(GIT_COMMIT)

build: prepare-build
	go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY) $(PACKAGE)

all: build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64 build-windows-amd64

build-linux-amd64: prepare-build
	GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-linux-amd64 $(PACKAGE)

build-linux-arm64: prepare-build
	GOOS=linux GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-linux-arm64 $(PACKAGE)

build-darwin-amd64: prepare-build
	GOOS=darwin GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-darwin-amd64 $(PACKAGE)

build-darwin-arm64: prepare-build
	GOOS=darwin GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-darwin-arm64 $(PACKAGE)

build-windows-amd64: prepare-build
	GOOS=windows GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-windows-amd64.exe $(PACKAGE)

prepare-build:
	mkdir -p $(BUILD_DIR)
