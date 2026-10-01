.PHONY: all build build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64 build-windows-amd64 native prepare-build

BUILD_DIR ?= build
PACKAGE := ./cmd/node
BINARY := gton-node
GIT_COMMIT ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo unknown)
LDFLAGS := -w -s -X github.com/xssnick/gton/cmd/node/node.GitCommit=$(GIT_COMMIT)
BUILD_FLAGS ?= -trimpath -buildvcs=false
BUILD_TAGS ?=
NATIVE_LDFLAGS_EXTRA ?=
GO_BUILD = go build $(BUILD_FLAGS) $(if $(strip $(BUILD_TAGS)),-tags "$(BUILD_TAGS)")

build: prepare-build
	$(GO_BUILD) -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY) $(PACKAGE)

all: build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64 build-windows-amd64

build-linux-amd64: prepare-build
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO_BUILD) -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-linux-amd64 $(PACKAGE)

build-linux-arm64: prepare-build
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO_BUILD) -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-linux-arm64 $(PACKAGE)

build-darwin-amd64: prepare-build
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GO_BUILD) -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-darwin-amd64 $(PACKAGE)

build-darwin-arm64: prepare-build
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GO_BUILD) -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-darwin-arm64 $(PACKAGE)

build-windows-amd64: prepare-build
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GO_BUILD) -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-windows-amd64.exe $(PACKAGE)

native: prepare-build
	CGO_ENABLED=1 $(GO_BUILD) -ldflags "$(LDFLAGS) $(NATIVE_LDFLAGS_EXTRA)" -o $(BUILD_DIR)/$(BINARY)-$(shell go env GOOS)-$(shell go env GOARCH) $(PACKAGE)

prepare-build:
	mkdir -p $(BUILD_DIR)
