#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
BUILD_DIR="$ROOT/build"
DOCKER="${BUILD_RELEASE_DOCKER:-docker}"
IMAGE="${BUILD_RELEASE_IMAGE:-golang:1.26-alpine}"
GOAMD64_LEVEL="${BUILD_RELEASE_GOAMD64:-v3}"
GOARM64_LEVEL="${BUILD_RELEASE_GOARM64:-v8.0,lse}"
BUILD_TAGS="${BUILD_RELEASE_TAGS:-netgo,pebblegozstd}"
GIT_COMMIT=$(git -C "$ROOT" describe --tags --always --dirty 2>/dev/null || echo unknown)

die() {
	echo "build-release: $*" >&2
	exit 1
}

command -v "$DOCKER" >/dev/null 2>&1 || die "$DOCKER is not installed"
"$DOCKER" info >/dev/null 2>&1 || die "Docker daemon is not available"
mkdir -p "$BUILD_DIR"
rm -f \
	"$BUILD_DIR/gton-node-linux-amd64" \
	"$BUILD_DIR/gton-node-linux-arm64" \
	"$BUILD_DIR/gton-node-darwin-amd64" \
	"$BUILD_DIR/gton-node-darwin-arm64" \
	"$BUILD_DIR/gton-node-windows-amd64.exe"

run_builder() {
	local platform="$1"
	local cache="$2"
	shift 2

	"$DOCKER" run --rm --platform "$platform" \
		-v "$ROOT:/src:ro" \
		-v "$BUILD_DIR:/out" \
		-v flexserver-release-gomod:/go/pkg/mod \
		-v "$cache:/root/.cache/go-build" \
		-w /src \
		-e GOAMD64="$GOAMD64_LEVEL" \
		-e GOARM64="$GOARM64_LEVEL" \
		-e BUILD_DIR=/out \
		-e BUILD_FLAGS="-trimpath -buildvcs=false" \
		-e BUILD_TAGS="$BUILD_TAGS" \
		-e GIT_COMMIT="$GIT_COMMIT" \
		-e NATIVE_LDFLAGS_EXTRA="-linkmode=external -extldflags=-static" \
		-e LOCAL_UID="$(id -u)" \
		-e LOCAL_GID="$(id -g)" \
		"$IMAGE" sh -euc '
			apk add --no-cache build-base binutils >/dev/null
			make "$@"

			arch=$(go env GOARCH)
			binary="/out/gton-node-linux-$arch"
			test -s "$binary"
			go version -m "$binary" | grep -qE "^[[:space:]]*build[[:space:]]+CGO_ENABLED=1$"
			go version -m "$binary" | grep -qE "^[[:space:]]*build[[:space:]]+GOOS=linux$"
			go version -m "$binary" | grep -qE "^[[:space:]]*build[[:space:]]+GOARCH=$arch$"
			! readelf -l "$binary" | grep -q INTERP

			chown "$LOCAL_UID:$LOCAL_GID" /out/gton-node-*
		' sh "$@"
}

echo "build-release: linux/amd64 native build (GOAMD64=$GOAMD64_LEVEL)"
run_builder linux/amd64 flexserver-release-gobuild-amd64 \
	native build-darwin-amd64 build-darwin-arm64 build-windows-amd64

if "$DOCKER" run --rm --platform linux/arm64 "$IMAGE" \
	sh -euc 'test "$(go env GOARCH)" = arm64' >/dev/null 2>&1; then
	echo "build-release: linux/arm64 native build (GOARM64=$GOARM64_LEVEL)"
	run_builder linux/arm64 flexserver-release-gobuild-arm64 native
else
	echo "build-release: linux/arm64 is unavailable in this Docker setup; skipping" >&2
fi

echo "build-release: artifacts"
find "$BUILD_DIR" -maxdepth 1 -type f -name 'gton-node-*' -print | sort
