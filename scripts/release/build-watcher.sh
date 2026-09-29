#!/usr/bin/env bash
# Build kilnfi/cosmos-validator-watcher (MIT) from source with portable BLS
# code, for the release archives (goreleaser before-hook) and the lab.
#
# Why we ship our own build: upstream release binaries link blst (BLS
# signatures) compiled for CPUs with ADX (Broadwell+ / x86-64-v3); older
# CPUs such as the lab's Xeon E5-v3 die at startup with "Caught SIGILL in
# blst_cgo_init" (exit 132), and upstream publishes no arm64 build.
# blst's documented fix is -D__BLST_PORTABLE__, which picks the instruction
# set at runtime. Same source and `go build` as upstream `make build`
# otherwise; arm64 is cross-compiled (cgo, aarch64-linux-gnu-gcc).
#
# Builds in a Debian Go container (glibc 2.36, so the binary runs on
# Ubuntu 22.04+ / Debian 12+). Needs Docker and git.
#
#   scripts/release/build-watcher.sh [OUT_DIR] [ARCH...]
#   # default: build/watcher, amd64 arm64 -> build/watcher/<arch>/cosmos-validator-watcher
set -euo pipefail

VERSION=v0.16.3                    # bump together with the e2e Cosmos faults
GO_IMAGE="golang:1.23.3-bookworm"  # upstream go.mod toolchain for v0.16.x
OUT="${1:-build/watcher}"; shift || true
ARCHES=("$@"); [ ${#ARCHES[@]} -gt 0 ] || ARCHES=(amd64 arm64)

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
git clone -q --depth 1 --branch "$VERSION" https://github.com/kilnfi/cosmos-validator-watcher "$WORK/src"
for arch in "${ARCHES[@]}"; do
  case "$arch" in
    amd64) cc=gcc; pkgs="" ;;
    arm64) cc=aarch64-linux-gnu-gcc; pkgs="gcc-aarch64-linux-gnu libc6-dev-arm64-cross" ;;
    *) echo "unsupported arch: $arch" >&2; exit 2 ;;
  esac
  docker run --rm -v "$WORK/src:/src" -w /src \
    -e CGO_ENABLED=1 -e GOOS=linux -e GOARCH="$arch" -e CC="$cc" \
    -e CGO_CFLAGS="-O2 -D__BLST_PORTABLE__" -e GOFLAGS=-trimpath \
    "$GO_IMAGE" bash -c "
      set -e
      if [ -n '$pkgs' ]; then apt-get update -qq && apt-get install -y -qq $pkgs >/dev/null; fi
      git config --global --add safe.directory /src
      go build -o build/$arch/cosmos-validator-watcher -ldflags=\"-s -w -X 'main.Version=$VERSION-portable'\"
      chown -R $(id -u):$(id -g) build"
  install -D -m 0755 "$WORK/src/build/$arch/cosmos-validator-watcher" "$OUT/$arch/cosmos-validator-watcher"
  install -D -m 0644 "$WORK/src/LICENSE" "$OUT/$arch/LICENSE"
  echo "built $OUT/$arch/cosmos-validator-watcher ($VERSION, portable blst)"
done
sha256sum "$OUT"/*/cosmos-validator-watcher
