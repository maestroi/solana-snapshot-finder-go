#!/usr/bin/env bash
set -euo pipefail

OUT="dist/solana-snapshot-finder"
mkdir -p dist

GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
  go build -ldflags="-s -w" -o "$OUT" ./cmd/solana-snapshot-finder

echo "Built: $OUT"
