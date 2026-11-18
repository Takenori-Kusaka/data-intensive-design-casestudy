#!/usr/bin/env bash
# Pull the pinned images for topic D (tags are defined in cs/d/config.py; keep in sync).
set -euo pipefail
for img in postgres:18.7-alpine3.24 mysql:8.4.13 cockroachdb/cockroach:v26.4.0 ghcr.io/shopify/toxiproxy:2.13.0; do
  docker pull -q "$img"
done
docker images --format '{{.Repository}}:{{.Tag}} {{.Size}}' | grep -E '^(postgres:18.7|mysql:8.4.13|cockroachdb/cockroach:v26.4.0|ghcr.io/shopify/toxiproxy)'
