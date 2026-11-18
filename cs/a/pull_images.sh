#!/usr/bin/env bash
# Pull the pinned images for topic A (tags are defined in cs/a/config.py; keep in sync).
set -euo pipefail
for img in postgres:18.7-alpine3.24 ghcr.io/shopify/toxiproxy:2.13.0 \
           docker.redpanda.com/redpandadata/redpanda:v26.2.7 apache/kafka:4.4.0 \
           quay.io/debezium/connect:3.7.1.Final; do
  docker pull -q "$img"
done
docker images --format '{{.Repository}}:{{.Tag}} {{.Size}}' | grep -E 'redpanda|apache/kafka|debezium/connect'
