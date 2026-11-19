#!/usr/bin/env bash
# Pull the pinned images for topic B (tags are defined in cs/b/config.py; keep in sync).
set -euo pipefail
for img in postgres:18.7-alpine3.24 docker.redpanda.com/redpandadata/redpanda:v26.2.7 apache/kafka:4.4.0 \
           quay.io/debezium/connect:3.7.1.Final clickhouse:26.8.36.3; do
  docker image inspect "$img" >/dev/null 2>&1 || docker pull -q "$img"
done
docker images --format '{{.Repository}}:{{.Tag}} {{.Size}}' | grep -E 'redpanda|apache/kafka|debezium/connect|clickhouse|postgres:18'
