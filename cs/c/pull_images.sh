#!/usr/bin/env bash
# Images of topic C. PostgreSQL, ClickHouse and Redpanda were pulled for topics B / D;
# only the Collector is new (about 0.35 GB).
set -euo pipefail
for img in postgres:18.7-alpine3.24 clickhouse:26.8.36.3 docker.redpanda.com/redpandadata/redpanda:v26.2.7 \
           otel/opentelemetry-collector-contrib:0.165.0; do
  docker image inspect "$img" >/dev/null 2>&1 || docker pull -q "$img"
  docker image inspect "$img" --format '{{index .RepoTags 0}} {{index .RepoDigests 0}} {{.Size}}'
done
