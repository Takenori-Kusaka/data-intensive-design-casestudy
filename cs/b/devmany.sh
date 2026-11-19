#!/usr/bin/env bash
# Development helper: run several run ids one after another and print only the "brief" of each.
cd "$(dirname "$0")/../.."
for pat in "$@"; do
  echo "== $pat"
  timeout -s INT 280 bash cs/b/py.sh cs.b.dev_one "$pat" brief 2>&1 | tail -n "${DIC_TAIL:-4}" | cut -c1-900
done
docker ps -a --format '{{.Names}}' | grep '^dic-b-' | tr '\n' ' '
