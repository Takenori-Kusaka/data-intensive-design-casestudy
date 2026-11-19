#!/usr/bin/env bash
# Development helper: bash cs/b/dev.sh <run id pattern> [keys...]; output is cut to the last lines.
cd "$(dirname "$0")/../.."
timeout -s INT 500 bash cs/b/py.sh cs.b.dev_one "$@" 2>&1 | tail -n "${DIC_TAIL:-60}"
