#!/usr/bin/env bash
# Entry point inside WSL: bash cs/c/run.sh [args for cs.c.run]
set -euo pipefail
cd "$(dirname "$0")/../.."
VENV="${DIC_VENV:-$HOME/dic-cs/.venv}"
exec "$VENV/bin/python" -u -m cs.c.run "$@"
