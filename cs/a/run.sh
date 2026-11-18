#!/usr/bin/env bash
# Entry point inside WSL: bash cs/a/run.sh [args for cs.a.run]
set -euo pipefail
cd "$(dirname "$0")/../.."
VENV="${DIC_VENV:-$HOME/dic-cs/.venv}"
exec "$VENV/bin/python" -u -m cs.a.run "$@"
