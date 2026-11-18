#!/usr/bin/env bash
# Entry point inside WSL: bash cs/d/run.sh [args for cs.d.run]
set -euo pipefail
cd "$(dirname "$0")/../.."
VENV="${DIC_VENV:-$HOME/dic-cs/.venv}"
exec "$VENV/bin/python" -u -m cs.d.run "$@"
