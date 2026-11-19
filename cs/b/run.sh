#!/usr/bin/env bash
# Entry point inside WSL: bash cs/b/run.sh [args for cs.b.run]
set -euo pipefail
cd "$(dirname "$0")/../.."
VENV="${DIC_VENV:-$HOME/dic-cs/.venv}"
exec "$VENV/bin/python" -u -m cs.b.run "$@"
