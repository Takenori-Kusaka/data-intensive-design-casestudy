#!/usr/bin/env bash
# Run a module of this repository with the experiment venv: bash cs/a/py.sh cs.a.<module> [args]
set -euo pipefail
cd "$(dirname "$0")/../.."
VENV="${DIC_VENV:-$HOME/dic-cs/.venv}"
exec "$VENV/bin/python" -u -m "$@"
