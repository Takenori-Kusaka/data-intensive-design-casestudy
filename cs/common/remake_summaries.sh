#!/usr/bin/env bash
# Rebuild every summary and results/RERUN.md from the recorded JSONL files.
# Aggregation only: no experiment is run and Docker is not touched.
# Usage (inside WSL): bash cs/common/remake_summaries.sh [d a b c rerun]
set -euo pipefail
cd "$(dirname "$0")/../.."
VENV="${DIC_VENV:-$HOME/dic-cs/.venv}"
PY="$VENV/bin/python"
[ "$#" -gt 0 ] || set -- d a b c rerun
for t in "$@"; do
  case "$t" in
    rerun) "$PY" -m cs.common.compare_runs >/dev/null ;;
    *) "$PY" -m "cs.$t.aggregate" >/dev/null ;;
  esac
done
sha256sum results/*/summary.md
[ -f results/RERUN.md ] && sha256sum results/RERUN.md || true
