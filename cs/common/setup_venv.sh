#!/usr/bin/env bash
# Create the Python environment for the experiments inside the WSL home (not on the Windows drive).
set -euo pipefail
VENV="${DIC_VENV:-$HOME/dic-cs/.venv}"
export PATH="$HOME/.local/bin:$PATH"
if [ ! -x "$VENV/bin/python" ]; then
  mkdir -p "$(dirname "$VENV")"
  uv venv "$VENV" --python 3.12
fi
uv pip install --python "$VENV/bin/python" "psycopg[binary]>=3.2" "pymysql>=1.1" "cryptography>=43"
"$VENV/bin/python" -c "import psycopg, pymysql; print('psycopg', psycopg.__version__, 'pymysql', pymysql.__version__)"
