#!/usr/bin/env bash
# Topic C needs DuckDB, plus numpy and pyarrow for the generator (CSV / NDJSON writer and the truth).
set -euo pipefail
"$(dirname "$0")/../common/setup_venv.sh"
VENV="${DIC_VENV:-$HOME/dic-cs/.venv}"
export PATH="$HOME/.local/bin:$PATH"
uv pip install --python "$VENV/bin/python" "duckdb>=1.1" "numpy>=2" "pyarrow>=17"
"$VENV/bin/python" -c "import duckdb, numpy, pyarrow; print('duckdb', duckdb.__version__, 'numpy', numpy.__version__, 'pyarrow', pyarrow.__version__)"
