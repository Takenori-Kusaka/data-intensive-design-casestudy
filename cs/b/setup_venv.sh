#!/usr/bin/env bash
# Topic B needs the Kafka client (as topic A) and the three serialization libraries.
set -euo pipefail
"$(dirname "$0")/../a/setup_venv.sh"
VENV="${DIC_VENV:-$HOME/dic-cs/.venv}"
export PATH="$HOME/.local/bin:$PATH"
uv pip install --python "$VENV/bin/python" "fastavro>=1.9" "protobuf>=5" "jsonschema>=4.20"
"$VENV/bin/python" -c "import fastavro, google.protobuf, jsonschema; print('fastavro', fastavro.__version__, 'protobuf', google.protobuf.__version__, 'jsonschema', jsonschema.__version__)"
