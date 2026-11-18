#!/usr/bin/env bash
# Topic A needs a Kafka client on top of the common environment.
set -euo pipefail
"$(dirname "$0")/../common/setup_venv.sh"
VENV="${DIC_VENV:-$HOME/dic-cs/.venv}"
export PATH="$HOME/.local/bin:$PATH"
uv pip install --python "$VENV/bin/python" "confluent-kafka>=2.6"
"$VENV/bin/python" -c "import confluent_kafka as c; print('confluent-kafka', c.version(), 'librdkafka', c.libversion())"
