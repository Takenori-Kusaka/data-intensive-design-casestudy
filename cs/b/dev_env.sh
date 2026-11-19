#!/usr/bin/env bash
# Show the local state relevant to topic B (images, dic-* leftovers, disk, venv packages).
export PATH="$HOME/.local/bin:$PATH"
VENV="${DIC_VENV:-$HOME/dic-cs/.venv}"
docker images --format '{{.Repository}}:{{.Tag}} {{.Size}}'
echo '--- dic- containers / volumes / networks'
docker ps -a --format '{{.Names}} {{.Status}}' | grep '^dic-' || true
docker volume ls --format '{{.Name}}' | grep '^dic-' || true
docker network ls --format '{{.Name}}' | grep '^dic-' || true
echo '--- disk'
df -h / | tail -1
echo '--- venv'
uv pip list --python "$VENV/bin/python" 2>/dev/null
free -m | head -2
