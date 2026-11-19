#!/usr/bin/env bash
# Show the local state (memory, disk, dic-* leftovers, images, venv packages). Writes nothing.
export PATH="$HOME/.local/bin:$PATH"
VENV="${DIC_VENV:-$HOME/dic-cs/.venv}"
free -m | head -2
df -h "$HOME" | tail -1
echo "containers: $(docker ps -a --format '{{.Names}}' | grep '^dic-' | tr '\n' ' ')"
echo "volumes: $(docker volume ls --format '{{.Name}}' | grep '^dic-' | tr '\n' ' ')"
docker images --format '{{.Repository}}:{{.Tag}} {{.Size}}' | grep -i 'postgres\|clickhouse\|otel\|redpanda'
uv pip list --python "$VENV/bin/python" 2>/dev/null | grep -i 'duckdb\|numpy\|pyarrow\|psycopg'
du -sh "${DIC_DATA_C:-$HOME/dic-cs/data-c}" 2>/dev/null
echo "windows_free_gb=$(powershell.exe -NoProfile -Command '[math]::Round((Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory/1MB,1)' | tr -d '\r')"
