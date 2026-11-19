#!/usr/bin/env bash
# Dev: run the given run ids without recording (see dev_one.py), then show leftovers and memory.
cd "$(dirname "$0")/../.."
VENV="${DIC_VENV:-$HOME/dic-cs/.venv}"
timeout -s INT "${DIC_SLICE_S:-540}" "$VENV/bin/python" -u -m cs.c.dev_one "$@" 2>&1 | tail -n "${DIC_TAIL:-40}"
echo "left: $(docker ps -a --format '{{.Names}}' | grep '^dic-c-' | tr '\n' ' ')"
echo "windows_free_gb=$(powershell.exe -NoProfile -Command '[math]::Round((Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory/1MB,1)' | tr -d '\r')"
