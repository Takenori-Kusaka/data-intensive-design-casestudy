#!/usr/bin/env bash
# One foreground slice of the run (at most 9 minutes), then the line counts and the free memory.
#   bash cs/c/go.sh <steps and options for run.py>
cd "$(dirname "$0")/../.."
timeout -s INT "${DIC_SLICE_S:-540}" bash cs/c/run.sh "$@" 2>&1 | grep -v '^\s' | tail -n "${DIC_TAIL:-8}"
wc -l results/c/*.jsonl 2>/dev/null | tr '\n' ';'
echo
echo "left: $(docker ps -a --format '{{.Names}}' | grep '^dic-c-' | tr '\n' ' ')"
echo "windows_free_gb=$(powershell.exe -NoProfile -Command '[math]::Round((Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory/1MB,1)' | tr -d '\r')"
