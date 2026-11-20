"""Memory log during a full run: about every 60 seconds one line is appended to
results/a/memlog.jsonl with the Windows free memory, the working set of the
WSL VM process (vmmemWSL) and the used / buff-cache figures inside WSL."""
import datetime
import json
import os
import shutil
import subprocess
import threading

from ..common.record import RESULTS

PATH = os.path.join(RESULTS, 'a', 'memlog.jsonl')
_PS = ['powershell.exe', '/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe']
_CMD = ("$f=[math]::Round((Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory/1MB,2);"
        "$p=Get-Process vmmemWSL,vmmem -ErrorAction SilentlyContinue | Sort-Object WorkingSet64 -Descending | "
        "Select-Object -First 1;$w=if($p){[math]::Round($p.WorkingSet64/1GB,2)}else{-1};Write-Output \"$f $w\"")


def _windows():
    for ps in _PS:
        exe = shutil.which(ps) or (ps if os.path.exists(ps) else None)
        if not exe:
            continue
        try:
            r = subprocess.run([exe, '-NoProfile', '-Command', _CMD], capture_output=True, text=True, timeout=40)
            a, b = r.stdout.strip().replace(',', '.').split()[:2]
            return float(a), (None if float(b) < 0 else float(b))
        except Exception:  # noqa: BLE001
            continue
    return None, None


def _wsl():
    m = {}
    with open('/proc/meminfo') as f:
        for line in f:
            k, v = line.split(':', 1)
            m[k] = int(v.split()[0]) // 1024
    cache = m.get('Buffers', 0) + m.get('Cached', 0) + m.get('SReclaimable', 0)
    return {'wsl_total_mb': m['MemTotal'], 'wsl_used_mb': m['MemTotal'] - m['MemFree'] - cache,
            'wsl_buff_cache_mb': cache, 'wsl_available_mb': m.get('MemAvailable')}


def snapshot(label=''):
    free, vm = _windows()
    return {'time': datetime.datetime.now().astimezone().strftime('%Y-%m-%dT%H:%M:%S%z'), 'label': label,
            'windows_free_gb': free, 'vmmem_wsl_working_set_gb': vm, **_wsl()}


class NoLog:
    """Stands in for MemLog when nothing is to be written (additional steps)."""
    label = ''

    def stop(self):
        pass


class MemLog:
    def __init__(self, interval=60):
        self.interval, self.label = interval, ''
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._loop, daemon=True)

    def _write(self):
        try:
            row = snapshot(self.label)
            os.makedirs(os.path.dirname(PATH), exist_ok=True)
            with open(PATH, 'a', encoding='utf-8', newline='\n') as f:
                f.write(json.dumps(row, sort_keys=True) + '\n')
        except Exception:  # noqa: BLE001 - the log must never stop the experiment
            pass

    def _loop(self):
        self._write()
        while not self._stop.wait(self.interval):
            self._write()

    def start(self):
        self._t.start()
        return self

    def stop(self):
        self._stop.set()
        self._write()
