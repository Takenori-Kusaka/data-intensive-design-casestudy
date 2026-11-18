"""Free-memory check. The limit is on the Windows host (free physical memory),
read through WSL interop. Falls back to WSL MemAvailable when interop is off."""
import os
import shutil
import subprocess

MIN_FREE_GB = float(os.environ.get('DIC_MIN_FREE_GB', '3.0'))
_PS_CANDIDATES = ['powershell.exe', '/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe']
_CMD = '[math]::Round((Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory/1MB,2)'


class LowMemory(RuntimeError):
    pass


def windows_free_gb():
    for ps in _PS_CANDIDATES:
        exe = shutil.which(ps) or (ps if os.path.exists(ps) else None)
        if not exe:
            continue
        try:
            r = subprocess.run([exe, '-NoProfile', '-Command', _CMD], capture_output=True, text=True, timeout=30)
            return float(r.stdout.strip().replace(',', '.'))
        except Exception:  # noqa: BLE001
            continue
    return None


def wsl_available_gb():
    with open('/proc/meminfo') as f:
        for line in f:
            if line.startswith('MemAvailable:'):
                return round(int(line.split()[1]) / 2**20, 2)
    return None


def snapshot():
    return {'windows_free_gb': windows_free_gb(), 'wsl_available_gb': wsl_available_gb()}


def ensure(label=''):
    """Raise LowMemory when the Windows host has less than MIN_FREE_GB free."""
    s = snapshot()
    w = s['windows_free_gb']
    if w is not None and w < MIN_FREE_GB:
        raise LowMemory(f'Windows free memory {w} GB < {MIN_FREE_GB} GB at {label}')
    return s
