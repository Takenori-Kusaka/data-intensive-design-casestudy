"""Start, wait for, and remove experiment containers.

Rules enforced here (see research/IMPLEMENTATION_RULES.md):
- every container / network / volume name must start with ``dic-``
- nothing that does not start with the given prefix is ever touched
- named volumes hold the DB data (no bind mounts on the Windows drive)

Runs inside WSL (the docker CLI is called directly).
"""
import json
import subprocess
import time

PREFIX = 'dic-'


class CmdError(RuntimeError):
    pass


def sh(args, check=True, timeout=180, input=None):
    r = subprocess.run(args, capture_output=True, text=True, encoding='utf-8', errors='replace',
                       timeout=timeout, input=input)
    if check and r.returncode != 0:
        raise CmdError(f"{' '.join(map(str, args))[:300]} -> rc={r.returncode}\n{r.stdout[-800:]}\n{r.stderr[-1200:]}")
    return r


def docker(*args, check=True, timeout=180, input=None):
    return sh(['docker', *args], check=check, timeout=timeout, input=input)


def _guard(name):
    if not name.startswith(PREFIX) or len(name) <= len(PREFIX):
        raise ValueError(f'refusing to touch {name!r}: name must start with {PREFIX!r}')
    return name


def network_create(name):
    _guard(name)
    if docker('network', 'inspect', name, check=False).returncode != 0:
        docker('network', 'create', name)


def run(name, image, *, network=None, aliases=(), ports=None, env=None, volumes=None,
        memory='512m', cmd=(), extra=()):
    """docker run -d. ``ports`` maps host port -> container port (bound to 127.0.0.1)."""
    _guard(name)
    a = ['run', '-d', '--name', name, '--memory', memory]
    if network:
        _guard(network)
        a += ['--network', network]
        for al in aliases:
            a += ['--network-alias', al]
    for hp, cp in (ports or {}).items():
        if not 25000 <= int(hp) <= 25999:
            raise ValueError('host ports must be within 25000-25999')
        a += ['-p', f'127.0.0.1:{hp}:{cp}']
    for k, v in (env or {}).items():
        a += ['-e', f'{k}={v}']
    for vol, path in (volumes or {}).items():
        _guard(vol)
        a += ['-v', f'{vol}:{path}']
    a += list(extra)
    a.append(image)
    a += list(cmd)
    return docker(*a).stdout.strip()


def logs_tail(name, n=40):
    _guard(name)
    r = docker('logs', '--tail', str(n), name, check=False)
    return (r.stdout + r.stderr)[-4000:]


def wait_ready(name, probe, timeout=120, interval=0.5):
    """Poll ``probe()`` (returns True when ready; exceptions count as not ready).
    On timeout, raise with the tail of the container log so a failed start is not
    mistaken for an experiment result."""
    t0 = time.monotonic()
    last = None
    while time.monotonic() - t0 < timeout:
        try:
            if probe():
                return time.monotonic() - t0
        except Exception as e:  # noqa: BLE001 - not ready yet
            last = e
        time.sleep(interval)
    raise CmdError(f'{name} not ready after {timeout}s (last: {last!r})\n--- logs ---\n{logs_tail(name)}')


def _ls(kind, prefix):
    fmt = '{{.Names}}' if kind == 'ps' else '{{.Name}}'
    cmd = {'ps': ['ps', '-a'], 'volume': ['volume', 'ls'], 'network': ['network', 'ls']}[kind]
    out = docker(*cmd, '--format', fmt).stdout.split()
    return sorted(n for n in out if n.startswith(prefix))


def leftovers(prefix):
    _guard(prefix)
    return {'containers': _ls('ps', prefix), 'volumes': _ls('volume', prefix), 'networks': _ls('network', prefix)}


def cleanup(prefix):
    """Remove containers, volumes and networks whose names start with ``prefix``.
    Use a topic prefix such as ``dic-d-`` so that other topics running at the same
    time are left alone."""
    _guard(prefix)
    for n in _ls('ps', prefix):
        docker('rm', '-f', '-v', n, check=False)
    for n in _ls('volume', prefix):
        docker('volume', 'rm', '-f', n, check=False)
    for n in _ls('network', prefix):
        docker('network', 'rm', n, check=False)
    return leftovers(prefix)


def rm(*names, volumes=()):
    for n in names:
        docker('rm', '-f', '-v', _guard(n), check=False)
    for v in volumes:
        docker('volume', 'rm', '-f', _guard(v), check=False)


def image_info(image):
    r = docker('image', 'inspect', image, check=False)
    if r.returncode != 0:
        return None
    d = json.loads(r.stdout)[0]
    return {'id': d.get('Id'), 'repo_digests': d.get('RepoDigests', []), 'size_mb': round(d.get('Size', 0) / 1e6)}


def ensure_image(image):
    if image_info(image) is None:
        docker('pull', '-q', image, timeout=1800)
    return image_info(image)


def mem_usage(prefix):
    """Memory used by running containers with the prefix (docker stats, one shot)."""
    _guard(prefix)
    r = docker('stats', '--no-stream', '--format', '{{.Name}} {{.MemUsage}}', check=False)
    out = {}
    for line in r.stdout.splitlines():
        p = line.split()
        if p and p[0].startswith(prefix):
            out[p[0]] = p[1]
    return out


def host_info():
    info = json.loads(docker('info', '--format', '{{json .}}').stdout)
    return {'docker_server': info.get('ServerVersion'), 'os': info.get('OperatingSystem'),
            'kernel': info.get('KernelVersion'), 'ncpu': info.get('NCPU'),
            'wsl_mem_total_gb': round(info.get('MemTotal', 0) / 2**30, 1), 'arch': info.get('Architecture')}
