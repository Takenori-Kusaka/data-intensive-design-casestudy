"""C-6: where does an OpenTelemetry Collector still lose logs? (guide Part 13: "テレメトリは
失いうる前提" says topic C measures the cases that still lose data.)

  sender (this process) --OTLP/HTTP--> dic-c-otel (Collector) --OTLP/HTTP--> sink (this process)
  kafka designs:  dic-c-otel --> dic-c-broker (Redpanda) --> dic-c-otel2 (Collector) --> sink

  design   memory       sending queue in memory (as shipped)
           file         sending queue on disk (file_storage extension, fsync on)
           kafka        Redpanda in between; the consuming Collector as shipped
           kafka_after  the same, but the consuming Collector marks a message only after the
                        pipeline succeeded and has no queue of its own (back-pressure to Kafka)
  scenario normal        no fault
           outage        the sink answers 503 for a while, then recovers
           crash         during the outage the Collector next to the sink is killed and restarted
           overflow      outage with a small queue (queue_size 10 requests)
           retry_timeout outage longer than retry_on_failure.max_elapsed_time (3 s)
           retention     kafka: outage longer than the retention of the topic (2 s)
           broker_down   kafka: the broker is paused while the sender keeps sending, then resumed
           broker_down_crash  kafka: as above, and the first Collector is killed before the broker is back

The sender does not retry. Every log record has an id; the truth is the list of ids and the
HTTP status each request got (200 = accepted by the Collector). judge_delivery counts, from
what the sink finally holds: accepted but never delivered (lost), delivered twice, and
rejected records (the sender was told). Faults start and end at fixed request numbers.
"""
import http.server
import json
import os
import socket
import threading
import time
import urllib.error
import urllib.request

from ..common import dockerutil as D
from ..common.record import lat_summary
from . import config as C, prod

NET = 'dic-c-net'
OTEL, OTEL2, BROKER = 'dic-c-otel', 'dic-c-otel2', 'dic-c-broker'
PORT_AGENT, PORT_SINK = 25718, 25719
REQUESTS, PER_REQUEST = 100, 20
FAULT_FROM, FAULT_TO = 20, 60            # the fault holds while requests 21..60 are sent
TRIALS = 3
TOPIC = 'otlp_logs'
SCENARIOS = {
    'memory': ['normal', 'outage', 'crash', 'overflow', 'retry_timeout'],
    'file': ['normal', 'outage', 'crash', 'overflow', 'retry_timeout'],
    'kafka': ['normal', 'outage', 'crash', 'retention', 'broker_down', 'broker_down_crash'],
    'kafka_after': ['normal', 'outage', 'crash', 'retention'],
}
DESIGN_JA = {'memory': 'メモリのキュー（既定）', 'file': 'ディスクのキュー（file_storage）',
             'kafka': 'Kafka を間に置く（受け側の Collector は既定のまま）',
             'kafka_after': 'Kafka を間に置く（受け側は処理の後で印を付け、自前のキューなし）'}
SCENARIO_JA = {'normal': '障害なし（正常な操作）', 'outage': '下流が一時的に 503 を返す',
               'crash': '下流の停止中に Collector を強制終了して再起動', 'overflow': '下流の停止中にキューがあふれる（queue_size 10）',
               'retry_timeout': '下流の停止が再試行の上限（3 秒）より長い', 'retention': '下流の停止がトピックの保持（2 秒）より長い',
               'broker_down': 'ブローカーを一時停止し、戻す', 'broker_down_crash': 'ブローカーの停止中に送り側の Collector を強制終了'}


def plan():
    return [{'exp': 'c6', 'design': d, 'scenario': sc, 'trial': t, 'seed': C.SEED0}
            for d, scs in SCENARIOS.items() for sc in scs for t in range(1, TRIALS + 1)]


def run_id_of(s):
    return f'c6:{s["design"]}:{s["scenario"]}:t{s["trial"]}'


# ------------------------------------------------------------------------ sink
class Sink:
    """OTLP/HTTP (JSON) receiver. ``up`` False: every request gets 503."""

    def __init__(self):
        self.up = True
        self.arrivals = []                 # (monotonic time, id)
        self.requests = {'ok': 0, 'refused': 0}
        self._lock = threading.Lock()
        sink = self

        class H(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def do_POST(self):  # noqa: N802
                body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
                if not sink.up:
                    with sink._lock:
                        sink.requests['refused'] += 1
                    self._reply(503, b'{"code":14,"message":"sink is down"}')
                    return
                now = time.monotonic()
                ids = []
                try:
                    for rl in json.loads(body).get('resourceLogs', []):
                        for sl in rl.get('scopeLogs', []):
                            for lr in sl.get('logRecords', []):
                                ids.append(lr.get('body', {}).get('stringValue'))
                except ValueError:
                    self._reply(400, b'{}')
                    return
                with sink._lock:
                    sink.requests['ok'] += 1
                    sink.arrivals += [(now, i) for i in ids]
                self._reply(200, b'{}')

            def setup(self):
                super().setup()
                self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            def _reply(self, code, payload):
                # status line, headers and body in one write: two small writes would wait for the
                # peer's delayed ACK (about 40 ms per response) and that wait would look like a
                # property of the Collector
                reason = 'OK' if code == 200 else 'Unavailable'
                head = (f'HTTP/1.1 {code} {reason}\r\nContent-Type: application/json\r\n'
                        f'Content-Length: {len(payload)}\r\n\r\n').encode()
                self.wfile.write(head + payload)

            def log_message(self, *a):
                pass

        class Server(http.server.ThreadingHTTPServer):
            def handle_error(self, request, client_address):   # a killed Collector resets its connections
                pass
        self.httpd = Server(('0.0.0.0', PORT_SINK), H)
        self.httpd.daemon_threads = True
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def count(self):
        with self._lock:
            return len(self.arrivals)

    def snapshot(self):
        with self._lock:
            return list(self.arrivals)

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


# ---------------------------------------------------------------------- sender
def _payload(ids):
    return json.dumps({'resourceLogs': [{
        'resource': {'attributes': [{'key': 'service.name', 'value': {'stringValue': 'orders'}}]},
        'scopeLogs': [{'scope': {'name': 'dic-cs'}, 'logRecords': [
            {'timeUnixNano': str(1_790_812_800_000_000_000 + n), 'severityText': 'INFO', 'body': {'stringValue': i}}
            for n, i in enumerate(ids)]}]}]}).encode()


def post(ids, timeout=5):
    req = urllib.request.Request(f'http://127.0.0.1:{PORT_AGENT}/v1/logs', data=_payload(ids), method='POST',
                                 headers={'Content-Type': 'application/json', 'User-Agent': 'dic-cs'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            r.read()
            return r.status
    except urllib.error.HTTPError as e:
        e.read()
        return e.code
    except Exception:  # noqa: BLE001 - connection refused / timeout: the sender knows it failed
        return 0


def judge_delivery(sent, arrivals):
    """sent: {id: http status}. arrivals: list of ids the sink holds (probe ids excluded by the caller)."""
    acked = {i for i, st in sent.items() if st == 200}
    got = {}
    for i in arrivals:
        got[i] = got.get(i, 0) + 1
    return {'sent': len(sent), 'acked': len(acked), 'rejected': len(sent) - len(acked),
            'delivered': sum(1 for i in sent if i in got), 'lost_acked': sum(1 for i in acked if i not in got),
            'duplicates': sum(n - 1 for i, n in got.items() if i in sent),
            'delivered_though_rejected': sum(1 for i in sent if i not in acked and i in got),
            'unknown_ids': sum(1 for i in got if i not in sent)}


def audit_cases():
    sent = {f'a{i}': 200 for i in range(10)}
    sent.update({f'r{i}': 503 for i in range(4)})
    z = {'sent': 14, 'acked': 10, 'rejected': 4, 'lost_acked': 0, 'duplicates': 0, 'delivered_though_rejected': 0, 'unknown_ids': 0}
    full = [f'a{i}' for i in range(10)]
    return [
        ('delivery_clean', lambda: judge_delivery(sent, full), {**z, 'delivered': 10}),
        ('delivery_lost_3', lambda: judge_delivery(sent, full[3:]), {**z, 'delivered': 7, 'lost_acked': 3}),
        ('delivery_duplicates_2', lambda: judge_delivery(sent, full + ['a1', 'a1']), {**z, 'delivered': 10, 'duplicates': 2}),
        ('delivery_rejected_arrived_1_unknown_1', lambda: judge_delivery(sent, full + ['r0', 'zz']),
         {**z, 'delivered': 11, 'delivered_though_rejected': 1, 'unknown_ids': 1}),
        ('delivery_all_planted', lambda: judge_delivery(sent, full[2:] + ['a5', 'r1', 'x']),
         {**z, 'delivered': 9, 'lost_acked': 2, 'duplicates': 1, 'delivered_though_rejected': 1, 'unknown_ids': 1}),
    ]


# ------------------------------------------------------------------ containers
_EXPORT_SINK = """  otlphttp:
    endpoint: http://host.docker.internal:{sink}
    encoding: json
    compression: none
    timeout: 2s
    retry_on_failure:
      enabled: true
      initial_interval: 500ms
      max_interval: 1s
      max_elapsed_time: {max_elapsed}
    sending_queue:
{queue}
"""
_SERVICE = """service:
{ext}  pipelines:
    logs:
      receivers: [{rcv}]
      exporters: [{exp}]
  telemetry:
    logs:
      level: warn
    metrics:
      level: none
"""
_FILE_EXT = """extensions:
  file_storage:
    directory: /var/lib/otelcol/file_storage
    fsync: true
"""
_OTLP_RCV = """receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
"""


def _sink_exporter(design, scenario):
    max_elapsed = '3s' if scenario == 'retry_timeout' else '120s'
    q = ['      enabled: true']
    if scenario == 'overflow':
        q.append('      queue_size: 10')
    if design == 'file':
        q.append('      storage: file_storage')
    if design == 'kafka_after':
        q = ['      enabled: false']
    return _EXPORT_SINK.format(sink=PORT_SINK, max_elapsed=max_elapsed, queue='\n'.join(q))


def configs(design, scenario):
    """YAML of the Collector(s). Everything not written here is as shipped."""
    if design in ('memory', 'file'):
        ext = _FILE_EXT if design == 'file' else ''
        svc = _SERVICE.format(ext='  extensions: [file_storage]\n' if design == 'file' else '', rcv='otlp', exp='otlphttp')
        return {OTEL: _OTLP_RCV + ext + 'exporters:\n' + _sink_exporter(design, scenario) + svc}
    agent = (_OTLP_RCV + f"""exporters:
  kafka:
    brokers: ["{BROKER}:9092"]
    logs:
      topic: {TOPIC}
    timeout: 2s
    retry_on_failure:
      enabled: true
      initial_interval: 500ms
      max_interval: 1s
      max_elapsed_time: 120s
""" + _SERVICE.format(ext='', rcv='otlp', exp='kafka'))
    after = """    message_marking:
      after: true
      on_error: false
    error_backoff:
      enabled: true
      initial_interval: 500ms
      max_interval: 1s
      max_elapsed_time: 0s
""" if design == 'kafka_after' else ''
    consumer = (f"""receivers:
  kafka:
    brokers: ["{BROKER}:9092"]
    logs:
      topics: ["{TOPIC}"]
    group_id: dic-c-otel2
    initial_offset: earliest
{after}exporters:
""" + _sink_exporter(design, scenario) + _SERVICE.format(ext='', rcv='kafka', exp='otlphttp'))
    return {OTEL: agent, OTEL2: consumer}


def _collector_up(name, yaml_text, port=None):
    d = os.path.join(C.DATA, '_otel')
    os.makedirs(d, exist_ok=True)
    os.chmod(d, 0o755)
    path = os.path.join(d, f'{name}.yaml')
    with open(path, 'w', encoding='utf-8', newline='\n') as f:
        f.write(yaml_text)
    os.chmod(path, 0o644)
    D.run(name, C.IMAGES['otelcol'], network=NET, memory='256m', ports={port: 4318} if port else None,
          volumes={f'{name}-q': '/var/lib/otelcol/file_storage'},
          extra=['--user', '0:0', '--add-host', 'host.docker.internal:host-gateway',
                 '-v', f'{path}:/etc/otelcol-contrib/config.yaml:ro'])


def _rpk(*args, check=True):
    return D.docker('exec', BROKER, 'rpk', *args, check=check)


def broker_up(retention):
    D.run(BROKER, C.IMAGES['redpanda'], network=NET, memory='1200m',
          volumes={'dic-c-broker-data': '/var/lib/redpanda/data'},
          cmd=['redpanda', 'start', '--smp', '1', '--memory', '700M', '--reserve-memory', '0M', '--overprovisioned',
               '--node-id', '0', '--check=false', '--unsafe-bypass-fsync=false', '--set', 'redpanda.developer_mode=false',
               '--kafka-addr', 'internal://0.0.0.0:9092', '--advertise-kafka-addr', f'internal://{BROKER}:9092',
               '--rpc-addr', '0.0.0.0:33145', '--advertise-rpc-addr', f'{BROKER}:33145'])
    D.wait_ready(BROKER, lambda: _rpk('cluster', 'info', check=False).returncode == 0, timeout=120, interval=1.0)
    for k, v in (('write_caching_default', 'false'), ('auto_create_topics_enabled', 'false'),
                 ('log_segment_ms_min', '1000'), ('log_compaction_interval_ms', '1000')):
        D.wait_ready(BROKER, lambda: _rpk('cluster', 'config', 'set', k, v, check=False).returncode == 0, timeout=60, interval=1.0)
    cfg = ['-c', 'retention.ms=2000', '-c', 'segment.ms=1000'] if retention else []
    D.wait_ready(BROKER, lambda: _rpk('topic', 'create', TOPIC, '-p', '1', '-r', '1', *cfg, check=False).returncode == 0
                 or 'TOPIC_ALREADY_EXISTS' in _rpk('topic', 'create', TOPIC, check=False).stdout, timeout=60, interval=1.0)
    v = _rpk('version', check=False).stdout.split()
    return next((x for x in v if x[:1] == 'v' and x[1:2].isdigit()), '')


def _wait_pipeline(sink, tag, timeout=90):
    """Send probe records until one arrives at the sink (the whole path is up)."""
    t_end = time.monotonic() + timeout
    n = 0
    while time.monotonic() < t_end:
        n += 1
        post([f'probe-{tag}-{n}'], timeout=3)
        time.sleep(0.5)
        if any(i.startswith(f'probe-{tag}-') for _t, i in sink.snapshot()):
            return n
    raise D.CmdError(f'pipeline not ready ({tag})\n--- {OTEL} ---\n{D.logs_tail(OTEL, 15)}')


def _wait_agent(timeout=60):
    t_end = time.monotonic() + timeout
    while time.monotonic() < t_end:
        if post(['probe-agent']) == 200:
            return True
        time.sleep(0.3)
    return False


def _drain(sink, expected, quiet=5.0, limit=60.0):
    """Wait until everything arrived, or nothing new arrived for ``quiet`` seconds."""
    t0 = time.monotonic()
    last, last_t = sink.count(), t0
    while time.monotonic() - t0 < limit:
        time.sleep(0.25)
        n = sink.count()
        if n != last:
            last, last_t = n, time.monotonic()
        real = sum(1 for _t, i in sink.snapshot() if not i.startswith(('probe-', 'filler-')))
        if real >= expected or time.monotonic() - last_t > quiet:
            break
    return round(time.monotonic() - t0, 1)


def run_one(s, log=print):
    t0 = time.monotonic()
    prod.down()
    design, sc = s['design'], s['scenario']
    kafka = design.startswith('kafka')
    sink = Sink()
    ver, events = {}, []
    sent, send_t = {}, {}
    try:
        D.network_create(NET)
        if kafka:
            ver['redpanda'] = broker_up(retention=(sc == 'retention'))
        cfg = configs(design, sc)
        _collector_up(OTEL, cfg[OTEL], PORT_AGENT)
        if kafka:
            _collector_up(OTEL2, cfg[OTEL2])
        ver['otelcol'] = C.IMAGES['otelcol'].split(':')[1]
        _wait_pipeline(sink, 'start')
        near_sink = OTEL2 if kafka else OTEL          # the Collector that talks to the sink

        def send(a, b):
            for r in range(a + 1, b + 1):
                ids = [f'r{r:03d}-{k:02d}' for k in range(PER_REQUEST)]
                send_t[r] = time.monotonic()
                st = post(ids)
                for i in ids:
                    sent[i] = st

        def mark(what):
            events.append({'at_request': max(send_t) if send_t else 0, 'event': what,
                           't': round(time.monotonic() - t0, 2)})
        send(0, FAULT_FROM)
        if sc in ('broker_down', 'broker_down_crash'):
            D.docker('pause', BROKER)
            mark('broker paused')
        elif sc != 'normal':
            sink.up = False
            mark('sink down')
        send(FAULT_FROM, FAULT_TO)
        if sc == 'outage':
            time.sleep(4)
        elif sc == 'overflow':
            time.sleep(2)
        elif sc == 'retry_timeout':
            time.sleep(8)
        elif sc == 'retention':
            for k in range(12):            # keep the segment rolling so that retention can remove it
                post([f'filler-{k}'])
                time.sleep(1)
        elif sc == 'crash':
            time.sleep(1)
            D.docker('kill', near_sink)
            mark(f'{near_sink} killed')
            D.docker('start', near_sink)
            mark(f'{near_sink} started')
            if not kafka and not _wait_agent():
                raise D.CmdError(f'{OTEL} did not come back\n{D.logs_tail(OTEL, 15)}')
        elif sc == 'broker_down':
            time.sleep(4)
        elif sc == 'broker_down_crash':
            time.sleep(1)
            D.docker('kill', OTEL)
            mark(f'{OTEL} killed')
            D.docker('start', OTEL)
            mark(f'{OTEL} started')
        if sc in ('broker_down', 'broker_down_crash'):
            D.docker('unpause', BROKER)
            mark('broker resumed')
            if sc == 'broker_down_crash' and not _wait_agent():
                raise D.CmdError(f'{OTEL} did not come back\n{D.logs_tail(OTEL, 15)}')
        elif sc != 'normal':
            sink.up = True
            mark('sink up')
        send(FAULT_TO, REQUESTS)
        # a killed Kafka consumer is only replaced in its group after the session timeout (10 s as shipped)
        drain_s = _drain(sink, len(sent), quiet=30.0 if kafka else 6.0, limit=120.0)
        arr = [(t, i) for t, i in sink.snapshot() if not i.startswith(('probe-', 'filler-'))]
        j = judge_delivery(sent, [i for _t, i in arr])
        first = {}
        for t, i in arr:
            first.setdefault(int(i[1:4]), t)
        delay = lat_summary([(first[r] - send_t[r]) * 1000 for r in first if r in send_t]) if sc == 'normal' else None
        by_status = {}
        for st in sent.values():
            by_status[str(st)] = by_status.get(str(st), 0) + 1
        lost_req = sorted({int(i[1:4]) for i, st in sent.items() if st == 200} - set(first))
        logs = {n: (D.docker('logs', n, check=False).stdout + D.docker('logs', n, check=False).stderr)
                for n in ([OTEL, OTEL2] if kafka else [OTEL])}
        dropped = {n: sum(1 for ln in t.splitlines() if 'ropping data' in ln or 'ropped' in ln) for n, t in logs.items()}
        cmem = prod.mem()
    finally:
        sink.close()
        D.docker('unpause', BROKER, check=False)
        prod.down()
    return {'run_id': run_id_of(s), **s, 'requests': REQUESTS, 'records_per_request': PER_REQUEST,
            'fault_requests': [FAULT_FROM + 1, FAULT_TO], 'versions': ver, 'events': events, 'judge': j,
            'status_counts': by_status, 'lost_requests_first_last': [lost_req[0], lost_req[-1]] if lost_req else [],
            'lost_requests': len(lost_req), 'sink_requests': dict(sink.requests), 'delay_normal_ms': delay,
            'collector_log_lines_dropped': dropped, 'drain_seconds': drain_s, 'container_memory': cmem,
            'seconds': round(time.monotonic() - t0, 1),
            'brief': f'acked={j["acked"]} rejected={j["rejected"]} lost_acked={j["lost_acked"]} dup={j["duplicates"]} '
                     f'status={by_status} lost_req={[lost_req[0], lost_req[-1]] if lost_req else []}'}
