"""B-1 (one schema change, old and new writers and readers mixed) and B-2 (a chain
v1 -> v2 -> v3 and a re-read from the beginning) on the schema registry built into
Redpanda. One run = one fresh broker.

Configurations are compatibility modes of the registry:
  N  = NONE (no check), B = the registry default (set nothing), BE = BACKWARD_TRANSITIVE.
B-1 records all seven modes for the table of the guide.
"""
import time

from . import config as C, infra, schemas as S

MODES = ['NONE', 'BACKWARD', 'BACKWARD_TRANSITIVE', 'FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE']
B2_MODES = ['NONE', 'DEFAULT', 'BACKWARD_TRANSITIVE']
N_B1 = 200            # records per run (half by the old writer, half by the new one)
N_B2 = 100            # records per phase


def plan_b1():
    out = []
    for fmt in S.FORMATS:
        for _id, key, _ja, _intent in S.CHANGES:
            if S.change_specs(fmt, key) is not None:
                out.append({'exp': 'b1', 'format': fmt, 'change': key, 'seed': C.SEED0})
    return out


def plan_b2():
    return [{'exp': 'b2', 'format': fmt, 'sequence': seq, 'mode': mode, 'seed': C.SEED0}
            for fmt in S.FORMATS for seq, _ in S.SEQUENCES for mode in B2_MODES]


def run_id_of(s):
    if s['exp'] == 'b1':
        return f'b1:{s["format"]}:{s["change"]}'
    return f'b2:{s["format"]}:{s["sequence"]}:{s["mode"].lower()}'


def _register(subject, fmt, text):
    body = {'schema': text}
    if fmt != 'avro':
        body['schemaType'] = S.SR_TYPE[fmt]
    st, resp = infra.sr('POST', f'/subjects/{subject}/versions', body)
    out = {'http': st, 'accepted': st == 200}
    if st == 200:
        out['id'] = resp['id']
    else:
        out['error_code'] = resp.get('error_code') if isinstance(resp, dict) else None
        out['message'] = (resp.get('message') if isinstance(resp, dict) else str(resp))[:300]
    return out


def _set_mode(subject, mode):
    st, resp = infra.sr('PUT', f'/config/{subject}', {'compatibility': mode})
    if st != 200:
        raise RuntimeError(f'cannot set {mode} on {subject}: {st} {resp}')


def _read_all(fmt, topic, id2spec, readers, closed):
    """{(reader label, writer label): tally} over the whole topic."""
    res = {}
    for _p, _off, key, value in infra.scan_topic(topic):
        sid, payload = S.unframe(value)
        wlabel, wspec, truth = id2spec[sid][0], id2spec[sid][1], TRUTH[(topic, key)]
        for rlabel, rspec in readers:
            try:
                dec = S.decode(fmt, rspec, wspec, payload, closed)
            except Exception as e:  # noqa: BLE001 - the reader failed on this record
                dec = e
            res.setdefault((rlabel, wlabel), []).append(S.judge_read(fmt, rspec, wspec, truth, dec))
    return {f'{r}_reads_{w}': S.tally(v) for (r, w), v in res.items()}


TRUTH = {}


def run_b1(spec, log=print):
    t0 = time.monotonic()
    fmt, key, seed = spec['format'], spec['change'], spec['seed']
    v1, v2, closed = S.change_specs(fmt, key)
    t1, t2 = S.schema_text(fmt, v1, closed), S.schema_text(fmt, v2, closed)
    infra.down()
    infra.broker_up('redpanda', registry=True)
    default_mode = infra.sr('GET', '/config')[1]
    registry = {}
    for mode in MODES + ['DEFAULT']:
        subj = f'b1-{mode.lower()}-value'
        if mode != 'DEFAULT':
            _set_mode(subj, mode)
        r1 = _register(subj, fmt, t1)
        r2 = _register(subj, fmt, t2)
        if not r1['accepted']:
            raise RuntimeError(f'v1 was rejected under {mode}: {r1}')
        registry[mode] = {'v1': r1, 'v2': r2}
    ids = {registry['NONE']['v1']['id']: ('old', v1), registry['NONE']['v2']['id']: ('new', v2)}
    if len(ids) != 2:
        raise RuntimeError('v1 and v2 got the same schema id')
    # rolling update with no check: old and new writers interleaved on one topic
    topic = 'b1.events'
    infra.create_topic(topic)
    recs = []
    for i in range(N_B1):
        wspec, sid = (v1, registry['NONE']['v1']['id']) if i % 2 == 0 else (v2, registry['NONE']['v2']['id'])
        truth = S.gen_record(seed, i, wspec, fmt)
        TRUTH[(topic, str(i))] = truth
        recs.append((str(i), S.frame(sid, S.encode(fmt, wspec, truth))))
    infra.produce_all(topic, recs)
    traffic = _read_all(fmt, topic, ids, [('old', v1), ('new', v2)], closed)
    row = {'run_id': run_id_of(spec), **spec, 'intent': S.CHANGE_INTENT[key], 'n_records': N_B1,
           'schema_v1': t1, 'schema_v2': t2, 'registry_default': default_mode, 'registry': registry,
           'traffic': traffic, 'versions': {'redpanda': infra.broker_version('redpanda')},
           'container_memory': infra.mem()}
    rej = [m for m in MODES if not registry[m]['v2']['accepted']]
    row['brief'] = (f'rejected={",".join(rej) or "-"} old<-new={_b(traffic["old_reads_new"])} '
                    f'new<-old={_b(traffic["new_reads_old"])}')
    infra.down()
    row['seconds'] = round(time.monotonic() - t0, 1)
    return row


def _b(t):
    return f'e{t["error"]}/w{t["wrong"]}/m{t["missing"]}/ok{t["ok"]}'


def run_b2(spec, log=print):
    t0 = time.monotonic()
    fmt, seq, mode, seed = spec['format'], spec['sequence'], spec['mode'], spec['seed']
    specs = S.sequence_specs(fmt, seq)
    infra.down()
    infra.broker_up('redpanda', registry=True)
    subj, topic = 'b2.events-value', 'b2.events'
    if mode != 'DEFAULT':
        _set_mode(subj, mode)
    effective = infra.sr('GET', f'/config/{subj}?defaultToGlobal=true')[1]
    infra.create_topic(topic)
    steps, ids, deployed, phases = [], {}, None, []
    for k, sp in enumerate(specs, 1):
        r = _register(subj, fmt, S.schema_text(fmt, sp))
        steps.append({'version': f'v{k}', **r})
        if r['accepted']:
            deployed = (f'v{k}', sp, r['id'])
            ids[r['id']] = (f'v{k}', sp)
        if deployed is None:
            raise RuntimeError(f'v1 was rejected: {r}')
        # the writers of this phase run the newest version that the registry accepted
        recs = []
        for i in range((k - 1) * N_B2, k * N_B2):
            truth = S.gen_record(seed, i, deployed[1], fmt)
            TRUTH[(topic, str(i))] = truth
            recs.append((str(i), S.frame(deployed[2], S.encode(fmt, deployed[1], truth))))
        infra.produce_all(topic, recs)
        phases.append({'phase': k, 'writer': deployed[0], 'records': len(recs)})
    reader = deployed
    rebuild = _read_all(fmt, topic, ids, [(reader[0], reader[1])], False)      # from the beginning
    tot = {'n': 0, 'error': 0, 'wrong': 0, 'missing': 0, 'ok': 0}
    for v in rebuild.values():
        for kk in tot:
            tot[kk] += v[kk]
    row = {'run_id': run_id_of(spec), **spec, 'n_per_phase': N_B2, 'effective_mode': effective,
           'schemas': [S.schema_text(fmt, sp) for sp in specs], 'steps': steps, 'phases': phases,
           'reader': reader[0], 'rebuild_by_writer': rebuild, 'rebuild_total': tot,
           'tail_read': rebuild.get(f'{reader[0]}_reads_{phases[-1]["writer"]}'),
           'versions': {'redpanda': infra.broker_version('redpanda')}}
    row['brief'] = (f'mode={effective} steps={[int(x["accepted"]) for x in steps]} reader={reader[0]} '
                    f'rebuild={_b(tot)}')
    infra.down()
    row['seconds'] = round(time.monotonic() - t0, 1)
    return row


def run_one(spec, log=print):
    return run_b1(spec, log) if spec['exp'] == 'b1' else run_b2(spec, log)
