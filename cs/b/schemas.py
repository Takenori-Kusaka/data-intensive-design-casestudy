"""Event schemas of B-1 / B-2 in three formats (Avro, Protocol Buffers, JSON Schema),
generated from one compact description, with writers, readers, the record generator
and the judge.

A schema version is a list of fields ``F(name, type, sem, ...)``. ``sem`` is the meaning
of the field (what the generator calls it); a rename keeps ``sem`` and changes ``name``,
a reused Protocol Buffers number keeps ``num`` and changes ``sem``.

Framing on the wire: one zero byte, the schema id of the registry as 4 bytes, then the
payload (the same layout for all three formats; the Confluent serializers are not used).
"""
import io
import json
import random
import struct
from dataclasses import dataclass, replace

import fastavro
import jsonschema
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

NODEF = object()          # Avro: the field has no default
FORMATS = ('avro', 'protobuf', 'json')
SR_TYPE = {'avro': 'AVRO', 'protobuf': 'PROTOBUF', 'json': 'JSON'}


@dataclass(frozen=True)
class F:
    name: str
    type: str                 # int | long | string | sint64 (protobuf only) | number (json only)
    sem: str
    num: int = 0              # protobuf field number
    default: object = NODEF   # avro default
    required: bool = True     # json: listed in "required"
    aliases: tuple = ()       # avro aliases


BASE = [F('order_id', 'int', 'order_id', 1), F('status', 'string', 'status', 2), F('amount', 'long', 'amount', 3),
        F('qty', 'int', 'qty', 4), F('note', 'string', 'note', 5, default='', required=False)]
COUPON_OPT = F('coupon', 'string', 'coupon', 6, default='', required=False)
COUPON_REQ = F('coupon', 'string', 'coupon', 6)
EMAIL_OPT = F('email', 'string', 'email', 7, default='', required=False)


def _chg(spec, field, **kw):
    return [replace(f, **kw) if f.name == field else f for f in spec]


def _drop(spec, name):
    return [f for f in spec if f.name != name]


# ---- B-1: one change, v1 -> v2 -------------------------------------------------
# id, key, label, declared intent ('normal' = the guide calls it safe in both directions)
CHANGES = [
    ('c01', 'add_optional', '任意フィールドの追加（既定値つき）', 'normal'),
    ('c02', 'add_required', '必須フィールドの追加（既定値なし）', 'breaking'),
    ('c03', 'del_optional', '任意フィールドの削除（既定値つき）', 'normal'),
    ('c04', 'del_required', '必須フィールドの削除（既定値なし）', 'breaking'),
    ('c05', 'rename', '名前の変更（Avro は別名つき）', 'breaking'),
    ('c06', 'rename_noalias', '名前の変更（別名なし）', 'breaking'),
    ('c07', 'widen', '型を広げる（int → long）', 'breaking'),
    ('c08', 'narrow', '型を狭める（long → int）', 'breaking'),
    ('c09', 'retype', '型の変更（整数 → 文字列）', 'breaking'),
    ('c10', 'reuse_number', '番号の再利用（消した番号に別の意味のフィールド）', 'breaking'),
    ('c11', 'closed_add', '任意プロパティの追加（additionalProperties: false のスキーマ）', 'normal'),
]
CHANGE_JA = {k: f'{i} {ja}' for i, k, ja, _ in CHANGES}
CHANGE_INTENT = {k: it for _i, k, _ja, it in CHANGES}


def change_specs(fmt, key):
    """(v1, v2, json_closed) or None when the change does not exist in the format."""
    b = BASE
    if fmt == 'json':
        b = _chg(b, 'amount', type='number')          # JSON Schema: integer is the narrow type
    if key == 'add_optional':
        return b, b + [COUPON_OPT], False
    if key == 'add_required':
        return None if fmt == 'protobuf' else (b, b + [COUPON_REQ], False)      # proto3 has no required
    if key == 'del_optional':
        return b, _drop(b, 'note'), False
    if key == 'del_required':
        return b, _drop(b, 'status'), False
    if key == 'rename':
        return b, _chg(b, 'status', name='state', aliases=('status',) if fmt == 'avro' else ()), False
    if key == 'rename_noalias':
        return (b, _chg(b, 'status', name='state'), False) if fmt == 'avro' else None
    if key == 'widen':
        return b, _chg(b, 'qty', type='number' if fmt == 'json' else 'long'), False
    if key == 'narrow':
        return b, _chg(b, 'amount', type='int'), False
    if key == 'retype':
        return b, _chg(b, 'order_id', type='string'), False
    if key == 'reuse_number':
        return (b, _drop(b, 'note') + [F('email', 'string', 'email', 5)], False) if fmt == 'protobuf' else None
    if key == 'closed_add':
        return (b, b + [COUPON_OPT], True) if fmt == 'json' else None
    raise KeyError(key)


# ---- B-2: sequences v1 -> v2 -> v3 ---------------------------------------------
SEQUENCES = [('hole', '段ごとには通るが、v3 が v1 を読めない列'), ('benign', '任意フィールドを 2 回足す列（正常な変更）')]
SEQ_JA = dict(SEQUENCES)


def sequence_specs(fmt, key):
    b = BASE if fmt != 'json' else _chg(BASE, 'amount', type='number')
    if key == 'benign':
        return [b, b + [COUPON_OPT], b + [COUPON_OPT, EMAIL_OPT]]
    if fmt == 'protobuf':   # delete a number, then give the number to a field of another type
        return [b, _drop(b, 'amount'), _drop(b, 'amount') + [F('tip', 'sint64', 'tip', 3, default=0, required=False)]]
    if fmt == 'json':       # delete a property, then add the name again with another type
        return [b, _drop(b, 'amount'), _drop(b, 'amount') + [F('amount', 'string', 'amount_label', required=False)]]
    # Avro: add with a default, then drop the default
    return [b, b + [COUPON_OPT], b + [COUPON_REQ]]


SEQ_STEP_JA = {
    ('avro', 'hole'): 'v2 で coupon を既定値つきで足し、v3 で既定値を外す',
    ('json', 'hole'): 'v2 で amount（number）を消し、v3 で同じ名前 amount を string の任意プロパティとして足す',
    ('protobuf', 'hole'): 'v2 で amount（int64、番号 3）を消し、v3 で tip（sint64）に番号 3 を割り当てる',
}


# ---- schema text ---------------------------------------------------------------
def avro_schema(spec):
    fields = []
    for f in spec:
        d = {'name': f.name, 'type': f.type}
        if f.default is not NODEF:
            d['default'] = f.default
        if f.aliases:
            d['aliases'] = list(f.aliases)
        fields.append(d)
    return {'type': 'record', 'name': 'OrderEvent', 'namespace': 'dic', 'fields': fields}


_PB_TEXT = {'int': 'int32', 'long': 'int64', 'string': 'string', 'sint64': 'sint64'}
_PB_TYPE = {'int': descriptor_pb2.FieldDescriptorProto.TYPE_INT32, 'long': descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
            'string': descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
            'sint64': descriptor_pb2.FieldDescriptorProto.TYPE_SINT64}


def proto_text(spec):
    lines = ['syntax = "proto3";', 'package dic;', '', 'message OrderEvent {']
    lines += [f'  {_PB_TEXT[f.type]} {f.name} = {f.num};' for f in sorted(spec, key=lambda f: f.num)]
    return '\n'.join(lines + ['}', ''])


_pb_cache = {}


def proto_class(spec):
    key = tuple((f.name, f.type, f.num) for f in spec)
    if key not in _pb_cache:
        fdp = descriptor_pb2.FileDescriptorProto(name='order_event.proto', package='dic', syntax='proto3')
        m = fdp.message_type.add(name='OrderEvent')
        for f in spec:
            m.field.add(name=f.name, number=f.num, type=_PB_TYPE[f.type],
                        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL)
        pool = descriptor_pool.DescriptorPool()
        pool.Add(fdp)
        _pb_cache[key] = message_factory.GetMessageClass(pool.FindMessageTypeByName('dic.OrderEvent'))
    return _pb_cache[key]


_JS_TYPE = {'int': 'integer', 'long': 'integer', 'string': 'string', 'number': 'number'}


def json_schema(spec, closed=False):
    s = {'$schema': 'http://json-schema.org/draft-07/schema#', 'type': 'object',
         'properties': {f.name: {'type': _JS_TYPE[f.type]} for f in spec},
         'required': [f.name for f in spec if f.required]}
    if closed:
        s['additionalProperties'] = False
    return s


def schema_text(fmt, spec, closed=False):
    if fmt == 'avro':
        return json.dumps(avro_schema(spec))
    if fmt == 'protobuf':
        return proto_text(spec)
    return json.dumps(json_schema(spec, closed))


# ---- generator -----------------------------------------------------------------
STATUSES = ['accepted', 'cooking', 'delivering', 'completed', 'cancelled']
INT32_MAX = 2**31 - 1


def gen_record(seed, i, spec, fmt):
    """The values writer ``spec`` puts on the wire for record ``i``: {sem: value}.
    One record in five carries a large amount (beyond 32 bits; a fraction for a JSON number)."""
    rng = random.Random(f'{seed}:{i}')
    big = i % 5 == 3
    sem = {'order_id': 1000 + i, 'status': rng.choice(STATUSES),
           'amount': (INT32_MAX + 1 + rng.randrange(1, 10**6)) if big else rng.randrange(100, 100000),
           'qty': rng.randrange(1, 10), 'note': f'n{i}', 'coupon': f'C{i}', 'email': f'u{i}@example.test',
           'tip': rng.randrange(1, 500), 'amount_label': f'A{i}'}
    out = {}
    for f in spec:
        v = sem[f.sem]
        if f.type == 'string':
            v = str(v)
        elif f.type == 'int' and v > INT32_MAX:
            v = v % 100000 + 1
        elif f.type == 'number' and big and fmt == 'json':
            v = rng.randrange(100, 100000) + 0.5
        out[f.sem] = v
    return out


# ---- writers / readers ---------------------------------------------------------
def frame(schema_id, payload):
    return b'\x00' + struct.pack('>I', schema_id) + payload


def unframe(buf):
    if buf[:1] != b'\x00':
        raise ValueError('bad magic byte')
    return struct.unpack('>I', buf[1:5])[0], buf[5:]


def encode(fmt, spec, values):
    """values: {sem: value} as produced by gen_record for this spec."""
    by_name = {f.name: values[f.sem] for f in spec}
    if fmt == 'avro':
        b = io.BytesIO()
        fastavro.schemaless_writer(b, fastavro.parse_schema(avro_schema(spec)), by_name)
        return b.getvalue()
    if fmt == 'protobuf':
        return proto_class(spec)(**by_name).SerializeToString()
    return json.dumps(by_name).encode()


def decode(fmt, reader, writer, payload, closed=False):
    """What a reader built for schema ``reader`` sees: {field name: value}. Raises when
    the reader cannot read the record."""
    if fmt == 'avro':
        return fastavro.schemaless_reader(io.BytesIO(payload), fastavro.parse_schema(avro_schema(writer)),
                                          fastavro.parse_schema(avro_schema(reader)))
    if fmt == 'protobuf':
        m = proto_class(reader).FromString(payload)
        return {f.name: getattr(m, f.name) for f in reader}
    obj = json.loads(payload)
    jsonschema.Draft7Validator(json_schema(reader, closed)).validate(obj)
    return {f.name: obj.get(f.name) for f in reader}


def reader_default(fmt, f):
    if fmt == 'avro':
        return f.default
    if fmt == 'protobuf':
        return '' if f.type == 'string' else 0
    return None


# ---- judge ---------------------------------------------------------------------
def judge_read(fmt, reader, writer, truth, decoded):
    """Classify one read. truth = {sem: value written}; decoded = {name: value} or an
    Exception. Returns 'error' | 'wrong' | 'missing' | 'ok'.

    wrong: a field of the reader holds something else than what the writer wrote for the
    same meaning, or holds a value although the writer wrote nothing with that meaning.
    missing: no error and no wrong value, but a field that the reader's own schema
    declares as required (no default) was silently filled with the type default because
    the writer did not write it. A default in an optional field is 'ok'."""
    if isinstance(decoded, Exception):
        return 'error'
    carried = {f.sem for f in writer}
    missing = False
    for f in reader:
        want = truth[f.sem] if f.sem in carried else reader_default(fmt, f)
        got = decoded.get(f.name)
        if type(got) is not type(want) and not (isinstance(got, (int, float)) and isinstance(want, (int, float))
                                               and not isinstance(got, bool)):
            return 'wrong'
        if got != want:
            return 'wrong'
        if f.sem not in carried and f.required and f.default is NODEF:
            missing = True
    return 'missing' if missing else 'ok'


def tally(results):
    out = {'n': len(results), 'error': 0, 'wrong': 0, 'missing': 0, 'ok': 0}
    for r in results:
        out[r] += 1
    return out
