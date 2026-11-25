// Read the guide's deck sources (deck/pNN.js) with a recording stand-in for lib.js and dump every slide's
// title, lead, evidence line, tables, cards and notes to catalog/raw/guide_slides.json. Reads only; writes
// nothing into the guide repository. No dependencies.
//   node catalog/scripts/extract_guide.js [path to a deck repository] [output file name under catalog/raw]
const fs = require('fs');
const path = require('path');
const GUIDE = path.resolve(process.argv[2] || path.join(__dirname, '..', '..', '..', 'data-intensive-design-guide'));
const DECK = path.join(GUIDE, 'deck');
const PARTS = ['p00', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10', 'p11', 'p12', 'p13', 'p13b', 'p14', 'p15', 'p16', 'p17'];

const txt = (c) => {
  if (c === null || c === undefined) return '';
  if (typeof c === 'string' || typeof c === 'number') return String(c);
  if (Array.isArray(c)) return c.map((x) => (Array.isArray(x) ? x.map(txt).join('') : txt(x) + (x && x.options && x.options.breakLine ? '\n' : ''))).join('');
  if (typeof c === 'object' && 'text' in c) return txt(c.text);
  return '';
};
const anyProxy = (name) => new Proxy(function () {}, { get: (t, k) => (k === Symbol.toPrimitive ? () => name : k === 'toString' ? () => name : anyProxy(name + '.' + String(k))), apply: () => anyProxy(name + '()') });

const slides = [];
let cur = null, part = null, section = null;
const newSlide = (kind, title) => {
  cur = { n: slides.length + 1, file: part, section, kind, title: title || '', lead: '', tag: '', stages: [], refs: {}, notes: '', tables: [], cards: [], boxes: [], bullets: [], texts: [] };
  slides.push(cur);
  const rec = cur;
  return new Proxy({}, {
    get: (t, k) => {
      if (k === 'addNotes') return (x) => { rec.notes = txt(x); };
      if (k === 'addText') return (x) => { const v = txt(x); if (v) rec.texts.push(v); };
      if (k === 'addTable') return (rows) => { rec.tables.push(rows.map((r) => r.map(txt))); };
      if (k === '_rec') return rec;
      return () => undefined;
    },
  });
};
const L = {
  C: anyProxy('C'), SH: anyProxy('SH'), HEX: anyProxy('HEX'), MUTED: '', BORDER: '', MONO: '',
  pres: new Proxy({}, { get: (t, k) => (k === 'addSlide' ? () => newSlide('raw', '') : k === 'addSection' ? () => undefined : anyProxy('pres.' + String(k))) }),
  section: (t) => { section = t; },
  content: (title, o = {}) => { const s = newSlide('content', title); Object.assign(s._rec, { lead: o.lead || '', tag: o.tag || '', stages: o.stages || [], refs: o.refs || {}, notes: o.notes || '', src: o.src || '' }); return s; },
  divider: (no, title, sub, qs, notes) => { const s = newSlide('divider', title); Object.assign(s._rec, { lead: sub || '', tag: no, bullets: [(qs || []).map(txt)], notes: notes || '' }); return s; },
  table: (s, rows) => { s._rec.tables.push(rows.map((r) => r.map(txt))); },
  card: (s, o) => { s._rec.cards.push({ title: txt(o.title), body: Array.isArray(o.body) ? o.body.map(txt) : [txt(o.body)] }); },
  note: (s, kind, text, o) => { s._rec.boxes.push({ kind, label: o && o.label !== undefined ? o.label : '', text: txt(text) }); },
  bullets: (s, items) => { s._rec.bullets.push(items.map(txt)); },
  T: (s, text) => { const v = txt(text); if (v) s._rec.texts.push(v); },
  runs: (items) => items.map((it) => ({ text: txt(it), options: { breakLine: true } })),
  flow: (s, steps) => { s._rec.texts.push(steps.map((st) => txt(st.t) + (st.d ? '：' + txt(st.d) : '')).join(' → ')); },
  box: (s, text) => { const v = txt(text); if (v) s._rec.texts.push(v); },
  stat: (s, o) => { s._rec.texts.push(txt(o.value) + ' ' + txt(o.label)); },
  refs: () => undefined, icon: () => undefined, crumb: () => undefined, arrow: () => undefined, bar: () => undefined, line: () => undefined,
  code: (s, code) => { s._rec.texts.push(txt(code)); },
};
const range = {};
for (const p of PARTS) {
  const f = path.join(DECK, p + '.js');
  if (!fs.existsSync(f)) continue;
  part = p;
  const a = slides.length + 1;
  delete require.cache[require.resolve(f)];
  require(f)(L);
  range[p] = [a, slides.length];
}
const out = path.join(__dirname, '..', 'raw', process.argv[3] || 'guide_slides.json');
fs.writeFileSync(out, JSON.stringify({ range, slides }, null, 1));
console.log(Object.entries(range).map(([p, r]) => p + '\t' + r[0] + '-' + r[1] + '\t' + (r[1] - r[0] + 1)).join('\n'));
console.log('total', slides.length);
