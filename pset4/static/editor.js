/* DoomDraft browser client — OT + SSE (ops + cursors) + multi-doc */

(function () {
  'use strict';

  /** @typedef {{type:'I',pos:number,text:string}|{type:'D',pos:number,len:number}} Op */

  function transform(a, b) {
    if (a.type === 'I' && b.type === 'I') {
      if (b.pos <= a.pos) return { type: 'I', pos: a.pos + b.text.length, text: a.text };
      return a;
    }
    if (a.type === 'I' && b.type === 'D') {
      if (a.pos <= b.pos) return a;
      if (a.pos >= b.pos + b.len) return { type: 'I', pos: a.pos - b.len, text: a.text };
      return { type: 'I', pos: 0, text: '' };
    }
    if (a.type === 'D' && b.type === 'I') {
      if (b.pos >= a.pos + a.len) return a;
      if (b.pos <= a.pos) return { type: 'D', pos: a.pos + b.text.length, len: a.len };
      return { type: 'D', pos: a.pos, len: a.len + b.text.length };
    }
    /* D vs D */
    const pa = a.pos, la = a.len, pb = b.pos, lb = b.len;
    if (pa + la <= pb) return a;
    if (pa >= pb + lb) return { type: 'D', pos: pa - lb, len: la };
    if (pa >= pb && pa + la <= pb + lb) return { type: 'D', pos: 0, len: 0 };
    if (pa <= pb && pa + la >= pb + lb) return { type: 'D', pos: pa, len: la - lb };
    if (pa < pb) return { type: 'D', pos: pa, len: pb - pa };
    return { type: 'D', pos: pb, len: pa + la - pb - lb };
  }

  function applyOp(text, op) {
    if (op.type === 'I') {
      if (!op.text) return text;
      const pos = Math.min(op.pos, text.length);
      return text.slice(0, pos) + op.text + text.slice(pos);
    }
    if (!op.len) return text;
    const pos = Math.min(op.pos, text.length);
    const len = Math.min(op.len, text.length - pos);
    return text.slice(0, pos) + text.slice(pos + len);
  }

  function computeDelta(oldText, newText) {
    if (oldText === newText) return [];
    let i = 0;
    const n = Math.min(oldText.length, newText.length);
    while (i < n && oldText[i] === newText[i]) i++;
    let j = 0;
    while (
      j < oldText.length - i &&
      j < newText.length - i &&
      oldText[oldText.length - 1 - j] === newText[newText.length - 1 - j]
    ) {
      j++;
    }
    const oldMid = oldText.slice(i, oldText.length - j);
    const newMid = newText.slice(i, newText.length - j);
    /** @type {Op[]} */
    const out = [];
    if (oldMid.length) out.push({ type: 'D', pos: i, len: oldMid.length });
    if (newMid.length) out.push({ type: 'I', pos: i, text: newMid });
    return out;
  }

  function adjustCursor(cursor, op) {
    if (op.type === 'I') {
      if (op.pos < cursor) return cursor + op.text.length;
      return cursor;
    }
    const end = op.pos + op.len;
    if (cursor <= op.pos) return cursor;
    if (cursor < end) return op.pos;
    return cursor - op.len;
  }

  function opToWire(op, seq, clientId) {
    if (op.type === 'I') {
      return { type: 'I', pos: op.pos, text: op.text, client_id: clientId, seq };
    }
    return { type: 'D', pos: op.pos, len: op.len, client_id: clientId, seq };
  }

  function wireToOp(o) {
    if (o.type === 'I') return { type: 'I', pos: o.pos, text: o.text || '' };
    return { type: 'D', pos: o.pos, len: o.len };
  }

  const clientId = 'c_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 9);

  const el = {
    server: () => document.getElementById('serverUrl'),
    doc: () => document.getElementById('docId'),
    docSelect: () => document.getElementById('docSelect'),
    btnNew: () => document.getElementById('btnNewDoc'),
    btnConnect: () => document.getElementById('btnConnect'),
    failR: () => document.getElementById('failReplica'),
    btnFail: () => document.getElementById('btnFail'),
    status: () => document.getElementById('status'),
    ver: () => document.getElementById('ver'),
    ta: () => document.getElementById('editor'),
    overlay: () => document.getElementById('cursor-overlay'),
  };

  const s = {
    serverText: '',
    pending: /** @type {{seq:number, op:Op}[]} */ ([]),
    mySeqs: new Set(),
    inflightSeq: /** @type {number|null} */ (null),
    nextSeq: 0,
    serverVersion: 0,
    es: /** @type {EventSource|null} */ (null),
    cursorTimer: /** @type {number|null} */ (null),
    peerCursors: /** @type {Map<string, {pos:number, color:string}>} */ (new Map()),
  };

  const COLORS = ['#f38ba8', '#fab387', '#f9e2af', '#a6e3a1', '#89dceb', '#cba6f7', '#eba0ac'];

  function setStatus(t, cls) {
    const st = el.status();
    st.textContent = t;
    st.className = cls || '';
  }

  function baseUrl() {
    return el.server().value.replace(/\/$/, '');
  }

  function fullText() {
    let t = s.serverText;
    for (const p of s.pending) t = applyOp(t, p.op);
    return t;
  }

  function syncTextareaFromModel() {
    const want = fullText();
    const ta = el.ta();
    if (ta.value !== want) {
      const sel = ta.selectionStart;
      const delta = want.length - ta.value.length;
      ta.value = want;
      try {
        ta.setSelectionRange(Math.max(0, sel + delta), Math.max(0, sel + delta));
      } catch (_) {}
    }
  }

  async function refreshDocList() {
    const sel = el.docSelect();
    const r = await fetch(baseUrl() + '/docs');
    if (!r.ok) return;
    const list = await r.json();
    sel.innerHTML = '';
    for (const id of list) {
      const o = document.createElement('option');
      o.value = id;
      o.textContent = id;
      sel.appendChild(o);
    }
    sel.value = el.doc().value;
  }

  async function sendPendingFront() {
    if (s.inflightSeq !== null || !s.pending.length) return;
    const { seq, op } = s.pending[0];
    s.inflightSeq = seq;
    s.mySeqs.add(seq);
    const docId = el.doc().value;
    try {
      const r = await fetch(baseUrl() + '/doc/' + encodeURIComponent(docId) + '/op', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(opToWire(op, seq, clientId)),
      });
      if (!r.ok) {
        const t = await r.text();
        throw new Error(t || r.statusText);
      }
    } catch (e) {
      console.error(e);
      setStatus('send failed: ' + e, 'err');
      s.mySeqs.delete(seq);
      s.inflightSeq = null;
    }
  }

  function handleSSEOp(data) {
    // GET /doc/<id> already returns full text at serverVersion. The stream replays
    // all committed ops from version 0 for new connections, so without this
    // guard we would apply every historical op again (reload / second tab breaks).
    if (data.version <= s.serverVersion) {
      return;
    }
    const ta = el.ta();
    if (s.pending.length && data.seq === s.pending[0].seq && s.mySeqs.has(data.seq)) {
      const committed = wireToOp(data.op);
      s.serverText = applyOp(s.serverText, committed);
      s.pending.shift();
      s.mySeqs.delete(data.seq);
      s.inflightSeq = null;
      s.serverVersion = data.version;
      el.ver().textContent = 'v ' + s.serverVersion;
      syncTextareaFromModel();
      sendPendingFront();
      return;
    }
    const foreign = wireToOp(data.op);
    for (let i = 0; i < s.pending.length; i++) {
      s.pending[i].op = transform(s.pending[i].op, foreign);
    }
    s.serverText = applyOp(s.serverText, foreign);
    s.serverVersion = data.version;
    el.ver().textContent = 'v ' + s.serverVersion;
    let c = ta.selectionStart;
    c = adjustCursor(c, foreign);
    syncTextareaFromModel();
    try {
      ta.setSelectionRange(c, c);
    } catch (_) {}
  }

  function hashColor(id) {
    let h = 0;
    for (let i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) >>> 0;
    return COLORS[h % COLORS.length];
  }

  function renderCursorOverlay() {
    const ov = el.overlay();
    const ta = el.ta();
    ov.innerHTML = '';
    const style = window.getComputedStyle(ta);
    const lineHeight = parseFloat(style.lineHeight) || 20;
    for (const [pid, cur] of s.peerCursors) {
      if (pid === clientId) continue;
      const pos = Math.max(0, Math.min(cur.pos, ta.value.length));
      const line = ta.value.slice(0, pos).split('\n').length - 1;
      const colStart = ta.value.lastIndexOf('\n', pos - 1) + 1;
      const col = pos - colStart;
      const top = line * lineHeight;
      const leftCh = col;
      const bar = document.createElement('div');
      bar.className = 'caret-marker';
      bar.style.top = top + 'px';
      bar.style.left = leftCh + 'ch';
      bar.style.height = lineHeight + 'px';
      const lab = document.createElement('div');
      lab.className = 'caret-label';
      lab.style.top = top + 'px';
      lab.style.left = leftCh + 'ch';
      lab.style.background = cur.color;
      lab.textContent = pid.slice(0, 12);
      ov.appendChild(bar);
      ov.appendChild(lab);
    }
  }

  function handleSSECursor(data) {
    if (data.client_id === clientId) return;
    s.peerCursors.set(data.client_id, { pos: data.pos, color: hashColor(data.client_id) });
    renderCursorOverlay();
  }

  function scheduleCursorPost() {
    if (s.cursorTimer) clearTimeout(s.cursorTimer);
    s.cursorTimer = window.setTimeout(postCursor, 80);
  }

  async function postCursor() {
    if (!s.es) return;
    const ta = el.ta();
    const pos = ta.selectionStart ?? 0;
    const docId = el.doc().value;
    try {
      await fetch(baseUrl() + '/doc/' + encodeURIComponent(docId) + '/cursor', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ client_id: clientId, pos }),
      });
    } catch (_) {}
  }

  function disconnect() {
    if (s.es) {
      s.es.close();
      s.es = null;
    }
    s.pending = [];
    s.mySeqs.clear();
    s.inflightSeq = null;
    s.peerCursors.clear();
    renderCursorOverlay();
    setStatus('disconnected', '');
    el.ver().textContent = 'v —';
  }

  async function connect() {
    disconnect();
    const docId = el.doc().value.trim();
    if (!docId) {
      setStatus('need doc id', 'err');
      return;
    }
    setStatus('loading…', '');
    try {
      const r = await fetch(baseUrl() + '/doc/' + encodeURIComponent(docId));
      if (!r.ok) throw new Error(await r.text());
      const j = await r.json();
      s.serverText = j.text || '';
      s.serverVersion = j.version || 0;
      s.pending = [];
      s.inflightSeq = null;
      el.ta().value = fullText();
      el.ver().textContent = 'v ' + s.serverVersion;
    } catch (e) {
      console.error(e);
      setStatus('load failed', 'err');
      return;
    }

    const streamUrl = baseUrl() + '/doc/' + encodeURIComponent(docId) + '/stream';
    const es = new EventSource(streamUrl);
    s.es = es;
    es.addEventListener('op', (ev) => {
      try {
        handleSSEOp(JSON.parse(ev.data));
      } catch (e) {
        console.error(e);
      }
    });
    es.addEventListener('cursor', (ev) => {
      try {
        handleSSECursor(JSON.parse(ev.data));
      } catch (e) {
        console.error(e);
      }
    });
    // EventSource fires onerror during normal reconnect attempts, not only on
    // fatal failure; avoid flashing "stream error" unless the socket is dead.
    es.onerror = () => {
      if (es.readyState === EventSource.CLOSED) {
        setStatus('stream closed', 'err');
      }
    };
    es.onopen = () => setStatus('connected', 'ok');
  }

  function onInput() {
    if (!s.es) return;
    const ta = el.ta();
    const cur = ta.value;
    const prev = fullText();
    if (cur === prev) return;
    const ops = computeDelta(prev, cur);
    for (const op of ops) {
      s.pending.push({ seq: s.nextSeq++, op });
    }
    sendPendingFront();
  }

  async function onNewDoc() {
    const id = prompt('New document id (letters, digits, _.-):', 'notes');
    if (!id || !/^[A-Za-z0-9_.-]+$/.test(id)) return;
    try {
      const r = await fetch(baseUrl() + '/docs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id }),
      });
      if (!r.ok) throw new Error(await r.text());
      await refreshDocList();
      el.doc().value = id;
      el.docSelect().value = id;
      await connect();
    } catch (e) {
      alert(String(e));
    }
  }

  async function onFail() {
    const v = el.failR().value.trim();
    if (!v) return;
    try {
      const r = await fetch(baseUrl() + '/admin/fail/' + encodeURIComponent(v), { method: 'POST' });
      console.log(await r.text());
    } catch (e) {
      console.error(e);
    }
  }

  function wireUi() {
    el.btnConnect().addEventListener('click', () => connect());
    el.btnNew().addEventListener('click', () => onNewDoc());
    el.btnFail().addEventListener('click', () => onFail());
    el.docSelect().addEventListener('change', () => {
      el.doc().value = el.docSelect().value;
      connect();
    });
    el.ta().addEventListener('input', onInput);
    el.ta().addEventListener('select', scheduleCursorPost);
    el.ta().addEventListener('click', scheduleCursorPost);
    el.ta().addEventListener('keyup', scheduleCursorPost);
  }

  function parseQuery() {
    const q = new URLSearchParams(location.search);
    const su = q.get('server');
    const d = q.get('doc');
    if (su) el.server().value = su;
    if (d) el.doc().value = d;
  }

  async function boot() {
    parseQuery();
    wireUi();
    try {
      await refreshDocList();
    } catch (_) {}
    await connect();
  }

  /* ---- Console tests (same vectors as doc_ops.cc smoke tests) ---- */
  function runTests() {
    const doc = 'ABCDE';
    let n = 0;
    function check(cond, msg) {
      if (!cond) throw new Error(msg);
      n++;
    }
    function diamond(d0, a, b) {
      let p1 = d0;
      p1 = applyOp(p1, a);
      p1 = applyOp(p1, transform(b, a));
      let p2 = d0;
      p2 = applyOp(p2, b);
      p2 = applyOp(p2, transform(a, b));
      return p1 === p2;
    }
    check(
      transform({ type: 'I', pos: 3, text: 'XY' }, { type: 'I', pos: 1, text: 'PQ' }).pos === 5,
      'II/b-before'
    );
    check(diamond(doc, { type: 'I', pos: 3, text: 'XY' }, { type: 'I', pos: 1, text: 'PQ' }), 'II diamond');
    check(
      transform({ type: 'I', pos: 4, text: 'XY' }, { type: 'D', pos: 1, len: 2 }).pos === 2,
      'ID/after'
    );
    check(transform({ type: 'I', pos: 2, text: 'XY' }, { type: 'D', pos: 1, len: 3 }).text === '', 'ID/inside');
    check(
      transform({ type: 'D', pos: 3, len: 2 }, { type: 'I', pos: 1, text: 'XY' }).pos === 5,
      'DI/before'
    );
    check(
      transform({ type: 'D', pos: 1, len: 3 }, { type: 'I', pos: 2, text: 'XY' }).len === 5,
      'DI/inside expand'
    );
    const ddA = transform({ type: 'D', pos: 0, len: 1 }, { type: 'D', pos: 3, len: 2 });
    check(ddA.pos === 0 && ddA.len === 1, 'DD/A');
    const ddB = transform({ type: 'D', pos: 3, len: 2 }, { type: 'D', pos: 0, len: 2 });
    check(ddB.pos === 1 && ddB.len === 2, 'DD/B');
    console.log('runTests: passed', n, 'checks');
    return true;
  }

  window.runTests = runTests;
  window.connect = connect;

  boot();
})();
