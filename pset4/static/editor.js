/* DoomDraft browser client — OT + SSE (ops + cursors) + multi-doc */

(function () {
  'use strict';

  /** @typedef {{type:'I',pos:number,text:string}|{type:'D',pos:number,len:number}} Op */

  const DEBUG = true;

  function nowTag() {
    return new Date().toISOString().slice(11, 23);
  }

  function textHash(text) {
    let h = 2166136261 >>> 0;
    for (let i = 0; i < text.length; i++) {
      h ^= text.charCodeAt(i);
      h = Math.imul(h, 16777619) >>> 0;
    }
    return h.toString(16).padStart(8, '0');
  }

  function preview(text) {
    const s = String(text).replace(/\n/g, '\\n');
    return s.length > 80 ? s.slice(0, 80) + '…' : s;
  }

  function opSummary(op) {
    if (!op) return 'null';
    if (op.type === 'I') return `I@${op.pos}+${op.text.length} "${preview(op.text)}"`;
    return `D@${op.pos}x${op.len}`;
  }

  function pendingSummary() {
    return s.pending.map((p) => `#${p.seq}:${opSummary(p.op)}`).join(', ') || '(none)';
  }

  function debug(label, data) {
    if (!DEBUG) return;
    const base = {
      t: nowTag(),
      clientId,
      doc: el.doc() ? el.doc().value : undefined,
      serverVersion: s.serverVersion,
      inflightSeq: s.inflightSeq,
      pending: pendingSummary(),
      serverTextLen: s.serverText.length,
      serverTextHash: textHash(s.serverText),
      fullTextLen: fullText().length,
      fullTextHash: textHash(fullText()),
      textareaLen: el.ta() ? el.ta().value.length : undefined,
      textareaHash: el.ta() ? textHash(el.ta().value) : undefined,
    };
    const entry = { ...base, label, ...(data || {}) };
    console.log('[DoomDraft]', label, entry);
    try {
      const body = JSON.stringify(entry);
      const url = apiUrl('/debug/log');
      if (navigator.sendBeacon) {
        navigator.sendBeacon(url, new Blob([body], { type: 'application/json' }));
      } else {
        fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body,
          keepalive: true,
        }).catch(() => {});
      }
    } catch (_) {}
  }

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

  let connectGen = 0;

  const s = {
    serverText: '',
    pending: /** @type {{seq:number, op:Op}[]} */ ([]),
    mySeqs: new Set(),
    inflightSeq: /** @type {number|null} */ (null),
    nextSeq: 0,
    serverVersion: 0,
    es: /** @type {EventSource|null} */ (null),
    cursorTimer: /** @type {number|null} */ (null),
    cursorInflight: false,
    cursorDirty: false,
    peerCursors: /** @type {Map<string, {pos:number, color:string}>} */ (new Map()),
  };

  const COLORS = ['#f38ba8', '#fab387', '#f9e2af', '#a6e3a1', '#89dceb', '#cba6f7', '#eba0ac'];

  function setStatus(t, cls) {
    const st = el.status();
    st.textContent = t;
    st.className = cls || '';
  }

  /** Browser uses this for connection refused, CORS blocks, file://→http, etc. */
  function describeFetchFailure(e) {
    const msg = e && e.message != null ? String(e.message) : String(e);
    if (e instanceof TypeError && /failed to fetch|load failed|networkerror/i.test(msg)) {
      return (
        msg +
        ' — server not reachable or request blocked. Run ./build/pt-collab-server; ' +
        'open http://localhost:8080/ (not file://); set Server to the exact host in the address bar ' +
        '(localhost vs 127.0.0.1 must match if you are not using path-only /doc/… URLs).'
      );
    }
    return msg;
  }

  /**
   * When the configured server matches the page origin, return '' so requests
   * use path-only URLs (/doc/…). That avoids cross-origin POST (e.g. page on
   * http://localhost:8080 but Server field http://127.0.0.1:8080), which often
   * fails with "Failed to fetch" while GET/EventSource still work.
   */
  function apiRoot() {
    const raw = el.server().value.trim().replace(/\/$/, '');
    let s = raw || 'http://localhost:8080';
    if (!/^https?:\/\//i.test(s)) s = 'http://' + s;
    try {
      const api = new URL(s);
      if (location.protocol === 'http:' || location.protocol === 'https:') {
        if (api.origin === new URL(location.href).origin) return '';
      }
      return api.origin;
    } catch (_) {
      return 'http://localhost:8080';
    }
  }

  /** @param {string} path must start with / */
  function apiUrl(path) {
    const p = path.startsWith('/') ? path : '/' + path;
    return apiRoot() + p;
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
      debug('textarea resync', {
        beforeLen: ta.value.length,
        beforeHash: textHash(ta.value),
        wantLen: want.length,
        wantHash: textHash(want),
        selectionStart: sel,
        delta,
        beforePreview: preview(ta.value),
        wantPreview: preview(want),
      });
      ta.value = want;
      try {
        ta.setSelectionRange(Math.max(0, sel + delta), Math.max(0, sel + delta));
      } catch (_) {}
    }
  }

  async function refreshDocList() {
    const sel = el.docSelect();
    const r = await fetch(apiUrl('/docs'));
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

  /** Apply local ack from POST /op body; SSE may duplicate the same version (ignored there). */
  function ackPostIfStillInflight(version, seq) {
    debug('POST ack received', { version, seq });
    if (s.inflightSeq !== seq) {
      debug('POST ack ignored: different inflight seq', { version, seq });
      sendPendingFront();
      return;
    }
    if (!s.pending.length || s.pending[0].seq !== seq) {
      debug('POST ack ignored: pending front changed', { version, seq });
      s.inflightSeq = null;
      sendPendingFront();
      return;
    }
    if (version <= s.serverVersion) {
      debug('POST ack ignored: stale version', { version, seq });
      s.inflightSeq = null;
      sendPendingFront();
      return;
    }
    const { op: committed } = s.pending[0];
    debug('POST ack committing local op', { version, seq, op: opSummary(committed) });
    s.serverText = applyOp(s.serverText, committed);
    s.pending.shift();
    s.mySeqs.delete(seq);
    s.inflightSeq = null;
    s.serverVersion = version;
    el.ver().textContent = 'v ' + s.serverVersion;
    syncTextareaFromModel();
    sendPendingFront();
  }

  async function sendPendingFront() {
    if (s.inflightSeq !== null || !s.pending.length) {
      debug('sendPendingFront skipped', {
        reason: s.inflightSeq !== null ? 'already inflight' : 'no pending',
      });
      return;
    }
    const { seq, op } = s.pending[0];
    s.inflightSeq = seq;
    s.mySeqs.add(seq);
    const docId = el.doc().value;
    debug('POST /op send', {
      seq,
      op: opSummary(op),
      wire: opToWire(op, seq, clientId),
      baseServerVersion: s.serverVersion,
      baseServerTextHash: textHash(s.serverText),
      optimisticTextHash: textHash(fullText()),
    });
    try {
      const r = await fetch(apiUrl('/doc/' + encodeURIComponent(docId) + '/op'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(opToWire(op, seq, clientId)),
      });
      if (!r.ok) {
        const t = await r.text();
        throw new Error(t || r.statusText);
      }
      let version = /** @type {number|null} */ (null);
      try {
        const j = await r.json();
        if (typeof j.version === 'number' && Number.isFinite(j.version)) version = j.version;
        debug('POST /op response body', { seq, body: j });
      } catch (_) {
        debug('POST /op response had malformed/empty JSON', { seq });
        /* ignore malformed body */
      }
      if (version !== null && version > 0) ackPostIfStillInflight(version, seq);
    } catch (e) {
      console.error('[DoomDraft] POST /op failed', e);
      if (s.inflightSeq !== seq || !s.pending.length || s.pending[0].seq !== seq) {
        debug('POST /op failure ignored: op already resolved', {
          seq,
          op: opSummary(op),
          error: describeFetchFailure(e),
        });
        sendPendingFront();
        return;
      }
      debug('POST /op failed', { seq, op: opSummary(op), error: describeFetchFailure(e) });
      setStatus('send failed: ' + describeFetchFailure(e), 'err');
      s.mySeqs.delete(seq);
      s.inflightSeq = null;
    }
  }

  function handleSSEOp(data) {
    debug('SSE op received', {
      event: data,
      eventOp: opSummary(data && data.op),
      isMine: data && data.client_id === clientId,
      matchesPendingFront: !!(
        data && s.pending.length && data.seq === s.pending[0].seq
      ),
    });
    // GET /doc/<id> already returns full text at serverVersion. The stream replays
    // all committed ops from version 0 for new connections, so without this
    // guard we would apply every historical op again (reload / second tab breaks).
    if (data.version <= s.serverVersion) {
      debug('SSE op ignored: stale/duplicate version', {
        eventVersion: data.version,
        serverVersion: s.serverVersion,
      });
      return;
    }
    const ta = el.ta();
    if (s.pending.length && data.seq === s.pending[0].seq && s.mySeqs.has(data.seq) && data.client_id === clientId) {
      const committed = wireToOp(data.op);
      debug('SSE local ack path', { version: data.version, seq: data.seq, op: opSummary(committed) });
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
    debug('SSE foreign apply start', {
      version: data.version,
      seq: data.seq,
      client_id: data.client_id,
      foreign: opSummary(foreign),
      pendingBefore: pendingSummary(),
    });
    for (let i = 0; i < s.pending.length; i++) {
      const before = s.pending[i].op;
      s.pending[i].op = transform(s.pending[i].op, foreign);
      debug('pending transformed by foreign op', {
        pendingSeq: s.pending[i].seq,
        before: opSummary(before),
        foreign: opSummary(foreign),
        after: opSummary(s.pending[i].op),
      });
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
    debug('SSE foreign apply done', {
      version: data.version,
      cursorAfter: c,
      pendingAfter: pendingSummary(),
    });
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
    debug('SSE cursor received', data);
    s.peerCursors.set(data.client_id, { pos: data.pos, color: hashColor(data.client_id) });
    renderCursorOverlay();
  }

  function scheduleCursorPost() {
    if (s.cursorTimer) clearTimeout(s.cursorTimer);
    s.cursorTimer = window.setTimeout(postCursor, 80);
  }

  async function postCursor() {
    if (!s.es) return;
    if (s.cursorInflight) {
      s.cursorDirty = true;
      debug('POST /cursor coalesced', {});
      return;
    }
    s.cursorInflight = true;
    s.cursorDirty = false;
    const ta = el.ta();
    const pos = ta.selectionStart ?? 0;
    const docId = el.doc().value;
    debug('POST /cursor send', { pos });
    try {
      await fetch(apiUrl('/doc/' + encodeURIComponent(docId) + '/cursor'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ client_id: clientId, pos }),
      });
    } catch (e) {
      debug('POST /cursor failed', { pos, error: describeFetchFailure(e) });
    } finally {
      s.cursorInflight = false;
      if (s.cursorDirty && s.es) scheduleCursorPost();
    }
  }

  /** @param {{ reconnecting?: boolean }} [opts] */
  function disconnect(opts) {
    const reconnecting = !!(opts && opts.reconnecting);
    debug('disconnect', { reconnecting });
    if (s.es) {
      const old = s.es;
      s.es = null;
      // Closing the socket can fire onerror with CLOSED; clear handlers first
      // so doc switches do not flash "stream closed" / look like a server crash.
      old.onerror = null;
      old.onopen = null;
      old.close();
    }
    s.pending = [];
    s.mySeqs.clear();
    s.inflightSeq = null;
    s.cursorInflight = false;
    s.cursorDirty = false;
    s.peerCursors.clear();
    renderCursorOverlay();
    if (!reconnecting) {
      setStatus('disconnected', '');
      el.ver().textContent = 'v —';
    }
  }

  async function connect() {
    const myGen = ++connectGen;
    debug('connect start', { generation: myGen, apiRoot: apiRoot() });
    disconnect({ reconnecting: true });
    const docId = el.doc().value.trim();
    if (!docId) {
      setStatus('need doc id', 'err');
      return;
    }
    setStatus('loading…', '');
    try {
      const r = await fetch(apiUrl('/doc/' + encodeURIComponent(docId)));
      if (myGen !== connectGen) return;
      if (!r.ok) throw new Error(await r.text());
      const j = await r.json();
      if (myGen !== connectGen) return;
      s.serverText = j.text || '';
      s.serverVersion = j.version || 0;
      s.pending = [];
      s.inflightSeq = null;
      el.ta().value = fullText();
      el.ver().textContent = 'v ' + s.serverVersion;
      debug('GET /doc loaded', {
        generation: myGen,
        loadedVersion: s.serverVersion,
        loadedLen: s.serverText.length,
        loadedHash: textHash(s.serverText),
        loadedPreview: preview(s.serverText),
      });
    } catch (e) {
      if (myGen !== connectGen) return;
      console.error(e);
      setStatus('load failed: ' + describeFetchFailure(e), 'err');
      return;
    }

    if (myGen !== connectGen) return;
    const streamUrl = apiUrl('/doc/' + encodeURIComponent(docId) + '/stream');
    debug('SSE open requested', { streamUrl, generation: myGen });
    const es = new EventSource(streamUrl);
    s.es = es;
    es.addEventListener('op', (ev) => {
      if (s.es !== es) return;
      try {
        handleSSEOp(JSON.parse(ev.data));
      } catch (e) {
        console.error(e);
      }
    });
    es.addEventListener('cursor', (ev) => {
      if (s.es !== es) return;
      try {
        handleSSECursor(JSON.parse(ev.data));
      } catch (e) {
        console.error(e);
      }
    });
    // EventSource fires onerror during normal reconnect attempts, not only on
    // fatal failure; avoid flashing "stream error" unless the socket is dead.
    es.onerror = () => {
      if (s.es !== es) return;
      debug('SSE error', { readyState: es.readyState });
      if (es.readyState === EventSource.CLOSED) {
        setStatus('stream closed', 'err');
      }
    };
    es.onopen = () => {
      if (s.es !== es) return;
      debug('SSE open', { readyState: es.readyState });
      setStatus('connected', 'ok');
    };
  }

  function onInput() {
    if (!s.es) return;
    const ta = el.ta();
    const cur = ta.value;
    const prev = fullText();
    if (cur === prev) return;
    const ops = computeDelta(prev, cur);
    debug('input delta computed', {
      prevLen: prev.length,
      prevHash: textHash(prev),
      curLen: cur.length,
      curHash: textHash(cur),
      selectionStart: ta.selectionStart,
      ops: ops.map(opSummary),
      prevPreview: preview(prev),
      curPreview: preview(cur),
    });
    for (const op of ops) {
      s.pending.push({ seq: s.nextSeq++, op });
      debug('pending op enqueued', { seq: s.nextSeq - 1, op: opSummary(op) });
    }
    sendPendingFront();
  }

  async function onNewDoc() {
    const id = prompt('New document id (letters, digits, _.-):', 'notes');
    if (!id || !/^[A-Za-z0-9_.-]+$/.test(id)) return;
    try {
      const r = await fetch(apiUrl('/docs'), {
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
      const r = await fetch(apiUrl('/admin/fail/' + encodeURIComponent(v)), { method: 'POST' });
      console.log(await r.text());
    } catch (e) {
      console.error(e);
    }
  }

  function wireUi() {
    el.btnConnect().addEventListener('click', () => {
      connect().catch((e) => console.error(e));
    });
    el.btnNew().addEventListener('click', () => onNewDoc());
    el.btnFail().addEventListener('click', () => onFail());
    el.docSelect().addEventListener('change', () => {
      el.doc().value = el.docSelect().value;
      connect().catch((e) => console.error(e));
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
    if (su) {
      el.server().value = su;
    } else if (
      (location.protocol === 'http:' || location.protocol === 'https:') &&
      location.port === '8080'
    ) {
      // Page served from the collab server port → POST/SSE must hit same origin.
      el.server().value = location.origin;
    }
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
