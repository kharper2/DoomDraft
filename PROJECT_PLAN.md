# CS 2620 Pset 4: Collaborative Document Editing on Paxos

**Ambition:** 4/5 in the handout sense (“application on Paxos + failures”); this plan aims at the **upper end** of that band (full OT in sim + optional browser demo).  
**Team:** 2 people (~15 hours each — treat as a **lower bound**; scope below is aggressive).  
**Deadline:** May 6 (demo), May 13 (final)

**Roles**

- **Aengus:** Paxos-side integration, `collab_model`, `pt-collab`, OT core + simulation failure campaign.
- **Kathryn:** HTTP/SSE API, `pt-collab-server`, browser client, demo polish.

**Scope tiers (use this when time runs short):**

1. **Tier 1 — assignment-critical:** Simulation harness + OT/unit tests + randomized diamond check + `pt-collab` with failure schedules + **pairwise same reconstructed text on every live replica** after each seed. No browser required.
2. **Tier 2 — planned demo:** HTTP + SSE + single doc + two browser tabs + basic failover story.
3. **Tier 3 — stretch:** Cursors/presence, multi-document registry, full `collab-bench.sh` matrix at large `-R`. Cut tier 3 first, then trim tier 2 to curl-only if needed.

**Single source of truth:** This repo uses **only** `PROJECT_PLAN.md` for planning (no second planning file — avoids drift and “which doc is right?”).

---

## One-paragraph description

Build a Google Docs–style collaborative text editor backed by the existing Paxos implementation. The system has two distinct layers. The **backend** (Aengus) is a simulation-based test harness: multiple simulated editors submit character-level insert/delete operations through Paxos, which imposes a total order on all ops; Operational Transformation (OT) on the client side reconciles each editor's pending local edits against remotely committed ops; a `collab_model::check()` function verifies that all live replicas converge to identical document text under every failure schedule (failover, split-brain, partition, torture). The **frontend** (Kathryn) is a real HTTP/EventSource server that exposes the Paxos backend as a web API, with a browser-based editor where multiple real users can collaboratively edit in real-time over HTTP, with cursor/presence tracking and a document registry. The combination of rigorous simulation-based correctness testing and a working real-world deployment is what pushes this to the upper end of Ambition 4.

---

## Architecture overview

### Why Paxos makes OT tractable

OT's hard part is establishing a total order for concurrent operations. Paxos already solves this: the commit log slot number *is* the canonical total order. This lets us use the simplified Jupiter model:

- The **Paxos log** = the "server state" (total order of all committed ops).
- Each **editor** has a queue of pending ops not yet committed.
- When the editor sees a newly committed op from another editor, it OT-transforms its pending ops against it, then updates its local document view.
- When the editor's own op is committed (it appears in the log), it pops it from the pending queue.

### Data layout in PancyDB

| Key pattern | Value | Who writes |
|---|---|---|
| `doc/<id>/op/<client_id>/<seq>` | serialized insert/delete op | Each editor client |
| `doc/<id>/cursor/<client_id>` | serialized cursor position | Each editor client |
| `docs/registry` | JSON list of doc IDs | Document creation |

Document text is **never stored directly** — it is reconstructed by iterating all `doc/<id>/op/*` keys, sorting by a **per-replica commit order** you can rely on (see below), and replaying ops in order. This gives deterministic reconstruction on every live replica.

**Verify in the real codebase (do not assume from this doc alone):**

- **Total order key:** Confirm whether `pancy::version_type` (or whatever PancyDB exposes) is a **single global sequence** aligned with Paxos log order for all keys. If not, store an explicit **log index / slot** in the value or use one ordered namespace so “sort for replay” matches **true commit order**.
- **Iteration:** Confirm you can **list keys by prefix** (`doc/<id>/op/…`) efficiently; if not, switch layout (e.g. one append-only key per slot).
- **Leader reads (Kathryn / HTTP path):** Any “read the leader’s DB” path must use the **same leader discovery / redirect** behavior as the rest of the Paxos client stack after failover, or you can read stale state.

Op serialization format:
- Insert: `I <pos> <text>` e.g. `I 5 hello`
- Delete: `D <pos> <len>` e.g. `D 5 3`
- Cursor: `C <pos>` e.g. `C 12`

### OT transform rules (character-level)

`transform(a, b)` returns `a'` such that `apply(apply(doc, b), a') == apply(apply(doc, a), transform(b, a))`. Four cases:

| a \ b | Insert(pb, tb) | Delete(pb, lb) |
|---|---|---|
| **Insert(pa, ta)** | pb ≤ pa → Insert(pa+\|tb\|, ta); else unchanged | pb+lb ≤ pa → Insert(pa−lb, ta); pb ≤ pa < pb+lb → Insert(pb, ta); else unchanged |
| **Delete(pa, la)** | pb ≤ pa → Delete(pa+\|tb\|, la); pa ≤ pb < pa+la → three-way split; else unchanged | Four overlap sub-cases (shrink, eliminate, shift, split) |

---

## Aengus: Backend, simulation, and testing (~15 hours)

**Owner (Aengus):** correctness of the OT engine, simulation-based test harness, failure testing campaign.

### Files

```
doc_ops.hh/cc         — op types, OT transform, serialization, unit tests
doc_state.hh/cc       — document reconstruction from PancyDB
collab_model.hh/cc    — simulation client model extending client_model
pt-collab.cc          — main test binary (mirrors pt-paxos.cc)
collab-bench.sh       — failure testing campaign script
```

### Phase A1 — OT engine (3.5 hours)

**`doc_ops.hh`**:

```cpp
namespace collab {

struct insert_op { size_t pos; std::string text; };
struct delete_op { size_t pos; size_t len; };
using doc_op = std::variant<insert_op, delete_op>;

std::string serialize(const doc_op&);
doc_op deserialize(std::string_view);
void apply(std::string& text, const doc_op&);

// OT core: a' = transform(a, b) means "a as if b had already been applied"
doc_op transform(const doc_op& a, const doc_op& b);

// Convenience: transform a against an ordered sequence of already-committed ops
doc_op transform_seq(doc_op a, std::span<const doc_op> committed);

}
```

**`doc_ops.cc`** — implement the four transform cases. Add standalone unit tests runnable via `./pt-collab --test`:

1. Insert at same position as concurrent insert (tie-break by client ID).
2. Insert after concurrent insert (position shifts).
3. Insert inside a concurrent delete (lands at delete start).
4. Delete before a concurrent delete (no overlap, position shifts).
5. Delete overlapping a concurrent delete (shrink/eliminate cases).
6. Delete spanning a concurrent insert (split into two deletes).
7. Commutativity property: `apply(apply(doc, a), transform(b,a)) == apply(apply(doc, b), transform(a,b))` for 1000 random op pairs.
8. Serialize/deserialize round-trip for both op types.

Test 7 is a randomized property check — generate random short strings and random insert/delete ops, assert the diamond property holds. This is the most important test.

### Phase A2 — Document state reconstruction (1.5 hours)

**`doc_state.hh`**:

```cpp
namespace collab {

struct committed_op {
    pancy::version_type version;  // PancyDB version = Paxos commit slot
    uint64_t client_id;
    uint64_t client_seq;
    doc_op op;
};

// Collect all committed ops for a document from a pancydb, sorted by version
std::vector<committed_op> read_ops(const pancy::pancydb& db,
                                   std::string_view doc_id);

// Replay an ordered op sequence to produce document text
std::string reconstruct(std::span<const committed_op> ops);

}
```

`read_ops` iterates PancyDB keys with prefix `doc/<doc_id>/op/`, deserializes each, sorts by `vv.version`. The `version` field is already set by PancyDB's monotonic counter, so no extra bookkeeping is needed.

### Phase A3 — Collaborative client model (4 hours)

**`collab_model.hh`** extends `client_model`:

```cpp
class collab_model : public client_model {
public:
    collab_model(size_t nreplicas, random_source&,
                 std::string doc_id = "main", size_t nclients = 8);

    void start() override;
    void stop() override;
    std::optional<std::string> check(const pancy::pancydb& db) override;

    unsigned long ops_submitted = 0;
    unsigned long ops_committed = 0;
    unsigned long ops_transformed = 0;

private:
    std::string doc_id_;
    size_t nclients_;

    struct editor_state {
        cotamer::task<> task;
        size_t leader;
        std::string local_text;            // optimistic local view
        std::vector<collab::doc_op> pending;  // submitted but not yet committed
        std::vector<uint64_t> pending_seqs;
        std::map<uint64_t, uint64_t> peer_next_seq; // cid -> next seq to fetch
        pancy::version_type last_seen_version = 0;
        uint64_t next_seq = 0;
    };

    std::vector<std::unique_ptr<editor_state>> editors_;
    cotamer::task<> editor(unsigned cid);
    cotamer::task<> sync_committed_ops(editor_state&, unsigned cid);
};
```

**Editor coroutine loop** (`editor()` in `collab_model.cc`):

```
loop:
  1. sync_committed_ops():
     For each known peer, try GET doc/<id>/op/<peer_cid>/<peer_next_seq>.
     If found:
       - If peer == me and seq is in pending: pop from pending (committed).
       - Else: OT-transform all my pending ops against this committed op;
               apply committed op to local_text; update last_seen_version.
       - Increment peer_next_seq[peer_cid].

  2. With probability 0.6, generate a random edit:
     - Random insert (random position, 1–5 char random string) OR
     - Random delete (random position, length ≤ min(5, remaining)).
     - Apply to local_text immediately (optimistic local update).
     - Push onto pending; record seq.

  3. If pending non-empty, submit pending[0] as PUT doc/<id>/op/<my_cid>/<seq>.
     Retry with redirect on timeout, switching leader every 3 timeouts.

  4. co_await cotamer::after(random 1–20ms).
```

**`check()` vs cross-replica convergence (keep the invariant obvious):**

- **`collab_model::check(db)` (per-replica):** Use this for **local** invariants on that replica’s view: e.g. no illegal ops (positions/lengths in range), optional per-client `seq` gap detection, deserialize errors. Avoid a fuzzy pattern like “first `check()` sets golden `expected_text_`” unless you clearly define what that means across replicas.
- **`try_one_seed()` in `pt-collab.cc` (canonical):** After steps / schedules, for **every pair of live replicas** `(i, j)`, compute `text_i = reconstruct(read_ops(db_i, doc_id))` and `text_j = reconstruct(...)` and **`assert(text_i == text_j)`**. That is the **real** convergence guarantee for the writeup and grading.
- Optional: also assert `text_i` equals a **reference simulation** (single-threaded replay of the same committed op sequence) if you have one — catches ordering bugs in `read_ops`.

Cross-replica checks must **not** depend on the order in which `check()` happens to be called on different replicas unless you document a single shared oracle (usually unnecessary — use pairwise equality in `try_one_seed()`).

### Phase A4 — Binary and test campaign (2 hours)

**`pt-collab.cc`** mirrors `pt-paxos.cc`:
- Same `testinfo` + failure schedule infrastructure (copy directly, change client type).
- `try_one_seed()` uses `collab_model` instead of `lockseq_model`.
- After simulation: reconstruct doc on each live replica, assert all match.
- Print ops submitted/committed/transformed.

**`collab-bench.sh`** test matrix:

| Test | Flags | Goal |
|---|---|---|
| OT unit tests | `--test` | Verify transform diamond property |
| Basic convergence | `-R 200` | All replicas converge, no divergence |
| Failover | `-f failover -R 50` | New leader, document intact |
| Recovery | `-f recover -R 50` | Rejoining replica catches up via log |
| Split brain | `-f split -R 50` | Minority cannot commit; no divergence |
| Unstable | `-f unstable -R 30` | Convergence under churn |
| Torture | `-f torture -R 20` | Convergence under adversarial scheduling |
| High loss | `-l 0.15 -R 100` | Still converges with 15% message loss |

### Phase A5 — Lab notebook for backend (2 hours)

Track: what broke, what surprising behaviors emerged, interesting seeds.

---

## Kathryn: HTTP server, browser UI, real-world demo (~15 hours)

**Owner (Kathryn):** real-world deployment, HTTP API, browser-based collaborative editor, cursor/presence, multiple documents.

### Files

```
http_server.hh/cc     — HTTP/SSE server sitting in front of Paxos replicas
doc_http.hh/cc        — HTTP handlers for document operations
collab_client.hh/cc   — HTTP client that wraps collab_model for real connections
static/editor.html    — single-file browser-based collaborative editor
static/editor.js      — JS OT client: applies incoming ops, submits local edits
pt-collab-server.cc   — main binary: runs Paxos replicas + HTTP server
```

### Phase B1 — HTTP API design (1 hour)

Design the REST + SSE API that the browser uses:

| Method | Path | Body | Response | Notes |
|---|---|---|---|---|
| GET | `/docs` | — | JSON list of doc IDs | lists `docs/registry` |
| POST | `/docs` | `{"id": "foo"}` | `{"id": "foo"}` | creates document |
| GET | `/doc/<id>` | — | `{"text": "...", "version": N}` | current snapshot |
| POST | `/doc/<id>/op` | `{"type":"I","pos":5,"text":"hi","client_id":"x","seq":3}` | `{"version": N}` | submits op through Paxos, blocks until committed |
| GET | `/doc/<id>/stream` | — | SSE stream of ops | EventSource; pushes committed ops |
| POST | `/doc/<id>/cursor` | `{"pos": 12}` | `{}` | updates cursor in PancyDB |
| GET | `/doc/<id>/cursors` | — | JSON map client→pos | all active cursors |

The SSE stream (`/doc/<id>/stream`) is the key real-time mechanism. The server keeps a sorted watermark of committed ops and pushes new ones as they appear, allowing browser clients to reconstruct the document without polling.

### Phase B2 — HTTP server layer (4 hours)

**`http_server.hh`** wraps Cotamer's HTTP support:

```cpp
class collab_http_server {
public:
    collab_http_server(pt_paxos_instance& paxos, std::string doc_id = "main");
    void start(uint16_t port);
    void stop();

private:
    pt_paxos_instance& paxos_;
    std::string doc_id_;

    // SSE subscriber list: each GET /stream gets a channel to receive new ops
    std::vector<cotamer::channel<collab::committed_op>*> sse_subscribers_;

    cotamer::task<> handle_request(http::request req);
    cotamer::task<> handle_get_doc(http::response& resp);
    cotamer::task<> handle_post_op(http::request& req, http::response& resp);
    cotamer::task<> handle_stream(http::response& resp);
    cotamer::task<> handle_cursors(http::response& resp);

    // Background loop: watch PancyDB for new committed ops, fan out to SSE
    cotamer::task<> watch_ops();
    pancy::version_type last_broadcast_version_ = 0;
};
```

The `watch_ops()` coroutine polls PancyDB (or hooks into the replica's `apply_committed_entries` callback) for new ops and fans them out to all SSE subscribers. This is the server-push mechanism.

Key implementation note: the HTTP server connects to the **leader replica's** PancyDB for reads, and routes write requests (POST /op) through the client request channel. This means the server is a special "privileged client" in the Paxos cluster.

### Phase B3 — Browser editor (4 hours)

**`static/editor.html`** — single-file, no build step, no dependencies except vanilla JS:

```
+--------------------------------------------------+
|  DoomDraft          [doc: main ▾]   [New doc]    |
+--------------------------------------------------+
|  [User: alice ●]  [User: bob ●]                  |
+--------------------------------------------------+
|                                                  |
|  Hello, world!                                   |
|    ^-- colored cursor for "bob"                  |
|                                                  |
|  (textarea with live collaborative editing)      |
|                                                  |
+--------------------------------------------------+
|  Status: connected  |  Version: 47  |  3 users   |
+--------------------------------------------------+
```

**`static/editor.js`** — vanilla JS OT client:

```javascript
class CollabEditor {
    constructor(docId, clientId) {
        this.docId = docId;
        this.clientId = clientId;
        this.seq = 0;
        this.pendingOps = [];      // submitted but not yet confirmed
        this.localText = '';
        this.serverVersion = 0;
    }

    // Called when user types (input event on textarea)
    onLocalEdit(insertions, deletions) {
        // Compute insert/delete ops from textarea diff
        const op = computeOp(this.localText, newText);
        this.localText = newText;
        this.pendingOps.push(op);
        this.submitNext();
    }

    // Submit pending[0] to POST /doc/<id>/op
    async submitNext() { ... }

    // Called when SSE delivers a committed op
    onCommittedOp(op) {
        if (op.client_id === this.clientId && op.seq in this.pending) {
            this.pendingOps.shift();  // our op committed
        } else {
            // OT-transform all pending against this op
            this.pendingOps = this.pendingOps.map(p => transform(p, op));
            applyToTextarea(op);
        }
        this.serverVersion = op.version;
    }
}
```

The JS `transform()` function is a direct port of the C++ OT logic — since both must implement the same rules, testing the two against each other is a free cross-check.

### Phase B4 — Cursor/presence tracking (2 hours)

Cursor positions are stored as PancyDB PUTs: key `doc/<id>/cursor/<client_id>`, value `C <pos>`. The HTTP server's SSE stream includes cursor events as well as op events. The browser renders other users' cursors as colored vertical bars in the textarea using a positioned overlay div.

The watch_ops() loop also polls `doc/<id>/cursor/*` keys for changes, broadcasting cursor events to SSE subscribers. Cursors must also be OT-transformed: when a remote op arrives, the local copy of peer cursor positions are shifted by the same rules as insert positions.

### Phase B5 — Multiple document support (1 hour)

- `docs/registry` key in PancyDB holds a JSON array of document IDs.
- Creating a document = PUT to `docs/registry` with updated list (CAS for safety).
- The HTTP `/docs` endpoint lists them; the browser lets you switch between docs.
- Each document gets its own op namespace (`doc/<id>/op/*`) so there is no interference.

### Phase B6 — `pt-collab-server.cc` (1 hour)

A separate main binary that runs the Paxos cluster AND the HTTP server in the same process (different coroutines). Start with 3 replicas on localhost and one HTTP server listening on `:8080`. The demo: open two browser tabs, type in both, watch edits merge.

```
./pt-collab-server --port 8080 --replicas 3
# Then open http://localhost:8080/editor.html in two browser tabs
# Kill the process running replica 0 to demo failover
# Watch the editor reconnect and continue working
```

### Phase B7 — Lab notebook for frontend (2 hours)

Track: what SSE edge cases arose, how cursor tracking under OT was handled, interesting browser-visible failure modes.

---

## Integration and joint work

### Work order checklist

Check boxes when that person’s part is **done and merged** (or tick **N/A** in chat if you skip an item). Steps are mostly **sequential**; read each step’s **gate** before starting.

---

#### Step 0 — Contract (both)

*Gate: none.*

- [ ] **Both:** Agreed **op string format** (e.g. `I <pos> <text>`, `D <pos> <len>`, plus any cursor line if you use it).
- [ ] **Both:** Sketched **HTTP routes** (paths, JSON fields, `client_id` / `seq` / `version` semantics) — enough that step 2 API spec is not vague.

---

#### Step 1 — `doc_ops` (Aengus)

*Gate: step 0 done.*

- [ ] **Aengus:** `doc_ops.hh` / `doc_ops.cc`: types, `serialize` / `deserialize`, `apply`, `transform`.
- [ ] **Aengus:** Unit tests wired (e.g. `./pt-collab --test` or your chosen binary) including **randomized diamond / commutativity** check when ready.

---

#### Step 2 — Parallel track

*Gate: **Aengus** — after step 1. **Kathryn (API only)** — after step 0. **Kathryn (`editor.js` / match `doc_ops`)** — after step 1.*

- [ ] **Aengus:** `doc_state.hh` / `doc_state.cc`: `read_ops`, `reconstruct` (or equivalent).
- [ ] **Kathryn:** **HTTP API spec** written down (B1): paths, request/response JSON, errors) — can finish **without** waiting for Aengus code.
- [ ] **Kathryn:** **`editor.js`** OT / tests **or** any code that must **match `doc_ops`** — only after **`doc_ops.hh` exists** (step 1).

---

#### Step 3 — Simulated clients (Aengus)

*Gate: Aengus step 2 (`doc_state`) usable.*

- [ ] **Aengus:** `collab_model` + editor coroutines (simulated editors → Paxos).
- [ ] **Aengus:** Sanity: sim clients commit ops and **reconstruct** matches expectations (manual or test).

---

#### Step 4 — Sim binary ∥ HTTP layer

*Gate: **Kathryn** C++ that `#include`s `doc_ops` — after step 1. **Kathryn** deep replica wiring — after step 3 if you need Aengus’s running layout. **Aengus** — after step 3.*

- [ ] **Aengus:** `pt-collab.cc` + failure schedules; **pairwise reconstruct** across live replicas in harness.
- [ ] **Kathryn:** `http_server` / `doc_http` (B2): handlers + Paxos client path (redirects / leader as needed).

---

#### Step 5 — Combined server binary (Kathryn)

*Gate: Aengus has a **runnable** Paxos + apply path you can call (steps 3–4).*

- [ ] **Kathryn:** `pt-collab-server` (B6): replicas + HTTP; smoke **two tabs** or curl.
- [ ] **Kathryn:** Document in README how to start (ports, # replicas).

---

#### Step 6 — Hardening & stretch

*Gate: step 4–5 basically working.*

- [ ] **Aengus:** `collab-bench.sh` / seed campaign / extra failure modes as planned.
- [ ] **Kathryn:** Cursors / presence (B4) — *optional if time.*
- [ ] **Kathryn:** Multi-document registry (B5) — *optional if time.*

---

#### Step 7 — Integration (both)

*Gate: steps 4–6 far enough that sim + server can run against same contract.*

- [ ] **Both:** Session: `pt-collab-server` + browser (or curl) **and** `pt-collab` sim on aligned behavior.
- [ ] **Both:** **Shared OT vectors:** same inputs → C++ `transform` and JS `transform` agree (script or documented manual vectors).
- [ ] **Both:** Demo path agreed (what you show for class / writeup).

---

### Joint tasks checklist (throughout / end)

*These are not strictly one step; tick when done.*

- [ ] **Both:** Joint design session done (can be the same work as step 0 — don’t double-count unless you did a second pass).
- [ ] **Aengus:** Lab notebook / backend notes caught up for writeup.
- [ ] **Kathryn:** Lab notebook / frontend notes caught up for writeup.
- [ ] **Aengus:** Own **4–6 page** writeup draft (OT, Paxos, testing).
- [ ] **Kathryn:** Own **4–6 page** writeup draft (HTTP, demo, protocol).

---

**Rule of thumb:** Kathryn can finish the **API spec** after **step 0** only. Code that **`#include`s `doc_ops.hh`** or JS that must **match** `serialize` / `transform` waits for **step 1**. **Live Paxos** in the server waits on Aengus through roughly **steps 3–5**.

---

## File structure summary

```
pset3/
  doc_ops.hh / doc_ops.cc          ← Aengus
  doc_state.hh / doc_state.cc      ← Aengus
  collab_model.hh / collab_model.cc ← Aengus
  pt-collab.cc                     ← Aengus
  collab-bench.sh                  ← Aengus
  http_server.hh / http_server.cc  ← Kathryn
  doc_http.hh / doc_http.cc        ← Kathryn
  pt-collab-server.cc              ← Kathryn
  static/
    editor.html                    ← Kathryn
    editor.js                      ← Kathryn
  CMakeLists.txt                   ← both (add two new targets)
```

All of `pancydb`, `netsim`, `client_model`, `pt_paxos_replica`, `cotamer`, `lockseq_model` are **unchanged**.

---

## What "done" looks like

**Simulation (Aengus):**
- `./pt-collab --test` passes all OT unit tests including the randomized diamond property check.
- `./pt-collab -R 500` passes with no divergence.
- `./pt-collab -f failover`, `-f split`, `-f torture` all pass.
- `./pt-collab -l 0.15 -R 100` passes (high-loss correctness).

**Real-world (Kathryn):**
- `./pt-collab-server --port 8080` starts.
- Two browser tabs on the same document show each other's edits in real time.
- Cursor positions update live.
- You can kill the terminal process for one replica; the editor reconnects and continues.
- You can create a second document and both documents work independently.

---

## Writeup division (4–6 pages each, shared intro/conclusion)

| Section | Owner |
|---|---|
| Introduction: collaborative editing problem | joint |
| Background: OT, Jupiter model, Paxos-as-sequencer | Aengus |
| System design: data model, API, protocol | Kathryn |
| OT implementation: transform rules, correctness proof sketch | Aengus |
| HTTP/SSE server and browser client | Kathryn |
| Testing: simulation-based, failure schedules, seed campaign | Aengus |
| Demo: real-world deployment and failure demo | Kathryn |
| Discussion: limitations, future work (CRDT, GOT for undo) | joint |
| References | joint |

---

## Key design decisions and tradeoffs

| Decision | Choice | Rationale |
|---|---|---|
| OT vs CRDT | OT (Jupiter model) | OT simpler to implement correctly; CRDT (Yjs-style) needs char IDs + tombstones — good future work |
| Sequencing mechanism | Paxos log order (may be reflected as PancyDB `version` **if** that matches global commit order — **confirm**) | Must match true commit order for replay; add explicit slot in op value if needed |
| Document storage | Op log in KV store, text reconstructed | Avoids hot-key CAS contention; gives full history for free |
| SSE vs WebSockets | SSE for server→client, HTTP POST for client→server | SSE fits Cotamer's HTTP model; full WebSockets harder to add, lower priority |
| Browser OT | JS port of C++ transform() | Cross-checks the C++ logic; no JS library dependency |
| Cursor tracking | PancyDB PUT + SSE | Reuses existing infrastructure; cursors are eventually consistent (fine) |

---

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| OT transform bugs | Randomized diamond-property test (A1 test 7) catches these fast |
| JS and C++ OT diverge | Run same test vectors through both; script this check |
| HTTP server integration takes longer than expected | Kathryn can demo with curl instead of browser if needed |
| Cotamer HTTP API is unfamiliar | Read existing HTTP handout code first; B1 (API design) is a safe first step |
| Running out of time | Follow **Scope tiers** above: ship Tier 1 first. Cut Tier 3 (cursors, multi-doc, huge `-R`), then shrink Tier 2 (curl-only demo, or static page without full OT in JS if you must). |

---

## Appendix: Course checklist, references, milestones

### Course checklist (reminder)

- **Track:** Application on Paxos (≈ ambition **4/5**): collaborative editing + **failure handling**.
- **Mandatory:** Automated tests; failures affecting **≥1 component**; define what “committed” means in tests.
- **Turnin:** Code, lab notebook, **4–6 page** writeup (Markdown or PDF) **per student**.
- **Groups:** Instructor **pre-approval** required; max 3.
- **Repo:** [github.com/kharper2/DoomDraft](https://github.com/kharper2/DoomDraft)

### References (related work — not a checklist to implement)

| Topic | Link |
|--------|------|
| Eg-walker | [arXiv:2409.14252](https://arxiv.org/pdf/2409.14252) |
| Differential synchronization | [neil.fraser.name/writing/sync](https://neil.fraser.name/writing/sync) |
| Jupiter | [ACM](https://dl.acm.org/doi/10.1145/215585.215706) |
| Yjs | [yjs.dev](https://yjs.dev) |
| Critiques / collab editing | [Moment part 1](https://www.moment.dev/blog/lies-i-was-told-pt-1), [part 2](https://www.moment.dev/blog/lies-i-was-told-pt-2) |

### Coarse milestones (assignment bar)

Use these as a sanity check alongside the detailed phases above:

- [ ] **A:** One client, full replica set, no failures → N ops → identical reconstructed text on all replicas.
- [ ] **B:** Two clients (or two simulated editors), no failures → interleaved ops → identical text everywhere.
- [ ] **C:** At least one **failure** model (crash/restart, partition, loss/delay per harness) → still identical survivors + no lost committed ops (per your definition).

### Open questions (fill in)

1. Topology: # replicas, how clients attach.
2. Idempotency / client `seq` if retries can duplicate proposals.
3. Instructor group approval: confirmed? Date?
