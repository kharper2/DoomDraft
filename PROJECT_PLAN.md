# CS 2620 Pset 4: Collaborative Document Editing on Paxos

**Ambition**: 4.5/5  
**Team**: 2 people (~15 hours each, ~30 hours total)  
**Deadline**: May 6 (demo), May 13 (final)

---

## One-paragraph description

Build a Google Docs–style collaborative text editor backed by the existing Paxos implementation. The system has two distinct layers. The **backend** (Person A) is a simulation-based test harness: multiple simulated editors submit character-level insert/delete operations through Paxos, which imposes a total order on all ops; Operational Transformation (OT) on the client side reconciles each editor's pending local edits against remotely committed ops; a `collab_model::check()` function verifies that all live replicas converge to identical document text under every failure schedule (failover, split-brain, partition, torture). The **frontend** (Person B) is a real HTTP/EventSource server that exposes the Paxos backend as a web API, with a browser-based editor where multiple real users can collaboratively edit in real-time over HTTP, with cursor/presence tracking and a document registry. The combination of rigorous simulation-based correctness testing and a working real-world deployment is what pushes this to the upper end of Ambition 4.

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

Document text is **never stored directly** — it is reconstructed by iterating all `doc/<id>/op/*` keys, sorting by PancyDB `version` (which equals the Paxos commit slot), and replaying ops in order. This gives deterministic reconstruction on every live replica.

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

## Person A: Backend, simulation, and testing (~15 hours)

**Owner**: correctness of the OT engine, simulation-based test harness, failure testing campaign.

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

**`check()` logic:**

```cpp
std::optional<std::string> collab_model::check(const pancy::pancydb& db) {
    auto ops = collab::read_ops(db, doc_id_);
    if (ops.empty()) return std::nullopt;  // nothing committed yet is fine
    // Verify op sequence has no gaps (every client_seq is contiguous)
    // Verify all ops are valid (non-negative positions, lengths in-bounds)
    auto text = collab::reconstruct(ops);
    // Store expected_text_ on first call; compare on subsequent calls
    if (expected_text_.empty()) {
        expected_text_ = text;
        return std::nullopt;
    }
    if (text != expected_text_) {
        return std::format("document mismatch: expected {} chars, got {}",
                           expected_text_.size(), text.size());
    }
    return std::nullopt;
}
```

Cross-replica convergence is checked in `try_one_seed()` in `pt-collab.cc`, which calls `collab::reconstruct()` on each live replica's DB and asserts all results match.

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

## Person B: HTTP server, browser UI, real-world demo (~15 hours)

**Owner**: real-world deployment, HTTP API, browser-based collaborative editor, cursor/presence, multiple documents.

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

### What both people do together (fits within individual hour budgets)

- **Joint design session** (1 hour): agree on op serialization format and API contract before coding.
- **Integration testing** (1 hour): run `pt-collab-server` + browser against the simulation's `pt-collab` correctness tests on the same document.
- **Cross-check OT**: verify JS `transform()` and C++ `transform()` produce identical results on the same test vectors.
- **Writeup** (each writes their own, ~3 hours each): Person A covers the OT + consensus sections; Person B covers the HTTP/deployment/demo sections. Both sections reference each other.

### Dependency order

```
Person A starts here        Person B starts here
      |                           |
  doc_ops.hh (A1)          API design (B1)   ← can be parallel
      |                           |
  doc_state (A2)           http_server (B2)  ← B2 needs doc_ops.hh
      |                           |
  collab_model (A3)        editor.js (B3)    ← can be parallel after API settled
      |                           |
  pt-collab.cc (A4)        pt-collab-server (B6)  ← B6 needs pt_paxos_instance
      |                           |
  Test campaign (A4)       Cursor support (B4) ← parallel
      |                           |
  Lab notebook (A5)        Multi-doc (B5)    ← parallel
```

Person B can start on B1 and B3 (API design + JS OT logic) before any of Person A's code is ready. Person B needs `doc_ops.hh` for the serialization format (available after A1, ~day 1 afternoon).

---

## File structure summary

```
pset3/
  doc_ops.hh / doc_ops.cc          ← Person A
  doc_state.hh / doc_state.cc      ← Person A
  collab_model.hh / collab_model.cc ← Person A
  pt-collab.cc                     ← Person A
  collab-bench.sh                  ← Person A
  http_server.hh / http_server.cc  ← Person B
  doc_http.hh / doc_http.cc        ← Person B
  pt-collab-server.cc              ← Person B
  static/
    editor.html                    ← Person B
    editor.js                      ← Person B
  CMakeLists.txt                   ← both (add two new targets)
```

All of `pancydb`, `netsim`, `client_model`, `pt_paxos_replica`, `cotamer`, `lockseq_model` are **unchanged**.

---

## What "done" looks like

**Simulation (Person A):**
- `./pt-collab --test` passes all OT unit tests including the randomized diamond property check.
- `./pt-collab -R 500` passes with no divergence.
- `./pt-collab -f failover`, `-f split`, `-f torture` all pass.
- `./pt-collab -l 0.15 -R 100` passes (high-loss correctness).

**Real-world (Person B):**
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
| Background: OT, Jupiter model, Paxos-as-sequencer | Person A |
| System design: data model, API, protocol | Person B |
| OT implementation: transform rules, correctness proof sketch | Person A |
| HTTP/SSE server and browser client | Person B |
| Testing: simulation-based, failure schedules, seed campaign | Person A |
| Demo: real-world deployment and failure demo | Person B |
| Discussion: limitations, future work (CRDT, GOT for undo) | joint |
| References | joint |

---

## Key design decisions and tradeoffs

| Decision | Choice | Rationale |
|---|---|---|
| OT vs CRDT | OT (Jupiter model) | OT simpler to implement correctly; CRDT (Yjs-style) needs char IDs + tombstones — good future work |
| Sequencing mechanism | Paxos commit slot (PancyDB version) | Free from existing infrastructure; no extra coordination needed |
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
| HTTP server integration takes longer than expected | Person B can demo with curl instead of browser if needed |
| Cotamer HTTP API is unfamiliar | Read existing HTTP handout code first; B1 (API design) is a safe first step |
| Running out of time | Simulation (Person A) is the mandatory core; HTTP + browser (Person B) is the "ambitious" layer. Can cut multi-doc if needed. |
