# Serial Work Breakdown: Collaborative Editor on Paxos

10 chunks, strictly ordered, alternating between partners. Each chunk is fully testable before the next begins.

---

## Chunk 1 — Person A (3 hrs): OT engine

**Files:** `doc_ops.hh`, `doc_ops.cc`

Implement `insert_op`, `delete_op`, `serialize()`, `deserialize()`, `apply()`, `transform()`, and `transform_seq()`. Also write a standalone `int main()` in `doc_ops.cc` that only activates under `#ifdef RUN_DOC_OPS_TESTS` and runs:
- All 4 transform cases with known inputs/outputs
- 1000 randomly generated op pairs verifying the diamond property: `apply(apply(doc, a), transform(b,a)) == apply(apply(doc, b), transform(a,b))`

**Done when:** compile `doc_ops.cc` standalone with `-DRUN_DOC_OPS_TESTS` and all tests pass. Person B can verify this before starting Chunk 2.

---

## Chunk 2 — Person B (3 hrs): Document state reconstruction

**Files:** `doc_state.hh`, `doc_state.cc`

Implement `committed_op`, `read_ops()`, `reconstruct()`. Also write a `test_doc_state()` function (called from `doc_state.cc` under `#ifdef RUN_DOC_STATE_TESTS`) that manually populates a `pancydb` with serialized ops (using `db.put()`), calls `read_ops()` and `reconstruct()`, and asserts the result matches a known string.

**Done when:** standalone test passes. No Paxos, no network simulation, no coroutines — just a `pancydb` and string operations.

---

## Chunk 3 — Person A (4 hrs): Collaborative client model

**Files:** `collab_model.hh`, `collab_model.cc`

Implement `collab_model` extending `client_model`. The editor coroutine submits random inserts/deletes through the Paxos request/response channels with redirect handling and timeout-based leader switching (pattern: copy from `lockseq_model.cc`). Implement `sync_committed_ops()` — poll for peer ops by key name, OT-transform pending ops against each new committed op. Implement `check()` — call `read_ops()` + `reconstruct()` on a live PancyDB and store the result for cross-replica comparison.

**Done when:** `collab_model` compiles without errors and the class structure matches the header. No test binary yet — Person B handles that in Chunk 4.

---

## Chunk 4 — Person B (3 hrs): `pt-collab` binary + basic correctness

**Files:** `pt-collab.cc`, `collab-bench.sh`

Copy the structure of `pt-paxos.cc`. Swap `lockseq_model` for `collab_model`. In `try_one_seed()`: after the simulation, call `collab::read_ops()` + `collab::reconstruct()` on each live replica's DB and assert all results match. Add a `--test` flag that calls the doc_ops and doc_state test suites and exits.

Write `collab-bench.sh` with this test matrix:

| Test | Flags | Goal |
|---|---|---|
| OT unit tests | `--test` | Diamond property, all transform cases |
| Basic convergence | `-R 100` | No divergence across seeds |
| Failover | `-f failover -R 50` | Document intact after leader death |
| Recovery | `-f recover -R 50` | Rejoining replica catches up |
| Split brain | `-f split -R 50` | Minority cannot corrupt document |
| Unstable | `-f unstable -R 30` | Convergence under churn |
| Torture | `-f torture -R 20` | Convergence under adversarial scheduling |
| High loss | `-l 0.15 -R 100` | Converges with 15% message loss |

**Done when:** `./pt-collab -R 100` passes and `./pt-collab --test` passes. Run this before handing off — bugs here are almost certainly in Chunk 3's sync logic, so Person A should be available to debug.

---

## Chunk 5 — Person A (3 hrs): Failure testing campaign

**Files:** no new files — run `collab-bench.sh` and fix bugs in `collab_model.cc`

Run every failure schedule from `collab-bench.sh`. The most likely failure mode: `sync_committed_ops()` misses an op under high message loss or after leader failover, causing reconstructed text to diverge. Fix bugs in `collab_model.cc` until all schedules pass their target seed counts.

**Done when:** all rows in the `collab-bench.sh` test matrix pass.

---

## Chunk 6 — Person B (4 hrs): HTTP server

**Files:** `http_server.hh`, `http_server.cc`

Expose the Paxos backend over HTTP using Cotamer's HTTP support. Implement these routes on top of a `pt_paxos_instance`:

| Method | Path | Response |
|---|---|---|
| `GET` | `/doc/<id>` | `{"text": "...", "version": N}` |
| `POST` | `/doc/<id>/op` | Submits op through Paxos; responds `{"version": N}` when committed |
| `GET` | `/doc/<id>/stream` | SSE stream; pushes each newly committed op as a JSON event |
| `POST` | `/doc/<id>/cursor` | Stores cursor position in PancyDB |
| `GET` | `/docs` | Lists document registry |

The `watch_ops()` coroutine polls the leader replica's PancyDB for ops with version > last broadcast and fans new ops out to all active SSE subscribers.

**Done when:** `curl localhost:8080/doc/main` returns the document, `curl -X POST localhost:8080/doc/main/op -d '{"type":"I","pos":0,"text":"hello"}'` commits and returns a version, and a second `curl localhost:8080/doc/main` shows the updated text.

---

## Chunk 7 — Person A (3 hrs): Browser editor

**Files:** `static/editor.html`, `static/editor.js`

Single HTML file, no build step, no dependencies. A `<textarea>` with an overlay `<div>` for peer cursors. The JS client:
- On load: `GET /doc/<id>` to fetch current text, set `serverVersion`
- Opens an `EventSource` to `/doc/<id>/stream`; for each incoming op, OT-transform pending ops against it, apply to textarea
- On `input` event: compute delta from last known text, submit as `POST /doc/<id>/op`, push to pending queue
- On cursor event from SSE: render colored cursor overlay

The JS `transform()` is a direct port of the C++ function — same 4 cases. Cross-check: run the same test vectors from Chunk 1 against the JS version.

**Done when:** two browser tabs on the same document show each other's edits in real time. Type in tab A, see it appear in tab B within one SSE event.

---

## Chunk 8 — Person B (2 hrs): Cursor tracking, multi-doc, server binary

**Files:** `pt-collab-server.cc`, update `http_server.cc`

Add cursor broadcast to the SSE stream (poll `doc/<id>/cursor/*` keys in `watch_ops()`). Add the document registry (`GET /docs`, `POST /docs`). Write `pt-collab-server.cc` as a thin main binary that starts 3 Paxos replicas and one HTTP server in the same process.

**Done when:** `./build/pt-collab-server --port 8080` starts, two browser tabs can collaboratively edit, cursor positions are visible, and you can create and switch between multiple documents.

---

## Chunk 9 — Person A (2 hrs): Writeup

Consensus/OT background, OT implementation, simulation testing results, failure analysis.

---

## Chunk 10 — Person B (2 hrs): Writeup

System architecture, HTTP/deployment design, browser demo, discussion/future work. Both partners share intro and conclusion.

---

## Contribution summary

| Chunk | Owner | Hours | Deliverable |
|---|---|---|---|
| 1 | A | 3 | OT engine + unit tests |
| 2 | B | 3 | Doc state reconstruction + tests |
| 3 | A | 4 | `collab_model` |
| 4 | B | 3 | `pt-collab` binary + correctness verified |
| 5 | A | 3 | All failure schedules passing |
| 6 | B | 4 | HTTP server, curl-verified |
| 7 | A | 3 | Browser editor, two-tab demo |
| 8 | B | 2 | Cursors, multi-doc, server binary |
| 9 | A | 2 | Writeup (A sections) |
| 10 | B | 2 | Writeup (B sections) |

**Person A: 15 hours. Person B: 14 hours.**

The key rule: each chunk's done criteria is a concrete, runnable test that the next person can independently verify before starting their chunk. If Chunk 4's 100-seed run fails, go back and fix Chunk 3 before moving forward — never carry bugs across a handoff.
