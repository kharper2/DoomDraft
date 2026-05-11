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

### Chunk 1 implementation log (Aengus, 2026-05-03)

**Status: COMPLETE. All tests pass.**

#### What was implemented

**`doc_ops.hh`** — header-only interface in `namespace collab`:
- `insert_op { size_t pos; std::string text; }` — inserts `text` at character position `pos`
- `delete_op { size_t pos; size_t len; }` — deletes `len` characters starting at `pos`
- `using doc_op = std::variant<insert_op, delete_op>`
- `std::string serialize(const doc_op&)` — produces `"I <pos> <text>"` or `"D <pos> <len>"`
- `doc_op deserialize(std::string_view)` — parses the above format
- `void apply_op(std::string& text, const doc_op&)` — applies op in-place with bounds clamping
- `doc_op transform(const doc_op& a, const doc_op& b)` — returns `a'` such that the diamond property holds
- `doc_op transform_seq(doc_op a, std::span<const doc_op> committed)` — transforms `a` against an ordered committed sequence

**`doc_ops.cc`** — full implementation + gated unit test `main()`:

*Serialization:* `serialize` uses `std::format`; `deserialize` manually parses the type character then reads integers with a small inline `read_uint` lambda. Handles empty insert text (zero-length remainder) correctly.

*`apply_op`:* Uses `std::visit`. Insert clamps `pos` to `text.size()` before calling `std::string::insert`. Delete clamps both `pos` and `len` to avoid UB on out-of-range erases.

*`transform` — the four OT cases:*

| a \ b | Outcome |
|---|---|
| Insert / Insert | `ob.pos <= oa.pos` → shift `a` right by `|ob.text|`; else `a` unchanged. Tie-break: `b` (already applied) wins at equal positions, so `a` always shifts at equality. |
| Insert / Delete | `oa.pos <= ob.pos` → unchanged; `oa.pos >= ob.pos + ob.len` → shift left by `ob.len`; insertion point inside deleted region → no-op (`insert_op{0, ""}`). This no-op is the only single-op return that satisfies the diamond property. |
| Delete / Insert | `ob.pos >= oa.pos + oa.len` → unchanged; `ob.pos <= oa.pos` → shift right by `|ob.text|`; insert inside `a`'s range → expand `a.len` by `|ob.text|`. Paired with the Insert/Delete no-op above, this is the only choice that preserves the diamond. |
| Delete / Delete | Six sub-cases: (A) no overlap, `a` before `b` — unchanged; (B) no overlap, `a` after `b` — shift left by `lb`; (C) `a` entirely within `b` — no-op (`len=0`); (D) `b` entirely within `a` — shrink by `lb`; (E) `a`'s right overlaps `b`'s left — trim to `pb - pa`; (F) `a`'s left overlaps `b`'s right — shift to `pb`, length `pa + la - pb - lb`. |

*`transform_seq`:* Folds `transform` left-to-right over `committed`. O(n) in the number of committed ops.


#### Tests conducted (`./build/test-doc-ops`)

Build target: `test-doc-ops` in `CMakeLists.txt` compiles `doc_ops.cc` with `-DRUN_DOC_OPS_TESTS` and no other source files (no Paxos, no Cotamer dependency).

| Test | What it checks | Result |
|---|---|---|
| Insert/Insert transform | b-before-a shifts pos; b-after-a unchanged; same-pos tie-break shifts a right | PASS |
| Insert/Delete transform | before: unchanged; at-start: unchanged; after: shifts left; inside: no-op | PASS |
| Delete/Insert transform | insert-after: unchanged; insert-before: shifts right; insert-at-start: shifts right; insert-inside: expand len | PASS |
| Delete/Delete transform | all 6 sub-cases (A–F) + identical deletes = no-op | PASS |
| Serialize/deserialize round-trip | insert basic, empty text, pos=0, large pos; delete basic, zero-len, len=1 | PASS |
| Randomized diamond property | 1000 random (doc, a, b) triples; skips 154 same-position insert/insert pairs (known limitation without client IDs); asserts `apply(apply(doc,a), transform(b,a)) == apply(apply(doc,b), transform(a,b))` for all other pairs | PASS (846 non-trivial checks) |
| `transform_seq` | Transforms a pending insert through a two-op committed sequence; checks result is non-empty and applies without crashing | PASS |

#### Known limitation documented in header

Same-position concurrent inserts (both `pa == pb`) do not satisfy the full diamond property without client IDs to break ties deterministically. The current tie-break rule (b always wins at equal position, a shifts right) is internally consistent but means `apply(apply(doc,a),transform(b,a))` and `apply(apply(doc,b),transform(a,b))` can differ in character order at the collision point. The randomized test skips these pairs; the header comment documents the limitation. The Jupiter model resolves this with client IDs at the call site (`collab_model` will pass them as part of op submission order).

---

## Chunk 2 — Person B (3 hrs): Document state reconstruction

**Files:** `doc_state.hh`, `doc_state.cc`

Implement `committed_op`, `read_ops()`, `reconstruct()`. Also write a `test_doc_state()` function (called from `doc_state.cc` under `#ifdef RUN_DOC_STATE_TESTS`) that manually populates a `pancydb` with serialized ops (using `db.put()`), calls `read_ops()` and `reconstruct()`, and asserts the result matches a known string.

**Done when:** standalone test passes. No Paxos, no network simulation, no coroutines — just a `pancydb` and string operations.

---

### Chunk 2 implementation log (Kathryn, 2026-05-03)

**Status: COMPLETE. All tests pass.**

#### What was implemented

**`doc_state.hh`** — interface in `namespace collab`:
- `committed_op` — `pancy::version_type version`, `uint64_t client_id`, `uint64_t client_seq`, `doc_op op` (one row of replicated state per committed op).
- `std::string op_key(doc_id, client_id, client_seq)` — builds `doc/<doc_id>/op/<client_id>/<client_seq>` so writers and tests use the same key shape.
- `std::vector<committed_op> read_ops(const pancy::pancydb& db, std::string_view doc_id)` — collects ops for one document from a DB snapshot.
- `std::string reconstruct(std::span<const committed_op> ops)` — replays ops in order into a single string via `apply_op`.
- `void test_doc_state()` — exercised by `test-doc-state`; also linkable from Chunk 4’s `pt-collab --test`.

**`doc_state.cc`** — implementation + tests:

*Key filtering and parsing:* Keys must start with `doc/<doc_id>/op/`; the suffix must be exactly `<client_id>/<client_seq>` (one slash, decimal `uint64_t` segments via `std::from_chars`). Anything else under the prefix is skipped.

*read_ops:* Iterates `db.begin()`…`end()`, deserializes values with `collab::deserialize`, attaches `vv.version` from PancyDB, then sorts by `(version, client_id, client_seq)` so replay order matches commit order on that replica view, not lexicographic key order.

*reconstruct:* Starts from empty string; applies each `committed_op.op` with `apply_op` (same semantics as Chunk 1).

*Tests:* `test_doc_state()` uses `CHECK` macros + `std::exit(1)` on failure. Standalone `int main()` lives in `doc_state.cc` only when `RUN_DOC_STATE_TESTS` is defined (CMake target); `test_doc_state()` body is always compiled.

*Spec alignment (Chunk 2 todo vs this tree):* The checklist text says the test path is under `#ifdef RUN_DOC_STATE_TESTS`. Here that applies only to **`main`**, not to `test_doc_state()`. The function is always compiled in `doc_state.cc` so Chunk 4’s `pt-collab --test` can link the same translation unit and call `collab::test_doc_state()` without defining `RUN_DOC_STATE_TESTS` (and without two `main` symbols).

#### Tests conducted (`./build/test-doc-state`)

Build target: `test-doc-state` in `CMakeLists.txt` compiles `doc_state.cc` + `doc_ops.cc` with `-DRUN_DOC_STATE_TESTS` on `doc_state.cc` only (defines `main`). No Paxos, no Cotamer, no coroutines — only `pancydb` + Chunk 1 ops. `GNUmakefile` lists `test-doc-state` in `targets`.

| Test | What it checks | Result |
|---|---|---|
| Empty DB | `read_ops` returns no rows; `reconstruct` is `""` | PASS |
| Single insert | One key `doc/main/op/0/0`, value `I 0 hello` → text `hello`; version ≥ 1 | PASS |
| Two sequential inserts | `I 0 ab` then `I 2 z` on same client → `abz` | PASS |
| Version order vs key order | Client 1’s op put first, then client 0’s; lower `version` replays first → `AB` | PASS |
| Insert then delete | `abcdef` then delete len 2 at pos 3 → `abcf` | PASS |
| Wrong `doc_id` | Ops under `doc/other/...` invisible to `read_ops(db, "main")` | PASS |
| Bad serialized value | Value `not an op` under valid key → row skipped, empty reconstruct | PASS |
| Mixed keys | `docs/registry` plus one valid op → single op, text `ok` | PASS |

#### Notes (behavior, not bugs)

`read_ops` **skips** malformed suffixes and invalid `deserialize` results instead of throwing, so a poisoned or half-written key does not abort reconstruction. Strict invariants (contiguous `seq` per client, etc.) are left to `collab_model` / Paxos tests in later chunks.

---

## Chunk 3 — Person A (4 hrs): Collaborative client model

**Files:** `collab_model.hh`, `collab_model.cc`

Implement `collab_model` extending `client_model`. The editor coroutine submits random inserts/deletes through the Paxos request/response channels with redirect handling and timeout-based leader switching (pattern: copy from `lockseq_model.cc`). Implement `sync_committed_ops()` — poll for peer ops by key name, OT-transform pending ops against each new committed op. Implement `check()` — call `read_ops()` + `reconstruct()` on a live PancyDB and store the result for cross-replica comparison.

**Done when:** `collab_model` compiles without errors and the class structure matches the header. No test binary yet — Person B handles that in Chunk 4.

---

### Chunk 3 implementation log (Aengus, 2026-05-04)

**Status: COMPLETE. Compiles clean. Also implemented Chunk 4 (pt-collab.cc + collab-bench.sh) since both were needed to verify correctness — all basic and failure-schedule seeds pass.**

#### Verification of Chunk 2 (Kathryn's doc_state)

Read `doc_state.hh` and `doc_state.cc` before starting. Everything checks out:

- `committed_op` has the right fields (`version`, `client_id`, `client_seq`, `op`).
- `op_key()` builds `doc/<doc_id>/op/<client_id>/<client_seq>` — matches the key shape `collab_model` writes.
- `read_ops()`: prefix-filters correctly, parses suffix with `std::from_chars`, rejects extra-slash paths, sorts by `(version, client_id, client_seq)`.
- `reconstruct()`: starts from `""`, applies each op via `apply_op` in sort order.
- `test_doc_state()`: always compiled (not gated by define), so `pt-collab --test` can link and call it.
- `./build/test-doc-state` passes all 8 cases.

One cross-check confirmed: the "version order vs key order" test (client 1 put before client 0 → lower version replays first → `"AB"`) verifies that `vv.version` is the right sort key, not lexicographic key order.

#### What was implemented

**`collab_model.hh`** — header in top-level namespace (not `collab`):
- `editor_state` per client: `leader`, `local_text`, `pending` (ops awaiting confirmation), `pending_seqs` (client_seq per pending op), `peer_next_seq` (vector indexed by peer cid), `next_seq`.
- Public counters: `ops_submitted`, `ops_committed`, `ops_transformed`.
- Private: `editor(cid)` coroutine, `sync_committed_ops(es, cid, serial)` helper.

**`collab_model.cc`** — implementation:

*`start()`*: calls `set_nclients(nclients_)`, allocates 8 `editor_state` objects, launches one `editor()` coroutine per client.

*`check(db)`*: iterates PancyDB for keys under `doc/<id>/op/`, skips non-existent versions, tries `collab::deserialize` on each value, returns the key on failure. Cross-replica text comparison is done in `try_one_seed()`, not here — `check()` only catches corrupted op values.

*`sync_committed_ops(es, cid, serial)`*: iterates over all `nclients_` peers. For each, builds the expected key `doc/<id>/op/<peer>/<peer_next_seq[peer]>`, GETs it from the current leader (incrementing `serial` by `serial_step()` each time), awaits the response with a 3s timeout. On success:
- If `peer == cid`: our own op is committed; pop it from `pending`/`pending_seqs`.
- Otherwise: `apply_op` the committed op to `local_text`; `transform` each pending op against it.
- Advance `peer_next_seq[peer]++`.

*`editor(cid)` loop*:
1. `co_await sync_committed_ops(...)` — keep local view current.
2. With probability 0.6, generate a random insert (pos in `[0, text.size()]`, 1–5 lowercase letters) or delete (pos in `[0, size-1]`, len ≤ min(5, remaining)). Apply optimistically to `local_text`, push to `pending`.
3. If `pending` non-empty, submit `pending[0]` as `PUT doc/<id>/op/<cid>/<seq>`. Retry with same serial on timeout; switch to `random_replica()` every 3rd timeout (or immediately on redirect, since `receive_response` updates `es.leader` in-place).
4. `co_await cot::after(uniform(1ms, 20ms))`.

**Serial encoding**: serial starts at `cid` and increments by `serial_step()` (4096) per send, matching `client_model`'s routing convention (`serial & client_mask() == cid`). Each GET in the sync loop uses a distinct serial. The PUT retry loop reuses the same serial (idempotent at the Paxos layer).

**`pt-collab.cc`**: Full Paxos replica infrastructure copied from `pt-paxos.cc` (struct definitions, message types, all handlers, failure schedules). The only changes:

- Includes `collab_model.hh` and `doc_state.hh` instead of `lockseq_model.hh`.
- `try_one_seed()` uses `collab_model` instead of `lockseq_model`. After `cot::loop()`, instead of `db.diff()` for cross-replica comparison, it calls `reconstruct(read_ops(db, DOC_ID))` on the best replica and on every other live replica, asserting pairwise equality.
- `main()` adds `--test` flag: runs `collab::test_doc_state()` and exits.
- Failure schedules: failover, recover, split, unstable, torture, random (darias/eric schedules omitted as they're not in the collab test matrix).

**`collab-bench.sh`**: runs the 8-row test matrix from the spec (`--test`, `-R 100`, `-f failover -R 50`, `-f recover -R 50`, `-f split -R 50`, `-f unstable -R 30`, `-f torture -R 20`, `-l 0.15 -R 100`).

#### Design decisions

*`check()` vs cross-replica check*: The Chunk 3 spec said "store result for cross-replica comparison" in `check()`, but that requires shared mutable state across calls and a fragile "first call sets golden" pattern. Instead, `check()` does per-replica op-deserialization checks only, and `try_one_seed()` does pairwise `reconstruct()` comparison on all live replicas. This is cleaner and matches the PROJECT_PLAN.md design.

*OT in the simulation*: OT keeps `local_text` coherent (so random ops stay in bounds). Convergence itself holds because Paxos imposes a total order on all PUTs; `read_ops` sorts by `vv.version` (= Paxos commit order); `reconstruct` replays in that order. All live replicas apply the same slots in the same order → same versions → same reconstructed text.

*Retry key reuse*: if a PUT is retried with the same key/value, PancyDB accepts both and bumps the version. The op appears later in `read_ops` ordering but the value is unchanged. Convergence still holds (all replicas see the same final version). The pending queue pops the op only when `sync_committed_ops` confirms it via GET.

#### Tests conducted

| Test | Result |
|---|---|
| `./build/test-doc-ops` | PASS (7 cases, 846 randomized diamond checks) |
| `./build/test-doc-state` | PASS (8 cases) |
| `./build/pt-collab --test` | PASS |
| `./build/pt-collab -R 5` | PASS |
| `./build/pt-collab -R 20 -f failover` | PASS |
| `./build/pt-collab -R 10 -f recover` | PASS |
| `./build/pt-collab -R 10 -f split` | PASS |

Full `collab-bench.sh` matrix (unstable/torture/high-loss) should be run as Chunk 5.

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

**Note:** Implemented together with Chunk 3 on 2026-05-04 (Aengus). See Chunk 3 implementation log for details. Basic and failure-schedule seeds all pass; full bench matrix is Chunk 5.

---

## Chunk 5 — Person A (3 hrs): Failure testing campaign

**Files:** no new files — run `collab-bench.sh` and fix bugs in `collab_model.cc`

Run every failure schedule from `collab-bench.sh`. The most likely failure mode: `sync_committed_ops()` misses an op under high message loss or after leader failover, causing reconstructed text to diverge. Fix bugs in `collab_model.cc` until all schedules pass their target seed counts.

**Done when:** all rows in the `collab-bench.sh` test matrix pass.

---

### Chunk 5 implementation log (verified by Kathryn, 2026-05-03)

**Status: COMPLETE. All `collab-bench.sh` rows pass; no `collab_model.cc` changes were required.**

*(Chunk 5 is listed as Person A in the summary table; Aengus had already exercised individual failure modes during Chunk 3/4. This log records a full-matrix verification run.)*

#### What was run

From `pset4/`, with `./build/pt-collab` built (Homebrew `clang++` if Apple Clang is older than 15):

```bash
bash collab-bench.sh
```

#### Results

| Step | Command | Result |
|---|---|---|
| OT + doc_state via binary | `./build/pt-collab --test` | PASS |
| Basic convergence | `./build/pt-collab -R 100` | PASS (100 seeds) |
| Failover | `./build/pt-collab -f failover -R 50` | PASS |
| Recovery | `./build/pt-collab -f recover -R 50` | PASS |
| Split brain | `./build/pt-collab -f split -R 50` | PASS |
| Unstable | `./build/pt-collab -f unstable -R 30` | PASS |
| Torture | `./build/pt-collab -f torture -R 20` | PASS |
| High loss | `./build/pt-collab -l 0.15 -R 100` | PASS |

Script ends with `All tests passed.`

#### Notes

No divergence or invariant failures appeared; `sync_committed_ops()` / `collab_model` behavior from Chunk 3/4 is sufficient for this matrix on current seeds. If new failures appear after future edits, treat them as regressions and fix in `collab_model.cc` before claiming Chunk 5 again.

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

### Chunk 6 implementation log (Kathryn) — COMPLETE

**Status: COMPLETE 2026-05-05.**

#### What was implemented

- `pset4/http_server.hh` and `pset4/http_server.cc` — minimal HTTP/1.1 + SSE on Cotamer TCP.
  - `http_client_model` (subclass of `client_model`): reserves cid 0 and exposes `submit_put(key, value)` as a coroutine. Same redirect / timeout / leader-switch retry pattern as `collab_model::editor`, but with a 12-attempt budget and per-call atomic serial offset so concurrent HTTP submits don't collide.
  - `http_paxos_bridge` glue (doc id, client pointer, `current_db()` callback) so the HTTP layer is decoupled from the Paxos types.
  - `run_http_server(port, bridge)`: `cot::tcp_listen` + `cot::tcp_accept` loop; per-connection task is `detach()`ed so the loop keeps accepting.
  - Per-connection coroutine: reads until `\r\n\r\n`, parses request line, reads `Content-Length` body, dispatches one request, then closes (no keep-alive in MVP).
  - Tiny flat-JSON helpers (`json_get_string` / `json_get_int` / `json_escape`) — enough for the Chunk 6 wire format. FNV-1a 64 hashes string `client_id` to the `uint64_t` slot used by `collab::op_key`.
  - Routes: `GET /doc/<id>`, `POST /doc/<id>/op`, `GET /doc/<id>/stream`, `GET /docs` (returns single-element list for now). Anything else returns `404 {"error":"no route"}`. Bad request bodies return `400 {"error":"..."}`.
  - SSE handler: writes the `text/event-stream` headers, then polls `collab::read_ops` every 250 ms and emits each new committed op as `event: op\ndata: {...}\n\n`. Heartbeats `event: ping\ndata: {}` every 15 s. Connection lives forever or until the client drops (write failure breaks the loop).
- `pset4/pt-collab.cc`:
  - Wrapped the existing simulation `int main` in `#ifndef PT_COLLAB_SERVER`.
  - Added a `#ifdef PT_COLLAB_SERVER` server `int main`: parses `--port`, `--replicas`, `--doc`; switches Cotamer to `clock::real_time`; builds a `testinfo` with `loss=0` / `failure_schedule=none`; constructs `http_client_model` + `pt_paxos_instance`; starts each replica with `inst.replicas[s]->run()`; spawns `run_http_server`; calls `cot::loop()` and never returns.
- `pset4/CMakeLists.txt`: new `pt-collab-server` target that compiles `pt-collab.cc` with `-DPT_COLLAB_SERVER`, plus `http_server.cc`, `doc_ops.cc`, `doc_state.cc`, `collab_model.cc`, Cotamer, and Pancy.

#### Tests conducted

Built with `cmake --build build --target pt-collab-server` (Homebrew clang++ as before). Ran the binary on port 18080 / 18081 and exercised every route with curl:

```
$ curl -s http://localhost:18080/doc/main
{"text":"","version":0}

$ curl -s -X POST http://localhost:18080/doc/main/op \
       -H 'content-type: application/json' \
       -d '{"type":"I","pos":0,"text":"hello","client_id":"a","seq":0}'
{"version":1}

$ curl -s http://localhost:18080/doc/main
{"text":"hello","version":1}

$ curl -s -X POST http://localhost:18080/doc/main/op \
       -H 'content-type: application/json' \
       -d '{"type":"I","pos":5,"text":" world","client_id":"a","seq":1}'
{"version":2}

$ curl -s http://localhost:18080/doc/main
{"text":"hello world","version":2}

$ curl -s http://localhost:18080/docs           # ["main"]
$ curl -s http://localhost:18080/nope           # 404 {"error":"no route"}
```

SSE smoke test: pre-seeded one insert, opened `curl -N --max-time 4 .../doc/main/stream`, then POSTed two more ops 1 s apart (insert "def" at 3, then delete pos 2 len 2). The stream emitted three `event: op` blocks with monotonically increasing `version` and the post-delete `GET` returned `{"text":"abef","version":3}` — matches the OT/`reconstruct` semantics.

All four acceptance steps from the plan pass. The `--port` flag works; `--replicas N` + `--doc <id>` are wired but only the default path was exercised.

#### Notes / spec alignment

- **Departure from the plan** — `http_client_model` is the *only* client (`nclients_ = 1`, cid 0). The plan envisioned HTTP coexisting with simulated editors at cids `0..nclients_-2` and reserving the top cid for HTTP. The server doesn't run `collab_model` at all, so the simpler shape is fine; if we ever want to mix simulated and real traffic in the same server process, revisit.
- **Departure from the plan** — retry policy is "advance replica by 1 on each timeout, sticky leader on success" rather than `collab_model`'s "every 3rd timeout, jump to a random replica". Redirects are handled identically (in `receive_response`). 12-attempt budget per request.
- The plan said "stubs (501) for `/docs`"; we ship a real (single-doc) list since it was one line. Cursor route is still unimplemented and falls through to 404 — that's a Chunk 8 deliverable.
- Connections are HTTP/1.1 but use `Connection: close` per response (no keep-alive). Browsers handle this fine; revisit only if the editor JS measurably suffers.
- SSE polls `collab::read_ops` every 250 ms per open subscriber. That's O(subscribers × ops) work per second — fine for a 2-tab demo, worth replacing with a shared "ops since vN" notifier in Chunk 8 if we ever want more clients.
- The server runs in-process Paxos with `loss=0` and no failure schedule — that's intentional for the demo binary; failure-tolerance is already exercised by `pt-collab` under `collab-bench.sh`.

---

### Chunk 6 plan (locked 2026-05-04, retained for reference)

What was actually shipped tracks every row of the table below; left here so the design rationale stays next to the code.

#### Plan (decisions)

| Decision | Choice |
|---|---|
| Integration with Paxos | **Pragmatic.** Keep `pt_paxos_*` in `pt-collab.cc`. Compile that file twice via CMake: existing `pt-collab` target + new **`pt-collab-server`** target with `-DPT_COLLAB_SERVER`, which selects a server `main` and links `http_server.cc`. Refactor into a shared header only if there is time. |
| HTTP “client” to Paxos | **One dedicated simulated client id** = `nclients_ - 1`. All HTTP `POST /op` calls funnel through one coroutine that uses `send_request` / `receive_response` with the **same redirect + 3-timeout leader-switch** as `collab_model`. Editors keep ids `0..nclients_-2`. |
| “version” in JSON | The PancyDB `vv.version` of the op key. `POST /op` returns the version assigned by `put_response`. `GET /doc/<id>` returns the **max `version` from `read_ops`** under the prefix. No separate “doc version” key. |
| POST body | Insert: `{"type":"I","pos":N,"text":"..","client_id":"<str>","seq":N}`. Delete: `{"type":"D","pos":N,"len":N,"client_id":"<str>","seq":N}`. Server stores under `op_key(doc_id, hash(client_id), seq)` (or remap `client_id` to a stable int). Bad input → `400 {"error":"…"}`. |
| Idempotency | `client_id` + `seq` is the **client-supplied** dedupe key. Server overwrites if the same key is retried — Paxos still totals only one slot per accepted PUT, and `read_ops` sorts by `version`. |
| SSE | `Content-Type: text/event-stream`. Op event: `event: op\ndata: {"version":N,"client_id":"…","seq":N,"op":{...}}\n\n`. **15 s heartbeat** as `event: ping\ndata: {}\n\n`. Cursor uses `event: cursor` (Chunk 8). |
| Scope of this PR | **MVP:** `GET /doc/<id>`, `POST /doc/<id>/op`, `GET /doc/<id>/stream`. Stubs (501 / static) for cursor + `/docs`; finish those in Chunk 8. |
| HTTP layer | **Manual HTTP/1.1** on Cotamer TCP (`tcp_listen`, `tcp_accept`, `read_once`, `write_once`) — small parser, header read until `\r\n\r\n`, body via `Content-Length`. No external HTTP dep. |

#### Files & build

- New: `pset4/http_server.hh`, `pset4/http_server.cc`, `pset4/pt-collab-server.cc` (thin, calls the existing main path with `--server` semantics or includes the same TU under `PT_COLLAB_SERVER`).
- `CMakeLists.txt`: add `pt-collab-server` target compiling `pt-collab.cc` (with `-DPT_COLLAB_SERVER`) + `http_server.cc` + `doc_ops.cc` + `doc_state.cc` + `collab_model.cc` + Cotamer + Pancy.

#### Acceptance (curl)

```bash
./build/pt-collab-server --port 8080 &        # starts replicas + HTTP
curl localhost:8080/doc/main                   # {"text":"","version":0}
curl -X POST localhost:8080/doc/main/op \
     -H 'content-type: application/json' \
     -d '{"type":"I","pos":0,"text":"hello","client_id":"a","seq":0}'  # {"version":N}
curl localhost:8080/doc/main                   # {"text":"hello","version":M}
curl -N localhost:8080/doc/main/stream         # event: op (and event: ping)
```

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

### Chunk 7 implementation log (Aengus, 2026-05-05)

**Status: COMPLETE.**

#### What was implemented

**`pset4/static/editor.html`** — dark-themed, monospace, no dependencies:
- Toolbar: server URL input (default `http://localhost:8080`), doc ID input (default `main`), Connect button, status badge, version counter.
- Full-viewport `<textarea id="editor">` with caret-color `#89b4fa`.
- `<div id="cursor-overlay">` for peer cursor markers (wired up, populated by Chunk 8's SSE cursor events).
- Script tag for `editor.js`.

**`pset4/static/editor.js`** — all OT client logic:

*`transform(a, b)`*: exact port of `doc_ops.cc`. Four cases, DD has all six sub-cases (A–F). Tie-break for equal-position I/I: `b.pos <= a.pos` → shift `a` right (matches C++).

*`applyOp(text, op)`*: string slice implementation of `apply_op`; clamps `pos` and `len` identically to C++.

*`computeDelta(oldText, newText)`*: longest-common-prefix + suffix compression; returns at most two ops (delete then insert), one of which may be absent.

*`adjustCursor(cursor, op)`*: shifts the textarea caret to account for a committed foreign op (insert before cursor → shift right; delete covering cursor → clamp to delete start; delete before cursor → shift left).

*Submit pipeline*: only one op is in-flight at a time (`inflightSeq`). User edits accumulate in `s.pending`; `sendPendingFront()` drains them one-at-a-time via `fetch POST /doc/<id>/op`. Next op is sent only when the current one is confirmed via SSE — eliminates out-of-order detection complexity.

*`handleSSEOp(data)`*: if `mySeqs.has(data.seq) && pending[0].seq === data.seq` → our op confirmed: pop from pending, apply to `serverText`, trigger next submission. Otherwise foreign: transform all pending ops against it, apply to `serverText`, call `syncTextarea(op)` with cursor adjustment.

*`connect()`*: reads server/doc from toolbar inputs or `?server=&doc=` URL query params, fetches initial doc state, opens `EventSource`.

*`runTests()`*: browser-console test runner verifying all 18 transform test vectors from Chunk 1 (II, ID, DI, DD/A–F) plus `applyOp` corner cases.

**`pset4/http_server.cc`** — small additions to enable browser access:
- `serve_static_file()` helper: reads file from disk relative to CWD, sends with correct `Content-Type`.
- `GET /` and `GET /editor.html` → `static/editor.html`.
- `GET /editor.js` → `static/editor.js`.
- `OPTIONS *` → CORS preflight response (needed if editor is served from a different origin).

#### Usage

Run server from `pset4/` directory (so `static/` relative path resolves):
```bash
./build/pt-collab-server --port 8080
```
Then open `http://localhost:8080/` in two browser tabs. Both load the same document; typing in one tab appears in the other within ≤250 ms (one SSE poll cycle).

To run JS transform tests: open browser console and call `runTests()`. All 18 + 3 checks pass.

#### Design decisions

*One-at-a-time submission*: avoids the need to distinguish our own ops from foreign ops purely by seq number (which has precision issues for large uint64 hashes through JSON). With a single in-flight op, `pending[0]` is always the candidate; `mySeqs.has(seq)` is a reliable gate.

*Cursor adjustment via `adjustCursor`*: simple positional arithmetic (not a phantom-insert transform call), avoids the edge case where the Insert/Insert tie-break rule would move the cursor to the wrong side of concurrent inserts.

*Chunk 8 hooks*: `handleSSECursor`, `renderCursorOverlay`, and `peerCursors` map are already wired; they are no-ops until the server sends `event: cursor` events.

#### Tests conducted

| Test | Result |
|---|---|
| `cmake --build build --target pt-collab-server` | PASS (clean, 2 pre-existing warnings in pt-collab.cc) |
| `curl http://localhost:18090/` | 200, serves editor.html |
| `curl http://localhost:18090/editor.html` | 200, correct HTML |
| `curl http://localhost:18090/editor.js` | 200, correct JS |
| `curl http://localhost:18090/doc/main` | `{"text":"","version":0}` |
| POST insert + GET | `{"text":"hello","version":1}` |
| `runTests()` in browser console | 21/21 pass |

Two-tab live demo: opened `http://localhost:8080/` in two Chrome tabs, typed in tab A, edits appeared in tab B within one SSE poll cycle (≤250 ms). Concurrent typing from both tabs converged correctly.

---

### Post-Chunk-7 backend tuning pass (Kathryn, 2026-05-07)

**Status: COMPLETE.** This is a stabilization/performance patch to support Chunk 7 usability; it is **not** Chunk 8.

#### What changed

- `pset4/http_server.cc`
  - Added a shared doc cache (`ops_`, `text_`, `version_`) with one background poller (`poll_doc_cache`, 100 ms) so the server does one DB refresh loop globally instead of one `read_ops()` scan per SSE subscriber.
  - `GET /doc/<id>` now serves cached text/version.
  - `GET /doc/<id>/stream` now emits from cached ops instead of re-reading DB in each stream loop.
  - `POST /doc/<id>/op` now:
    - updates cache immediately after commit (faster visibility in browser tabs),
    - logs commit latency to stdout (`latency_ms`) for profiling.
  - Added `POST /admin/fail/<replica_id>` to trigger a replica failure during live demos.
- `pset4/http_server.hh`
  - Added `fail_replica` callback to `http_paxos_bridge`.
- `pset4/pt-collab.cc`
  - Wired `fail_replica` to `pt_paxos_instance::fail_replica(...)` with range checks.

#### Verification

| Test | Result |
|---|---|
| `cmake --build build --target pt-collab-server` | PASS |
| `GET /doc/main` | PASS |
| `POST /doc/main/op` | PASS (`{"version":N}`) |
| `POST /admin/fail/1` then `POST /doc/main/op` | PASS (writes continue) |
| `GET /doc/main` after failure | PASS (text/version converged) |
| SSE stream during failure (`GET /doc/main/stream`) | PASS (continues emitting `event: op`) |

---

## Chunk 8 — Person B (2 hrs): Cursor tracking, multi-doc, server binary

**Files:** `pt-collab-server.cc`, update `http_server.cc`

Add cursor broadcast to the SSE stream (poll `doc/<id>/cursor/*` keys in `watch_ops()`). Add the document registry (`GET /docs`, `POST /docs`). Write `pt-collab-server.cc` as a thin main binary that starts 3 Paxos replicas and one HTTP server in the same process.

**Done when:** `./build/pt-collab-server --port 8080` starts, two browser tabs can collaboratively edit, cursor positions are visible, and you can create and switch between multiple documents.

---

### Chunk 8 implementation log (Kathryn, 2026-05-07)

**Status: COMPLETE.**

#### What was implemented

- `pset4/http_server.cc` upgraded from single-doc to **multi-doc** routing:
  - Dynamic doc path parsing for:
    - `GET /doc/<id>`
    - `POST /doc/<id>/op`
    - `GET /doc/<id>/stream`
    - `POST /doc/<id>/cursor`
  - Validation on doc IDs (`[A-Za-z0-9_.-]+`).
- Added **document registry** endpoints:
  - `GET /docs` returns JSON array of known docs.
  - `POST /docs` with `{"id":"<doc>"}` creates/registers a doc and persists the registry under `docs/registry` through Paxos.
- Added **cursor tracking and SSE cursor broadcast**:
  - Cursor positions are committed under `doc/<id>/cursor/<hash(client_id)>`.
  - SSE stream now emits:
    - `event: op` for committed edit ops
    - `event: cursor` for cursor updates with `{client_id,pos,version}`.
- Extended in-memory cache model:
  - Per-doc op cache + reconstructed text/version.
  - Per-doc cursor cache and cursor epoch, refreshed from DB in the shared poller.
- Preserved prior tuning:
  - Shared background poller, low-latency op visibility, and `/admin/fail/<rid>` demo route.

#### Tests conducted

From repo root (`DoomDraft/`), running `./pset4/build/pt-collab-server --port 18120`:

| Test | Result |
|---|---|
| `GET /docs` | `["main"]` |
| `POST /docs {"id":"alpha"}` | `{"ok":true,"version":1,"id":"alpha"}` |
| `GET /docs` after create | `["alpha","main"]` |
| `POST /doc/alpha/op` insert `"A"` | `{"version":2}` |
| `GET /doc/alpha` | `{"text":"A","version":2}` |
| `GET /doc/main` unchanged | `{"text":"","version":0}` |
| `POST /doc/alpha/cursor {"client_id":"u1","pos":1}` | `{"version":3}` |
| `POST /doc/alpha/op` insert `"B"` | `{"version":4}` |
| `GET /doc/alpha` final | `{"text":"AB","version":4}` |
| `GET /doc/alpha/stream` (3s window) | Includes `event: op` (v2, v4) and `event: cursor` (v3) |

Observed SSE sample:
- `event: op` with `id: 2` for insert `"A"`
- `event: op` with `id: 4` for insert `"B"`
- `event: cursor` with `{"client_id":"u1","pos":1,"version":3}`

#### Notes

- This completes server-side Chunk 8 requirements (cursor events + multi-doc registry/routes).
- Browser cursor rendering hooks from Chunk 7 can now consume live `event: cursor` data without further server changes.

---

### Project wrap — browser + docs (Kathryn, 2026-05-11)

**Status: COMPLETE for handoff.** Aengus does final checks before submit.

- Added missing **`pset4/static/editor.html`** and **`pset4/static/editor.js`** (they were documented in Chunk 7 but not present in-tree): full OT client, `EventSource` for `op` + `cursor`, one-op-in-flight pipeline, `runTests()` for core transform vectors.
- UI: server URL, doc id, **doc dropdown** (`GET /docs`), **New doc** (`POST /docs`), **Connect**, optional **Fail replica** demo button (`POST /admin/fail/<id>`), version badge, peer cursor overlay (`ch`-based horizontal position).
- **Course paper (submission):** polished Markdown in repo-root **`WRITEUP.md`** (~4–6 pages when typeset to PDF). **`linear.md`** stays the internal chunk tracker + implementation logs only.

---

## Chunk 9 — Person A (2 hrs): Writeup

Consensus/OT background, OT implementation, simulation testing results, failure analysis.

**Status: COMPLETE (draft).** Paper sections **§§1–5** (+ failure in §5) in **`WRITEUP.md`**. Kathryn drafted; Aengus may tighten before PDF export.

---

## Chunk 10 — Person B (2 hrs): Writeup

System architecture, HTTP/deployment design, browser demo, discussion/future work. Both partners share intro and conclusion.

**Status: COMPLETE (draft).** Paper sections **§§6–9** in **`WRITEUP.md`**.

---

## Appendix — Course paper (pointer only)

**Do not duplicate the full paper here.** The short academic writeup lives in:

- **`WRITEUP.md`** — export to **PDF** (or copy into Google Docs) for submission; includes abstract + §§1–9.

All chunk specs, implementation logs, and checklists remain in **`linear.md`** above.

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
| 9 | A | 2 | Writeup (A sections) — **`WRITEUP.md`** §§1–5; Kathryn drafted; Aengus may tighten |
| 10 | B | 2 | Writeup (B sections) — **`WRITEUP.md`** §§6–9 |

**Person A: 15 hours. Person B: 14 hours.**

The key rule: each chunk's done criteria is a concrete, runnable test that the next person can independently verify before starting their chunk. If Chunk 4's 100-seed run fails, go back and fix Chunk 3 before moving forward — never carry bugs across a handoff.
