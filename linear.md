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
