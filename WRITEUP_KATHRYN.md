# DoomDraft: Collaborative Text Editing on Replicated Paxos

Harvard CS 2620 — Problem Set 4 (distributed systems with failure).  
Writeup: Kathryn Harper, with Aengus (joint implementation; who did what lives in `labnotebook.md`).

---

## Abstract

We built DoomDraft, a collaborative text editor whose document state is replicated through Paxos-backed Pancydb. The key design is simple: Paxos gives all replicas one committed operation order, while the browser uses OT only to keep local unacknowledged edits usable.

Clients submit insert and delete operations over HTTP; the server commits each operation under a monotonic Paxos version and broadcasts the resulting log to subscribers with Server-Sent Events (SSE). Our browser client keeps a confirmed prefix and a pending queue, and applies operational transformation (OT) against foreign commits in the Jupiter style: one totally ordered commit stream from Paxos, OT only for work not yet acknowledged. We argue correctness with layered tests: standalone OT and document-reconstruction binaries, a Cotamer-based Paxos plus multi-editor simulation under explicit failure schedules, and `collab-bench.sh` over many random seeds. `pt-collab-server` exposes the same commit path over real TCP to a static HTML/JS editor, plus `/admin/fail` to kill a simulated replica for demos. Integrating a newer Cotamer and running the server on real wall-clock time changed how simulated delay and browser fan-out interact with liveness; we document what we saw and the knobs we used (`labnotebook.md`) instead of pretending the web demo scales without bound.

---

## 1. Introduction

The course asks for Paxos-backed replicated state that survives failures, plus automated tests that actually stress failure—not a happy-path demo alone.

### 1.1 Goals and motivation

Aengus and I live in Google Docs and similar tools for essays, notes, assignments, etc. We wanted to strip away the polish for a moment and see what has to be true when two people type in the same buffer: where does a single agreed order for committed edits come from, and what does the client still have to fix locally before those commits arrive? The handout gave us permission to build that story end-to-end instead of only staring at Paxos pseudocode.

We also wanted one repo we could argue over. Paxos on a page and Paxos inside Cotamer are different animals; it helped to pair-debug leader redirects, serial routing, and misalignment of replicas in real time. We still split chunks for throughput (see `labnotebook.md`), but we aligned on interfaces early so we were debugging one system, not gluing two homeworks at the end.

That steered us to the collaborative-editing ambition level: a browser you can open in two tabs, cursors, multi-doc registry, and a fat `pt-collab` harness that throws partitions and flaky links at many simulated editors. A thin KV layer would have been faster to finish; it would not have answered the question we cared about—how shared editing *feels* when the log and the network misbehave under you.

### 1.2 What we built (contributions)

- A mapping from character-level insert/delete ops to versioned Paxos puts: deterministic keys, replay in global version order, idempotent `(doc, client, seq)` at the storage layer.
- A browser client that converges against a live server: OT on pending work, SSE for commits (and cursors), `POST` acks so typing is not hostage to SSE ordering alone, optional `GET/POST /docs` for document discovery.
- `pt-collab` + `collab_model`: concurrent simulated editors on the same Paxos core as the course exercises, with schedules for failover, recovery, split, unstable, torture, and random choice among them—`collab-bench.sh` hammers all of that on many seeds.
- A clean split in our heads (and in the code) between bench time—virtual Cotamer clock, seconds-long campaigns—and server time—`cot::clock::real_time`, real TCP, wall-clock `netsim` delays—so we do not confuse passing the harness with feeling snappy in Chrome with eight tabs.

### 1.3 Out of scope

We did not build CRDTs, WebSockets, Byzantine tolerance, or a hardened admin surface. This is a course system: instructive first, internet-facing never.

---

## 2. System architecture

This section is mostly operational mechanics; we keep it close to the code paths we actually ship.

### 2.1 Data model and Paxos mapping

Each document is a text string edited by an append-only log of insert and delete operations. Committed operations are stored in Pancydb under keys of the form

`doc/<doc_id>/op/<client_id_uint64>/<seq>`.

The HTTP server maps the browser’s string `client_id` from JSON to a 64-bit identifier with `hash_client_id` in `http_server.cc` before forming `op_key` in `doc_state.cc`; the standalone `test-doc-state` path uses small integer client ids in the same key layout. Each `(doc, client id, seq)` triple is treated idempotently at the storage layer. Each committed row carries a Paxos version; the in-memory document cache applies operations in version order so that reconstructed text matches global commit order even when lexical key order would interleave different clients.

Document discovery uses `GET /docs` and `POST /docs` with a JSON body `{"id":"..."}`; the registry itself is replicated as a Paxos-backed value so new document ids survive across replicas (see `http_server.cc` and the `kDocsRegistryKey` path in the implementation).

### 2.2 Server: HTTP, SSE, and Paxos bridge

The `pt-collab-server` binary instantiates three in-process Paxos replicas with the course’s leader-based replicated-log design (Raft-shaped control flow in the handout code), together with a simulated Paxos client bridge so HTTP handlers can submit puts and observe versions without blocking the TCP accept loop incorrectly. Notable routes (see `http_server.cc`):

- `GET /doc/<id>` — JSON snapshot of current text and version from the leader’s view.
- `POST /doc/<id>/op` — JSON body with operation type, indices, string `client_id`, and monotonic `seq`; response includes `{"version":…}` on success.
- `GET /doc/<id>/stream` — `text/event-stream` with replay from `Last-Event-ID` on reconnect, distinct event kinds for operations and cursors, and periodic comments as keep-alives.
- `POST /doc/<id>/cursor` — cursor position under separate keys `doc/<id>/cursor/<cid_hash>`.
- `POST /admin/fail/<replica_index>` — calls the same `fail_replica` hook used in simulation (100% loss on all channels incident to that replica index).
- `GET /`, `GET /editor.html`, `GET /editor.js` — static editor assets.

### 2.3 Client: browser editor

The editor (`static/editor.js`) keeps `serverText` (last acknowledged prefix), a `pending` list transformed against commits learned from SSE, uses `fetch` for posts, and `EventSource` for the combined op and cursor stream. When the HTTP response to `POST …/op` returns a version, the client applies that acknowledgement immediately so typing does not depend solely on SSE ordering under reconnect or scheduling jitter.

### 2.4 Simulation clock versus server clock

Under `pt-collab`, Cotamer’s default virtual-time progression keeps long randomized campaigns tractable. Under `pt-collab-server`, the code switches to `cot::set_clock(cot::clock::real_time)` so `netsim` per-link delays and coroutine timeouts correspond to wall-clock time. That distinction matters when interpreting sluggish demos versus seconds-long simulation runs.

### 2.5 Code map

| Path | Role |
|------|------|
| `pset4/doc_ops.{hh,cc}` | OT insert/delete, serialize, `transform`, `transform_seq`; target `test-doc-ops`. |
| `pset4/doc_state.{hh,cc}` | `op_key`, `read_ops`, `reconstruct`, document cache; `test-doc-state` and helpers for `pt-collab --test`. |
| `pset4/collab_model.{hh,cc}` | Simulated concurrent editors issuing Paxos puts (`pt-collab` bench). |
| `pset4/http_server.{hh,cc}` | HTTP/1.1, JSON, SSE, multi-document registry, `hash_client_id`, `/admin/fail`. |
| `pset4/pt-collab.cc` | Paxos cluster, failure schedules, convergence checks, `pt-collab` and `pt-collab-server` entry points. |
| `pset4/static/editor.{html,js}` | Browser UI. |
| `pset4/collab-bench.sh` | Scripted regression over schedules and loss. |
| `cotamer/` | Coroutine runtime, timers, and I/O integrated with this tree. |

---

## 3. Operational transformation

We leaned on the Jupiter-style picture (Nichols et al., 1995): Paxos hands you a total order over committed character ops; the browser keeps a confirmed prefix and transforms anything still pending whenever foreign commits change indices underneath you. The heavy lifting is in `doc_ops.cc`—pairwise `transform`, listwise `transform_seq`, plus unit tests for all four insert/delete pairings, serialize/deserialize, and a thousand-trial randomized diamond run (seed 42 in `test-doc-ops`).

Same-position insert/insert pairs are skipped in the randomized diamond harness: without a tie-break, the raw OT layer does not owe you a diamond there. At the system boundary we require string `client_id` plus monotonic `seq` on every op so storage keys and commit order stay deterministic—same trick Jupiter-style systems use. The handout’s further-reading list (differential sync, CRDT editors, etc.) is real engineering; we did not go there. One ordered log plus client OT was enough rope for one semester.

---

## 4. Failure model and handling

### 4.1 What failure means in simulation

`pt-collab.cc` implements `fail_replica(rid)` by marking the replica unavailable and setting message loss probability to 1.0 on every simulated channel incident to that replica (client requests, replica-to-replica links). That models abrupt network isolation of a node, not a silent data corruption. `recover_replica` restores the baseline loss profile from the tester configuration. For split-brain exercises, `partition_one_against_rest` first heals all links, then severs replica `rid` from every peer in both directions while leaving the clique among the remaining replicas connected—so the isolated replica cannot participate in a majority until the schedule heals the partition.

Schedules named `failover`, `recover`, `split`, `unstable`, `torture`, and `random` are implemented as Cotamer tasks that await timed steps between these primitives; `collab-bench.sh` exercises the first five by name plus a high-loss run with a global loss parameter.

### 4.2 Convergence checks after each simulated run

After editors finish submitting work, the harness reconstructs text from each live replica’s database view using `collab::read_ops` and `collab::reconstruct`. It requires agreement among live replicas on reconstructed text. When an elected leader replica is observable in the simulation state, the harness additionally requires every *available* replica’s reconstructed document to match the leader’s reconstruction—a stricter invariant than comparing only to the peer with the highest applied version, and one that catches subtle ordering gaps between followers and the leader’s log view.

### 4.3 HTTP-visible failure

`POST /admin/fail/<id>` invokes the same `fail_replica` path wired through `http_server.cc` into the Paxos instance backing the server. Replica indices are zero-based for the default three-replica configuration. There is no symmetric unfail control in the UI; restarting the process clears simulated failures.

---

## 5. Testing strategy and recorded results

### 5.1 Layered verification

We stacked tests the way we stacked dependencies—if OT is wrong, Paxos cannot save you, so there is no point randomizing elections until `test-doc-ops` is green.

1. `test-doc-ops` — OT and serialization only (`doc_ops.cc` with `RUN_DOC_OPS_TESTS`), no Paxos or network.
2. `test-doc-state` — reads toy `pancydb` rows, sorts by `(version, client_id, seq)`, and checks `reconstruct` against golden strings (`doc_state.cc` with `RUN_DOC_STATE_TESTS`).
3. `pt-collab --test` — runs the doc_state self-checks, then four fixed-seed Paxos+collab convergence runs under `unstable` (seeds `66` and `212043175035943`, i.e. `0x42` and `0xC0DA26200427`), `failover` (seed `99`), and `recover` (seed `7`), matching the literals in `run_scheduled_collab_convergence_tests()` in `pt-collab.cc`.
4. `pt-collab -R N` and `bash collab-bench.sh` — many independent random seeds per schedule; `collab-bench.sh` also includes a baseline `-R 100` run without a named failure schedule (lossless links) plus `-l 0.15` global loss batches.

Every `collab-bench.sh` phase after `--test` still runs inside the simulator with loss and/or named schedules where the script says so—that matched how we read the requirement that tests must involve failure, not a separate brag.

### 5.2 Manual cross-check

We still open two Chrome tabs on `http://localhost:8080/` and type over each other like normal humans. It is not reproducible in CI the same way, but it catches stupid HTTP/SSE/JSON mistakes the harness will never file.

### 5.3 Representative transcript (May 2026)

We ran the following from a clean tree in `pset4/`; everything exited 0.

`./build/test-doc-ops` passed, including the diamond line (1000 trials; 154 skipped same-position insert/insert pairs—expected per `doc_ops.cc` and logged in Chunk 1 of `labnotebook.md`).

`./build/pt-collab --test` printed:

```
Running scheduled collab Paxos+failure convergence (fixed seeds)...
collab convergence: -f unstable -S 66
128 submitted, 122 committed, 334 transformed
collab convergence: -f unstable -S 212043175035943
130 submitted, 117 committed, 352 transformed
collab convergence: -f failover -S 99
170 submitted, 167 committed, 377 transformed
collab convergence: -f recover -S 7
99 submitted, 94 committed, 173 transformed
All tests passed.
```

`bash collab-bench.sh` completed on the same machine; its phases match the script verbatim (`--test`, `-R 100`, then `-f` failover through torture, then high loss). It ends with `All tests passed.` as the last line of output.

---

## 6. Cotamer integration and runtime behavior

We merged a newer `cotamer/` into this tree for HTTP/I/O and clock fixes. The surprise for us—not a bug report from the library, but a behavior change—was that `clock::real_time` and `clock::virtual_time` used to be effectively the same underlying value, so `cot::set_clock(real_time)` in `pt-collab-server` barely did anything. After the upgrade, `real_time` means wall clock: `netsim` delays sleep for real, SSE polling ticks for real, Paxos timeouts argue with the scheduler for real (`labnotebook.md`, Cotamer upgrade notes).

With `cot::clock::real_time` enabled, `testinfo` link/send/recv delays become real sleeps, `cot::after` in HTTP and SSE advances on the wall clock, and Paxos client timeouts argue with the real scheduler; `pt-collab` still uses virtual time for long sweeps.

That did not add a `#define MAX_TABS 4` anywhere. Extra tabs mean extra TCP and SSE work on the same coroutine loop, and every `POST /op` still funnels through one `http_client_model` bridge—so past a handful of windows the process can look healthy while edits stop crossing. The harness is where we trust the failure story; Chrome is where we convince ourselves we did not wire HTTP backwards.

For demos on localhost we usually zero out link/send/recv delay flags (see `labnotebook.md`) when we want snappy typing instead of faithful WAN pain.

---

## 7. Lessons learned

We are glad we invested in `test-doc-ops` and `test-doc-state` first. OT and cache-order bugs showed up as fast failing asserts, not as heisenbugs under `unstable` schedules.

We are glad failure schedules are just Cotamer code in `pt-collab.cc`. Adding a `collab-bench.sh` phase stayed a small diff because the schedule machinery was already there.

We are glad we tightened the harness to compare live replicas to the elected leader’s reconstructed text, not only to whichever replica had the highest version. That matched our mental model of Raft-shaped logs and caught mismatches we might have waved away otherwise.

We are very glad we noticed the clock split before defending performance numbers we never actually measured under real time.

We keep repeating this to ourselves: Paxos can keep commits safe while the UI still feels broken if the client path is saturated—different failure mode, same user frown.

---

## 8. Limitations and future work

- We did not tune SSE fan-out or coroutine fan-in for large rooms of editors.
- OT is insert/delete only; no undo tree, no rich text.
- `/admin/fail` is a teaching hook, not access control.
- If we had another week: event-driven SSE wakeups instead of polling, maybe WebSockets, maybe splitting read traffic from write traffic.

---

## 9. Reproduction

If you are grading this or future-us is debugging it: from repo root, `cd pset4`, then configure once and build.

```bash
cd pset4
cmake -B build && cmake --build build
```

Thereafter, incremental builds use `cmake --build build`. Typical verification:

```bash
./build/test-doc-ops
./build/test-doc-state
./build/pt-collab --test
bash collab-bench.sh          # several minutes; full scripted campaign
./build/pt-collab-server       # optional: browse http://localhost:8080/
# optional: ./run-server.sh --port 8080   # timestamped logs under logs/
```

Implementation narrative, flags we twiddled, and the blow-by-blow: `labnotebook.md`.

---

## 10. Conclusion

We think DoomDraft hits what CS 2620 asked for: Paxos-backed state, automated tests that are mean to replicas and links, and a browser surface that proves the HTTP path is not imaginary. Our strongest claims come from `pt-collab` and `collab-bench.sh`; the editor is the part we show friends because it lights up.

The Cotamer upgrade made the server honest about time. That was the right long-term fix, and it also forced us to stop conflating passing under virtual time with feeling instant in the browser. We would rather write that down here than oversell a demo.

Thank you for a great semester, a lot of knowledge learned, a lot of heads banged (and put together). Have a wonderful summer!