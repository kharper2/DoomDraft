# DoomDraft: Collaborative Editing on Paxos

**CS 2620 — Distributed systems project (writeup)**  
**Authors:** Kathryn Harper & Aengus  
**Repository:** `DoomDraft` (Paxos + PancyDB + OT + HTTP/SSE browser demo)

> **Submission note:** This file is the **standalone course paper** (polish here, then export to **PDF** or paste into Google Docs). Target length **~4–6 pages** when typeset. Internal work logs, chunk checklists, and implementation details stay in **`linear.md`** — not part of this document.

---

## Abstract

We present **DoomDraft**, a two-user collaborative text editor. A **simulated Paxos** cluster (Cotamer) provides a fault-tolerant **total order** of character-level **insert** and **delete** operations stored in **PancyDB**. **Operational Transformation (OT)** reconciles concurrent edits on each client. We validate correctness with **unit tests**, randomized **diamond** checks, and **`pt-collab`**: a harness that compares **reconstructed document text across all live replicas** under scripted failures and message loss. A second layer, **`pt-collab-server`**, exposes the same backend over **HTTP** and **Server-Sent Events (SSE)** with a **static browser client** for a live two-tab demo, including optional **replica failure** during editing.

---

## 1. Introduction

We built **DoomDraft**, a small Google Docs–style editor for two users. The backend is a **simulated Paxos cluster** (Cotamer coroutines) where every edit is a **Paxos-ordered** insert or delete on a shared log stored in **PancyDB**. The hard part of collaboration—**reconciling concurrent edits**—is handled with **Operational Transformation (OT)** on each client before and after commits. On top of that we added **`pt-collab-server`**: a real **HTTP + Server-Sent Events** front end and a **static browser client** so two tabs can edit live. We also run a **failure campaign** (`collab-bench.sh`) to show the replicated log survives leader failure, partitions, message loss, and adversarial schedules while **all live replicas agree on the reconstructed document**.

---

## 2. Consensus and total order (why Paxos)

**Paxos** gives us a fault-tolerant **total order** of committed values. For this project, each committed client request is effectively “append this key/value at this version.” That order is what makes OT tractable: instead of inventing a partial order of operations in the network, we treat the **PancyDB version** (aligned with Paxos commits) as the canonical sequence number for replaying ops.

Each simulated **editor** (in `collab_model`) and the **HTTP bridge** submit **PUT** requests through the same client redirect / timeout logic as the rest of the pset’s Paxos examples. After failover, clients may need to retry against a new leader; the important property for the editor is that **every accepted op eventually appears in the same relative order on all non-faulty replicas**.

---

## 3. Operational Transformation — background

OT is a classical way to keep **replicated structured documents** consistent under concurrent edits. The usual correctness statement is a **convergence** or **commutativity** property: if two operations `a` and `b` are generated against the same document state but applied in different orders at different sites, there exist adjusted operations so that both orders produce the **same final text**.

We use the standard **Jupiter-style** formulation: **`transform(a, b)`** returns `a'`, meaning “`a` as it should be executed **after** `b` has already been applied,” such that (informally) applying `b` then `a'` matches applying `a` then the symmetric transform of `b`. Our implementation follows the four-case split **Insert/Insert**, **Insert/Delete**, **Delete/Insert**, and **Delete/Delete** (with six overlap sub-cases for delete/delete), matching the handout’s “character-level OT” scope.

**Known limitation (documented in code):** concurrent **inserts at the exact same position** without an additional tie-break (beyond our positional rule) can break the full diamond property; the randomized test **skips** those pairs. In the full system, **client id + sequence** disambiguate real submissions; the simulation and HTTP paths both attach a client id and monotonic seq per logical editor.

---

## 4. OT implementation and testing (simulation layer)

**Implementation (`doc_ops.hh` / `doc_ops.cc`).**  
We represent ops as `insert_op{pos, text}` or `delete_op{pos, len}`, serialize them as plain text (`I pos text…`, `D pos len`), and implement **`apply_op`**, **`transform`**, and **`transform_seq`**. The unit test binary **`test-doc-ops`** (built with `RUN_DOC_OPS_TESTS`) runs:

- Hand-crafted cases for all II / ID / DI / DD sub-cases (matching the C++ expectations from the diamond checks).
- **Randomized diamond trials** over random documents and ops (excluding the same-position II case noted above).
- Serialize/deserialize round-trips.

**Document state (`doc_state.hh` / `doc_state.cc`).**  
We **reconstruct** the document by scanning the `doc/<id>/op/...` prefix in PancyDB, parsing ops, sorting by **committed version** (then tie-breaking on client id / seq), and replaying with `apply_op`. **`test-doc-state`** validates empty DBs, single and multi-op traces, cross-client ordering by version, and garbage values skipped safely.

**Collaborative simulation (`collab_model` + `pt-collab`).**  
Each simulated editor keeps **pending** local ops, polls for newly committed ops from peers, **OT-transforms** pending ops against each remote op in commit order, and submits its own ops through Paxos. **`pt-collab`** runs the same failure harness style as **`pt-paxos`**, and after each run **compares reconstructed text across all live replicas** — that is our strongest end-to-end correctness check for the OT + log + replay stack.

---

## 5. Failure testing — what we exercised

We run **`bash collab-bench.sh`** from `pset4/`, which executes `pt-collab` under:

- `--test` (doc_state smoke),
- basic **`-R 100`** convergence,
- **`failover`**, **`recover`**, **`split`**, **`unstable`**, **`torture`** schedules,
- **high message loss** (`-l 0.15` with many seeds).

**Goal:** under churn, message loss, and leader/partition scenarios, **reconstructed text must still match on every live replica** after the schedule completes. Failures here almost always indicate a bug in **sync / ordering / OT integration** (e.g. missing a committed op), not in Paxos itself.

**Live demo (browser path):** `pt-collab-server` additionally exposes **`POST /admin/fail/<replica_id>`** so we can **kill one replica** during a two-tab demo and show that **HTTP-committed edits still complete** (leader discovery + retries).

---

## 6. System architecture — HTTP + browser

**`pt-collab-server`** runs **three Paxos replicas** and the **HTTP accept loop** in one OS process (Cotamer **real-time** clock). There is **no separate Node server**: we parse **HTTP/1.1** by hand on top of **`tcp_listen` / `tcp_accept`**.

- **`GET /doc/<id>`** returns JSON `{"text","version"}` from cached + polled state.
- **`POST /doc/<id>/op`** commits an insert/delete through Paxos; response `{"version"}`.
- **`GET /doc/<id>/stream`** is **SSE**: `event: op` for edits, `event: cursor` for caret updates, periodic `event: ping`.
- **`GET /docs` / `POST /docs`** maintain a **`docs/registry`** key for multiple documents.
- **Static editor:** `pset4/static/editor.html` + `pset4/static/editor.js` — vanilla JS OT client with **`EventSource`**, one in-flight op at a time, doc picker, optional fail button.

---

## 7. Deployment / how to run the demo

1. Build **`pt-collab-server`** (CMake target in `pset4/`).  
2. From **`pset4/`**: `./build/pt-collab-server --port 8080` (so `static/` paths resolve).  
3. Open **`http://localhost:8080/`** in **two browser tabs**.  
4. (Optional) **New doc** → second document; show isolation from `main`.  
5. (Optional) **Fail replica** → type still works.  
6. For the **simulation grade**: `bash collab-bench.sh`.

---

## 8. Discussion and future work

- **Same-process cluster** is ideal for 262 debugging; a production system would separate processes/machines and use real TCP between nodes (the course’s Cotamer **netsim** already models loss/partition at the message layer).
- **HTTP client multiplexing:** the server uses one simulated Paxos client id for all HTTP traffic; enough for a demo; scaling would add more client ids or batching.
- **Cursor overlay** is intentionally simple (`ch` + line height); pixel-perfect carets would mirror the textarea layout.
- **CRDTs** are an alternative to OT for text; we chose OT to match the course’s Jupiter framing and to reuse a small, testable core.

---

## 9. Conclusion

DoomDraft meets the **Ambition 4/5** bar: an **application on Paxos** with **documented failure behavior**, plus a **working browser path** showing real-time collaboration. The simulation layer gives **strong confidence** (unit OT + randomized diamond + multi-replica reconstruction under stress); the HTTP layer shows the same abstractions can drive a **human-visible demo**.

**Suggested split for grading:** simulation + OT + failure campaign primarily **Person A** implementation; HTTP/SSE + static client + multi-doc/cursors primarily **Person B** — both partners should be able to explain **end-to-end** behavior in the oral if asked.
