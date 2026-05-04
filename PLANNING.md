# DoomDraft — planning & context

Shared notes for **Kathryn** and **Aengus**. Update this file as decisions change or milestones land.

---

## Course context (CS 2620, Pset 4)

- **Track:** (Ambition **4/5**) Application on Paxos — collaborative editing à la Google Docs; demonstrate **failure handling**.
- **Requirements:** More work than pset 2; calibrate ~**15 hours** (can go deeper). **Automated tests** are mandatory. **Failure handling** is mandatory — tests must include **failure of at least one system component**. Turnin: code, lab notebook, **4–6 page** writeup (Markdown or PDF).
- **Group:** Small groups allowed with **instructor pre-approval**; max 3; **each student** turns in their **own** writeup.
- **Repo:** [github.com/kharper2/DoomDraft](https://github.com/kharper2/DoomDraft)

### What we are building (one paragraph)

**DoomDraft** is a minimal **two-user** collaborative text editor on top of **replicated Paxos** (Aengus’s implementation as the single backend). Edits are proposed as operations, **totally ordered** by Paxos, and **deterministically applied** on every replica so all copies converge. We focus on **correctness under concurrency and failure**: automated tests for overlapping edits, message delay/reorder where the harness allows, and **at least one replica failure** (crash/restart), verifying convergence and no loss of **committed** operations (define “committed” to match the Paxos API we expose).

### References (related work — not a checklist to implement)

| Topic | Link / pointer |
|--------|----------------|
| Eg-walker (CRDT / list editing) | [arXiv:2409.14252](https://arxiv.org/pdf/2409.14252) |
| Differential synchronization | [neil.fraser.name/writing/sync](https://neil.fraser.name/writing/sync) |
| Jupiter (older OT-style work) | [ACM](https://dl.acm.org/doi/10.1145/215585.215706) |
| Yjs | [yjs.dev](https://yjs.dev) |
| Critiques of Yjs / collab editing | [Moment, part 1](https://www.moment.dev/blog/lies-i-was-told-pt-1), [part 2](https://www.moment.dev/blog/lies-i-was-told-pt-2) |

**v1 stance:** Paxos gives **central ordering**; merge semantics can stay **simple** (total order + deterministic tie-break). OT/full CRDT/Yjs is **optional later** if time; course credit is **replication + tests + failures**.

---

## Decisions (fill in / keep current)

| Decision | Choice | Notes |
|----------|--------|--------|
| Paxos codebase | **Aengus’s** | Single engine in this repo; do **not** blend two implementations. Port fixes/tests from the other tree if needed. |
| Clients (v1) | **Two** separate client processes | Same binary with `--client-id` or two binaries — pick one and document. |
| Transport | _TBD_ | Use whatever Cotamer / course stack exposes fastest (HTTP, TCP, etc.). |
| Document model (v1) | _TBD_ | e.g. `Insert{pos, byte, client_id}` / `Delete{pos}` — **deterministic** `apply` on all replicas. |
| “Committed” definition | _TBD_ | Align with Paxos: e.g. learned / applied after quorum — **document in tests**. |
| Concurrent same-index rule | _TBD_ | e.g. tie-break by `client_id` then log index — **must be deterministic**. |

---

## Target (“done enough” for the assignment)

1. Two clients edit the **same** plain-text document concurrently.
2. All edits go through **one global Paxos order**; replicas **replay/apply** identically → **same final string**.
3. **Automated tests:** concurrent edits + **≥1 failure** (crash/restart, or chaos: delay/reorder if supported).
4. Assertions: **convergence** on survivors; **no lost committed ops** (per your definition).
5. README: clone, build, run tests, how to run **3 replicas + 2 clients** (or whatever topology you use).

Optional: web UI, fancier merge — **after** the above is solid.

---

## Architecture (short)

```
Clients (2)  →  propose ops  →  Paxos (replicas)  →  ordered log
                                      ↓
                            deterministic apply → document state
```

- **Paxos:** durability + total order of operation values.
- **State machine:** `apply(op, doc) → doc'` — same on every replica; **no** nondeterminism except what’s fixed by op fields + log index.

---

## Phases & milestones

### Phase 0 — Repo & baseline

- [ ] Aengus’s Paxos (and minimal deps) in `DoomDraft`; **one** build path on `main`.
- [ ] README: build, run unit/integration tests, topology (how many replicas).
- [ ] Agree on **op wire format** and **committed** meaning.

**Exit:** `main` builds; existing Paxos tests still pass.

### Phase 1 — Vertical slice (one client)

- [ ] Client can propose ops; replicas apply to in-memory document.
- [ ] Single-client script: N ops → **all replicas** show identical string.

**Exit:** Milestone **A** — one client, no failures, identical state everywhere.

### Phase 2 — Two clients, no failures

- [ ] Two processes send interleaved ops.
- [ ] No hand-merge outside Paxos — order comes **only** from the log.

**Exit:** Milestone **B** — two clients, no failures, convergence.

### Phase 3 — Failure tests (mandatory)

- [ ] Inject **≥1** component failure (e.g. replica kill + restart, or network chaos).
- [ ] Tests assert convergence + committed-op properties.
- [ ] Multiple seeds if harness supports it (good for sim-heavy projects).

**Exit:** Milestone **C** — failures + same correctness story.

### Phase 4 — Writeup & demo

- [ ] Lab notebook updated as you go.
- [ ] 4–6 page paper: design, failures tested, **one** related-work citation from the table above.
- [ ] Short demo path for class / check-in.

---

## Work split (starting point — adjust as needed)

| Area | Primary owner |
|------|----------------|
| Paxos integration, propose/learn API, replica lifecycle | Aengus |
| Op encoding, `apply`, document invariants | Kathryn |
| Failure injection (Cotamer / existing test style) | Aengus (or pair) |
| Two-client harness, integration tests, convergence checks | Kathryn (or pair) |
| README, repro scripts, CI/script to run tests before merge | Shared |

**Integration rhythm:** merge to `main` often; short-lived branches (`feature/…`). Daily or frequent checkpoint: **same test** with two clients on latest `main`.

---

## Milestones checklist

- [ ] **A:** One client, full replica set, no failures → N ops → identical string on all replicas.
- [ ] **B:** Two clients, no failures → interleaved ops → identical string.
- [ ] **C:** Failure model (e.g. crash + restart) → still identical; committed ops not “vanished” from the test-visible story.

---

## Open questions

1. Exact **topology** (# replicas, client vs replica processes)?
2. **Idempotency / retries** — how do clients tag proposals so duplicates don’t double-apply (if applicable)?
3. Instructor **group approval** — confirmed? Date?

---

## Changelog (optional — append when something big lands)

| Date | Note |
|------|------|
| _YYYY-MM-DD_ | _e.g. Imported Paxos baseline; Milestone A green._ |
