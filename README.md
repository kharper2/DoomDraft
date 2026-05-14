# DoomDraft

## What this is

We built DoomDraft as a small collaborative text editor for Harvard CS 2620 pset4. Document state is replicated with Paxos-backed Pancydb; clients send insert/delete operations over HTTP, and the server streams committed ops (and cursor updates) with Server-Sent Events. A browser editor in `pset4/static/` talks to `pt-collab-server`. The `pt-collab` binary stress-tests Paxos plus operational transformation under Cotamer schedules that inject replica failure and partitions.

We (Aengus and Kathryn) work on this as a joint project; implementation credits and the debugging narrative live in `labnotebook.md`.

## Repository layout

- `pset4/` — application: CMake project, HTTP server, Paxos harness, `static/` assets, `collab-bench.sh`, `run-server.sh`.
- `cotamer/` — coroutine runtime and I/O we use for the server and simulations (vendored / course-integrated tree).

We summarize design and test results in `WRITEUP.md`.

## Prerequisites

From `pset4/`, configure with CMake (3.16 or newer). The project requests C++23; on macOS, Apple Clang 15+ (Xcode / CLT 15+) is required for the Cotamer dependency—see `pset4/CMakeLists.txt` if configure fails with libc++ / `<format>` errors.

## Build

We assume the working directory is `pset4/` for everything below (from the repo root: `cd pset4` once).

First-time configure and build:

```bash
cmake -B build && cmake --build build
```

After changing sources, rebuild only:

```bash
cmake --build build
```

## Tests and benchmarks

After a successful build, we rely on these as the main automated correctness story (failure schedules live in `pt-collab` and `collab-bench.sh`, not only in the browser).

| Command | Role |
|---------|------|
| `./build/test-doc-ops` | OT insert/delete transforms, serialization, randomized diamond trials. |
| `./build/test-doc-state` | Operation keys, `read_ops`, document reconstruction from a toy DB. |
| `./build/pt-collab --test` | Runs `test-doc-state` logic plus fixed-seed Paxos + collab convergence under `unstable`, `failover`, and `recover`. |
| `bash collab-bench.sh` | Longer scripted battery: `--test` then many random seeds across schedules (failover, recover, split, unstable, torture, high loss). Several minutes. |
| `./build/pt-collab -f <schedule> -R <N>` | Ad hoc campaigns (e.g. `-f unstable -R 100`). |

Quick sanity pass after core logic changes:

```bash
./build/test-doc-ops && ./build/test-doc-state && ./build/pt-collab --test
```

## Run the demo server

With `pset4/build` already built:

```bash
./build/pt-collab-server
```

Then open http://localhost:8080/ in a browser (multi-tab editing and optional replica fail via the UI or admin routes).

Timestamped logs under `pset4/logs/` (gitignored):

```bash
./run-server.sh --port 8080
```

For snappier localhost behavior when simulated link delays make Paxos feel slow, we document delay flags in `labnotebook.md`.

## Source and history

We develop on GitHub: https://github.com/kharper2/DoomDraft — that is still the best place to browse full history and blame.

We both hit the course-site rule that the grader cannot confirm access until a given `@college.harvard.edu` address has authored a commit on the repository. To satisfy course access, we also keep copies checked out in the CS 2620 environment the staff provide, which is what we use as the submission-facing tree for grading. For the complete commit graph, use the GitHub link above.
