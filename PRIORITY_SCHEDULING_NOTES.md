# Priority Scheduling — Work Log & Findings

**Status:** Phases 1–3 complete and validated. The Phase 2 "deadlock" was root-caused
(a sticky-victim **starvation livelock**, not a lost wakeup) and fixed; the fix survives
80/80 stress runs. Reserved high-priority workers (Phase 3) deliver flood-immune render
latency (~2 ms == idle baseline). Remaining: Phase 4 (unit tests + benchmark gate).

**Date:** 2026-07-04

---

## Goal

Add task **priority** and **reserved worker capacity** to this Taskflow fork so that
latency-critical work (e.g. UI rendering) is scheduled ahead of throughput-oriented
background work (e.g. large IO), instead of the app having to spin up multiple executors.

Agreed scope:
- **Priority ordering + reserved capacity** (dedicated high-priority workers), not priority-only.
- **Compile-time gated** by `TF_MAX_PRIORITY` (==1 must fold to the original single-queue codegen).
- **Benchmark-validated** so the hot path the fork optimized is not regressed (Phase 4, pending).

---

## Results (examples/priority_scheduling.cpp, 4 workers, 48 frames, render 2 ms, IO 40 ms)

| scenario | render latency (avg / p95) | frames missed |
|---|---|---|
| (1) render only (baseline) | **2.0 / 2.0 ms** | 0/48 |
| (2) render + IO flood, no priority | **~84 / ~320 ms** | 31/48 |
| (3) render HIGH + IO LOW | **~40 / 42 ms** | 48/48* |
| (4) render HIGH + IO LOW + **1 reserved worker** | **2.0 / 2.0 ms** | **0/48** |

\* every frame waits ~one 40 ms IO task — above the 16 ms budget but tight and predictable.

- (2) improved as a side effect of the liveness fix (was avg ~310 ms / 48-48 missed with
  the pre-fix pinned victims): rotating victims bound how long any buffer is ignored, so
  even unprioritized work is scheduled more fairly, at the price of a heavy tail (p95).
- (3): priority collapses the stall to ~one in-flight IO duration — the floor for pure
  priority ordering on a non-preemptive scheduler.
- (4): the reserved worker parks through the flood and runs each render task on arrival;
  latency equals the idle baseline. This is the feature's end goal, achieved.
- Gated build (`/DTF_MAX_PRIORITY=1`): (3) ≈ (4) ≈ (2) (tags and reserved count ignored),
  i.e. the priority machinery folds away as required.

---

## What was built

### Phase 1 — priority type + public API
- `TaskPriority { HIGH=0, NORMAL=1, LOW=2 }`, `TF_MAX_PRIORITY` (default 3) —
  `taskflow/core/declarations.hpp`.
- `Node::_priority`, `TaskParams::priority` (async tasks carry priority through
  `animate()`), `Task::priority()` get/set — `graph.hpp`, `task.hpp`.

### Phase 2 — per-priority queues
- `Worker::_wsq` → `std::array<BoundedWSQ<Node*>, TF_MAX_PRIORITY>` (`worker.hpp`).
- `Executor::Buffer::queue` → `std::array<UnboundedWSQ<Node*>, TF_MAX_PRIORITY>`.
- Routing by `node->_priority` in `_schedule` / `_spill` / `_bulk_*`; priority-first
  draining in `_exploit_task` / `_corun_until`; multi-queue empty-scans in `_wait_for_task`.

### The starvation fix — priority-major stealing sweep (`Executor::_steal_task`)
`_explore_task` and `_corun_until` no longer steal victim-major (pick one victim, try its
levels). `_steal_task` probes **every queue (all workers + all buffers) at priority 0
before any queue at priority 1**, etc., starting each sweep at a per-worker cursor that
**advances by one on every successful steal**. Consequences:
- **Global priority ordering**: a HIGH task in any buffer beats LOW tasks everywhere
  (previously priority was honored only *within* one buffer — cross-buffer inversion).
- **Liveness**: the rotating start bounds same-priority starvation; a full sweep finds any
  spilled task as soon as a worker frees up.
- Cost: a few extra empty-queue probes (2 relaxed loads each) per steal; only in
  `TF_MAX_PRIORITY > 1` builds.
- **`==1` builds keep the single-victim steal but adopt the rotation** (advance the victim
  hint on success instead of pinning it). This deviates from bit-exact original codegen by
  one add per successful steal — deliberately: the starvation reproduces on the gated
  build too (~1/60 stress runs, and the updated example hung live in scenario (2) at `=1`
  before this change). Re-check in the Phase 4 benchmark gate; if steal-locality loss
  shows up, "pin K times then rotate" is the fallback.

### Phase 3 — reserved high-priority workers
- `tf::Executor(N, num_reserved, wif = nullptr)` — reserves `num_reserved` workers (tail
  ids) that **only execute priority-0 tasks**; throws if `num_reserved >= N`; ignored on
  `TF_MAX_PRIORITY==1` builds (`_num_reserved` is `constexpr 0`, all branches fold away).
- Reserved workers sweep only level 0 in `_steal_task`; their local queues stay pure
  priority-0 (`_schedule`/`_bulk_schedule` spill lower-priority pushes to the buffers).
- **Separate parking lot (`_hp_lot`)**: reserved workers must never consume a wakeup
  meant for lower-priority work (they would re-check only p0 queues and park again — a
  genuine lost wakeup). Push sites notify `_notifier` always and the lot when priority-0
  work was scheduled (`_notify_hp`; bulk paths count p0 nodes, and `_bulk_spill*` return
  that count). The lot is a mutex/cv with a publish-then-rescan handshake (equivalent
  no-lost-wakeup guarantee to the 2PC notifier) — chosen over the lock-free notifier
  because the deadlock valve below needs *timed* waits; reserved workers park on the
  cold path, so a mutex/cv costs nothing that matters.

### The reserved-worker deadlock valve (found by downstream DUF/MeshLib load)
Strict refusal of lower-priority work deadlocks the executor when every general worker
is occupied by blocking work whose progress (transitively) depends on a pending
default-priority task — canonical trigger: an external thread blocks in `run().wait()`
while the general pool is saturated (MeshLib's off-worker `run_taskflow`). Reserving one
worker shrinks the general pool from N to N−1, and the pending task's only free worker
is the reserved one, which refuses it. Repro: with all N−1 general workers pinned by
non-yielding NORMAL tasks, a non-worker `run().wait()` of a NORMAL task hung forever;
with even one general worker free it completed.

**Naive fix rejected**: "reserved workers fall back to lower work when no HIGH work
exists" collapses reservation into a general worker (the priority-major sweep already
makes every general worker prefer HIGH globally) — scenario (4) would regress from
2 ms to ~40 ms, since between frames there is *always* lower work to fall into.

**Key insight**: a deadlocked pool and a healthy saturated pool are indistinguishable by
*pending work*; the discriminator is *progress*. Under the IO flood, general workers
complete tasks constantly; in the deadlock, nothing completes. So:

- Each worker counts invoked tasks (`Worker::_num_invokes`, relaxed, own cacheline,
  bumped only when reservation is active; folds away entirely at `TF_MAX_PRIORITY==1`).
- A reserved worker parks with a timeout (`TF_RESERVED_STALL_MS/2`, default 50 ms). On
  each timeout it samples general progress; **two consecutive windows with zero general
  progress while lower-priority work is pending** open the valve
  (`Worker::_reserved_assist`): the worker steals lower-priority work like a general
  worker (its `_steal_task`/corun scope widens with it, so it can also help drain a
  blocked task's own children). The valve closes the moment general progress resumes.

Verified: the fatal all-generals-blocked repro completes in ~220 ms (valve opens at
~150 ms, assist streak drains at full speed — 200 stranded NORMAL tasks in ~215 ms);
scenario (4) stays 2.03 ms avg / 2.10 ms max, 0/48 missed (A/B with the valve disabled
via `TF_RESERVED_STALL_MS=100000` shows identical ~2 ms worst-case — the valve never
opens under healthy load). Residual caveat: a workload whose every task runs longer
than the detection window can open the valve spuriously; cost is bounded (one
lower-priority task on the reserved worker, then strict again) and the window is
tunable via `TF_RESERVED_STALL_MS`.

---

## The Phase 2 hang: root cause (differs from every earlier hypothesis)

### It was a starvation livelock, not a lost wakeup
The instrumented hung process showed: **all 4 workers awake** (`num_waiters==0`), cycling
steal→invoke on 40 ms IO tasks (~1 iteration / 40 ms each), **all with the same sticky
victim** — the buffer the IO flood spills into — while the HIGH render task sat alone in a
*different* buffer for 12+ s:

```
worker 0..3: phase=invoke, sticky=5 (= buffer 1)
buffer 0: [1 0 0]   <- the starving HIGH render task
buffer 1: [0 0 28]  <- the IO flood, drained & refilled forever
```

Mechanism: `_spill` hashes tasks to buffers by node pointer (`(ptr>>16) % B` — node-pool
block-affine, so each submitting thread's tasks land in a stable buffer). The old
`_explore_task` stole from `w._sticky_victim` and re-randomized **only when a steal
failed**. Under saturation the flood buffer never runs dry, so no steal ever fails, no
victim is ever re-picked, and no worker ever reaches the `_wait_for_task` empty-scans.
A task spilled into any other buffer is invisible forever. The 2PC notifier protocol is
sound and was never the problem; every notify was correctly delivered to workers that
were simply never going to look at the right queue.

### Attribution (refining the earlier back-and-forth)
Fast repro (concurrent flood producer + `async().wait()` frames, 12 s watchdog):

| build | result |
|---|---|
| pristine fork HEAD | 0 hangs / 60 |
| this tree, gated `TF_MAX_PRIORITY=1`, before fix | **1 hang / 60** (and the updated example hung live) |
| this tree, `TF_MAX_PRIORITY=3`, before fix | ~5–10% of runs hang |
| this tree, `TF_MAX_PRIORITY=3`, **after fix** | **0 hangs / 140+** across all stress blocks |
| this tree, `TF_MAX_PRIORITY=1`, **after rotation fix** | 0 hangs (final validation) |

So the *mechanism* (sticky victim + hashed spill buffers) is latent in the fork's design —
the gated build, whose scheduler codegen matches the original, also starves, just rarely
(the only delta is `Node::_priority` changing node size and therefore the pointer-hash
dice). Phase 2 raised the expression probability dramatically (more queues, different
layout). The earlier session notes were each half-right: the hang was "introduced" by
Phase 2 in probability, but the bug class predates it — pristine HEAD carries it too and
was simply lucky over 60 runs. Worth flagging upstream/fork-owner. Both configurations of
this tree now carry the liveness fix (sweep at `>1`, rotation at `==1`).

### Diagnostic technique (for the next scheduler bug)
Watchdog-triggered state dump beat theorizing: per-worker phase markers (relaxed atomic
stamps at each stage of the scheduling loop) + sticky victim + per-priority queue sizes +
notifier state, dumped twice 500 ms apart to distinguish parked/spinning/progressing.
The instrumentation was temporary (`TF_PRIO_DEBUG`) and has been removed from the tree;
see this session's scratchpad `repro.cpp` pattern to recreate it.

---

## Build / repro on this machine (Windows, MSVC via VS2022)

Taskflow is header-only; only `-I <repo-root>` is needed.

```bash
# from Git Bash (vcvars64 + cl, see build_any.bat pattern in scratchpad)
cl /nologo /std:c++20 /EHsc /O2 /I "d:\Development\taskflow" examples\priority_scheduling.cpp
# gated build: add /DTF_MAX_PRIORITY=1
```

The example now uses a **concurrent producer thread** for the IO flood (the realistic
pattern; safe since the fix). Stress repro: 15 render `async().wait()` frames against the
flood, 12 s watchdog, run 60–80×.

## Files touched
- `taskflow/core/declarations.hpp` — `TaskPriority`, `TF_MAX_PRIORITY`
- `taskflow/core/graph.hpp` — `Node::_priority`, `TaskParams::priority`
- `taskflow/core/task.hpp` — `Task::priority()` get/set
- `taskflow/core/worker.hpp` — per-priority `_wsq` array
- `taskflow/core/executor.hpp` — per-priority buffers/routing, `_steal_task` sweep,
  reserved workers (`_num_reserved`, `_hp_notifier`, `_wait_for_task_reserved`, ctor)
- `examples/priority_scheduling.cpp` (+ `examples/CMakeLists.txt`) — 4-scenario demo

## Remaining plan — Phase 4
- Unit tests: HIGH-before-LOW ordering; cross-buffer HIGH-beats-LOW; no starvation under
  saturated flood (regression for the fix); reserved-worker pickup while flooded;
  reserved-worker parking (contributes nothing to LOW work); `num_reserved >= N` throws.
- Benchmark gate: `TF_MAX_PRIORITY==1` must be a wash vs fork HEAD; measure the `==3`
  sweep overhead on the fork's benchmark suite (steal-heavy cases in particular).
