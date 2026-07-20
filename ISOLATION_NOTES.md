# Work-Stealing Isolation Scopes — Design & Validation

**Status:** implemented and validated (suite + stress green across worker counts and
both compile-time gates). Companion feature to the priority work in
`PRIORITY_SCHEDULING_NOTES.md`.

**Date:** 2026-07-20

---

## Why

Downstream (MeshLib port, `SharedThreadSafeOwner`/`TbbTaskArenaAndGroup`): many
threads race to use a lazily built shared structure (AABB tree, dipoles); one is
elected to construct it — the construction itself is parallel — and the rest wait.
Original TBB ran this in a `task_arena` for **isolation**: participants "cannot
steal outside tasks until all these tasks are finished".

Vanilla Taskflow has no equivalent. Every wait primitive (`corun`, `corun_until`,
`TaskGroup::corun`) funnels into `_corun_until`, which steals from **every** queue:
group membership controls what is *waited for*, never what may be *stolen*. So a
worker corunning inside the construction can steal a pending outer chunk of the very
loop that triggered the construction; that chunk then waits — on the same stack —
for a completion that can only happen after it unwinds. Result: all-core livelock,
~1 hang in 5 runs of `findDisorientedFaces` on a cold mesh (deterministic with
1 worker + pending siblings). The interim downstream fix was a second executor as a
private arena; isolation scopes make one pool sufficient.

## What

`TF_ISOLATION` gate (default 1; `0` restores exact pre-feature codegen).

- `tf::IsolationScope` (`isolation.hpp`) — a lightweight arena **sharing the
  executor's workers** but owning its task queue (mutex-push / lock-free-steal, the
  buffer protocol). Created via `Executor::make_isolation_scope()` (pooled storage,
  64 concurrent scopes max, throws past that; slots reuse).
- `Executor::isolate(scope, f)` — RAII adoption on the calling thread (worker or
  not; nesting supported).
- `Node::_isolation` / `Topology::_isolation` — scope stamping:
  * `_set_up_graph`: topology-root setups use the scope captured at `run()`
    submission (correct for repeat runs / queued topologies set up by unrelated
    workers); corun/subflow/module setups use the spawning thread's live scope.
  * `_schedule_async_task` / `_schedule_dependent_async_task`: asyncs inherit the
    spawning thread's scope at creation (dependent asyncs may be scheduled later by
    a foreign worker, hence creation-time stamping).
- `_invoke` adopts the node's scope for the invocation (continuation-cache hops
  re-adopt per iteration) and restores the caller's scope on every exit path — this
  is what makes children inherit the scope and confines their corun stealing.
- Routing: `_schedule`/`_bulk_schedule` send isolated nodes to their scope queue —
  never to worker queues or buffers. Bulk fast paths are preserved when
  `_num_scopes == 0` (one relaxed load).
- Consumption:
  * `_corun_until` under a scope → `_corun_until_isolated`: drains **only** the
    scope's queue (executing anything else is exactly the self-steal deadlock).
  * `_explore_task` and scope-0 `_corun_until` sweep active scopes after the
    regular queues come up empty — free workers help any construction, so a scoped
    computation gets the executor's full parallelism.
  * `_wait_for_task` gained "Condition #1.5": don't park while a scope has pending
    work (same 2PC pairing as buffers; scope pushes notify `_notifier`).
- Reserved workers stay strict: they ignore isolated work unless the deadlock valve
  opens (`_lower_pending` now counts stranded isolated work; assist mode sweeps
  scopes via `_explore_task`).

### Why per-scope queues instead of predicate-filtered stealing

The queues are Chase-Lev deques: steal strictly from the top, pop strictly from the
bottom. A tag-filter (`steal_if`) cannot reach a matching task buried beneath a
foreign one, so isolated work co-mingled in shared queues can strand — with all
workers waiting under the scope, that is a **new** deadlock (worst at 1 worker).
Routing by queue removes the burying problem by construction, mirrors how the
priority feature solved its ordering problem (per-priority queues, not filters),
and keeps the no-scope hot path unchanged.

### Scope lifetime vs racing helpers

A helper may hold a scope pointer from the registry while the owner releases the
last handle. Scope storage is therefore **pooled inside the executor and never
freed while the executor lives**: the racing steal touches valid memory; at worst
it steals from the slot's next tenant, which is always legal for a helper.
Contract (documented on the API): all scope work must be joined before the last
handle is released — the same rule as `TaskGroup::corun`.

## Downstream usage sketch (MeshLib `TbbTaskArenaAndGroup`)

```cpp
// elected thread
auto scope = executor.make_isolation_scope();
executor.isolate(*scope, [&]{ creator(); });   // nested parallel_* stay confined
promise.set_value();
// worker waiters
executor.isolate(*scope, [&]{
  executor.corun_until([&]{ return future_ready(); });  // helps construction only
});
// non-workers just block on the future
```

This removes the interim second executor: waiters actually help the construction
instead of blocking, and there is no thread oversubscription.

## Validation (examples/isolation.cpp)

Modes: default = full suite; `--unsafe` = the pre-isolation pattern (expected to
hang; proves the repro detects the real bug); `--stress N`.

Wave 1 (core semantics): basic isolate+corun, poisoned livelock repro, helper
participation, nested scopes (A's construction needs B's), exception
propagation + scope restore, non-worker submission, scope pool + 64-slot cap,
priority interplay, task-group fan-out.

Wave 2 (documented feature surfaces × isolation, bodies assert
`pt::this_isolation` directly): recursive subflows; module composition corun'd
inside a construction (the `_set_up_graph` branch where `tpg` is non-null but
`parent` is the module node); condition-task loops (continuation-cache
re-adoption per iteration); dependent-async chains (creation-time stamping,
foreign-worker scheduling); a semaphore contended across contexts (release
inside a scope schedules the foreign waiter into normal queues — routing is by
node tag, not releasing thread); `run_n` repeat runs (topology re-stamped from
the submission scope by the tearing-down worker); cancellation of a large
isolated topology followed by a full poison round; 8 concurrent lazy caches
under one task storm; a multi-scope external-thread fuzz (registry
claim/release races, pool reuse); reserved-worker strictness via an observer
(0 isolated tasks on the reserved worker across 1280 isolated tasks under
healthy churn).

Results:
- Full suite (13 tests × {1, 4, 16} workers + 3 standalone): **all pass**,
  10/10 consecutive runs clean; 15–16 workers observed helping a single
  construction.
- `--unsafe`: hangs immediately at the 1-worker deterministic round (killed by
  watchdog) — negative control confirmed.
- `--stress 100`: 100 poison rounds × {1, 2, 4, 16} workers, twice, all clean
  (each round: 256 outer chunks racing one lazy cache with 64-task construction).
- Gates: `TF_ISOLATION=0` (corun, priority demo) and `TF_MAX_PRIORITY=1` +
  isolation (full suite) both build and pass.
- Priority regression: scenario (4) of `priority_scheduling` (reserved worker)
  unchanged at ~2.0–2.2 ms avg, 0/48 missed, A/B identical with `TF_ISOLATION=0`.
  Scenario (2)'s large run-to-run variance (65–311 ms avg) reproduces identically
  in the `=0` build — inherent to the unprioritized flood, not this feature.

## Known limits / notes

- 64 concurrent scopes per executor (fixed registry); sequential churn unlimited
  (pooled reuse). Throws on the 65th live scope.
- Isolated pushes take the scope mutex (no local-queue fast path). Fine for chunky
  construction workloads; if a fine-grained isolated workload ever shows contention,
  a per-worker side-slot is the next optimization.
- An isolated corun with an empty scope queue and an unready predicate yield-spins
  (same class as regular corun with nothing stealable); bounded by the
  construction's own progress since scope work is executable by every worker.
- A scoped corunner does NOT help *other* scopes (a foreign scope's task may
  transitively wait on this one — same cycle class it exists to prevent).
- The `examples/parallel_for.cpp 100000` crash observed during regression runs
  was root-caused to the example itself, not the scheduler: the grid demos
  compute `H*W` (and `D*H*W`) flat indices and allocation sizes in `int`, which
  overflows near N=46341 for 2D and N=1290 for 3D (verified: N=1000 passes on
  pristine HEAD and this tree, N=50000 crashes on both, identically). The
  example now caps its grid dimension at 128 (`G`) with a printed notice, so
  arbitrary N remains valid for the 1D demos.
