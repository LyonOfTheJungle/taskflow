#pragma once

#include "declarations.hpp"
#include "wsq.hpp"

#include <mutex>

/**
@file isolation.hpp
@brief work-stealing isolation scopes (TBB-style arena isolation on one executor)
*/

namespace tf {

#if TF_ISOLATION

// ----------------------------------------------------------------------------
// current-thread isolation scope
// ----------------------------------------------------------------------------

namespace pt {
/**
@private
The isolation scope the calling thread currently executes under, or nullptr.
Maintained by Executor::_invoke (adopted from the node, restored on exit) and
by tf::Executor::isolate (explicit adoption on any thread, worker or not).
Read by the scheduling paths to stamp newly created/scheduled work and to
confine corun stealing.
*/
inline thread_local IsolationScope* this_isolation {nullptr};
}  // namespace pt

// ----------------------------------------------------------------------------
// IsolationScope
// ----------------------------------------------------------------------------

/**
@class IsolationScope

@brief a work-stealing isolation scope — a lightweight arena that shares the
       executor's workers but owns its task queue

Tasks spawned under a scope (via tf::Executor::isolate, including everything
transitively spawned by such tasks: corun graphs, subflows, asyncs) are routed
to the scope's own queue instead of the worker/buffer queues. Consequences:

+ A worker corunning under the scope executes scope work only — it can never
  steal an unrelated outer task. This removes the classic re-entrant
  self-steal deadlock: an outer task that (transitively) waits on the scoped
  computation can never end up beneath that computation on the same stack.
+ Any free worker may still pick up scope work (helpers), so the scoped
  computation can use the executor's full parallelism.
+ Scope work never blocks behind outer work in a queue, and outer work never
  blocks behind scope work.

Scopes are created with tf::Executor::make_isolation_scope() and must not
outlive their executor. All work spawned under a scope must be joined (e.g.
by the corun that spawned it) before the last shared handle is released —
the same rule as tf::TaskGroup. Scope storage is pooled inside the executor,
so a helper racing a scope release only ever touches valid memory.
*/
class IsolationScope {

  friend class Executor;

  public:

  IsolationScope() = default;

  IsolationScope(const IsolationScope&) = delete;
  IsolationScope(IsolationScope&&) = delete;
  IsolationScope& operator = (const IsolationScope&) = delete;
  IsolationScope& operator = (IsolationScope&&) = delete;

  /**
  @brief queries whether the scope has no queued task (approximate)
  */
  bool empty() const { return _queue.empty(); }

  /**
  @brief queries the number of queued tasks (approximate)
  */
  size_t size() const { return _queue.size(); }

  private:

  // push is serialized by the mutex (multiple producers); steal is the
  // lock-free thief-side operation — the same protocol as the executor's
  // overflow buffers.
  std::mutex _mutex;
  UnboundedWSQ<Node*> _queue;

  // registry slot index while active; owned by the executor
  size_t _slot {0};
};

// ----------------------------------------------------------------------------
// ScopedIsolation
// ----------------------------------------------------------------------------

/**
@private
RAII adoption of an isolation scope on the calling thread; restores the
previous scope (which may be another scope — nesting is supported) on exit.
*/
class ScopedIsolation {

  public:

  explicit ScopedIsolation(IsolationScope* s) : _prev {pt::this_isolation} {
    pt::this_isolation = s;
  }

  ~ScopedIsolation() {
    pt::this_isolation = _prev;
  }

  ScopedIsolation(const ScopedIsolation&) = delete;
  ScopedIsolation& operator = (const ScopedIsolation&) = delete;

  private:

  IsolationScope* _prev;
};

#endif  // TF_ISOLATION

}  // end of namespace tf ------------------------------------------------------
