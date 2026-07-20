#pragma once

/**
@def TF_MAX_PRIORITY

@brief the number of distinct task priority levels recognized by the scheduler

Each worker maintains one work-stealing queue per priority level, and a freed
worker always drains higher-priority queues before lower-priority ones.
By default there are three levels (see tf::TaskPriority).

Defining @c TF_MAX_PRIORITY to @c 1 collapses every per-priority queue and loop
to a single level, restoring the exact single-queue scheduling (and codegen) of
a priority-agnostic build at zero overhead. It must be at least @c 1 and cover
every value of tf::TaskPriority you intend to use.
*/
#ifndef TF_MAX_PRIORITY
  #define TF_MAX_PRIORITY 3
#endif

/**
@def TF_RESERVED_STALL_MS

@brief detection window (milliseconds) of the reserved-worker deadlock valve

A reserved high-priority worker normally refuses all lower-priority work. If
the general workers make no progress for roughly this long while
lower-priority work is pending and the reserved worker sits idle, the reserved
worker temporarily assists with lower-priority work, reverting to strict
high-only the moment the general pool progresses again. This prevents a hard
deadlock when every general worker is occupied by blocking work whose progress
(transitively) depends on a pending default-priority task.
*/
#ifndef TF_RESERVED_STALL_MS
  #define TF_RESERVED_STALL_MS 100
#endif

/**
@def TF_ISOLATION

@brief compile-time gate for work-stealing isolation scopes

When enabled (the default), tf::Executor::make_isolation_scope() and
tf::Executor::isolate() provide TBB-style isolation: tasks belonging to an
isolation scope live in the scope's own queue, workers executing scope work
adopt the scope for the duration (including their nested corun stealing,
which is then confined to the scope), and free workers may still help with
any scope's work. This is the primitive that makes "several threads join a
computation without ever stealing unrelated outer tasks" deadlock-free on a
single executor.

Defining @c TF_ISOLATION to @c 0 removes the per-node scope pointer, all
scope queues, and every hot-path check, restoring the exact pre-isolation
codegen.
*/
#ifndef TF_ISOLATION
  #define TF_ISOLATION 1
#endif

namespace tf {

// ----------------------------------------------------------------------------
// Task Priority
// ----------------------------------------------------------------------------

/**
@enum TaskPriority

@brief enumeration of all task priority values

A task's priority is a scheduling @em hint: when a worker becomes free it always
picks a ready task of the highest available priority before any lower-priority
task. Lower enumerator value means higher priority, so tf::TaskPriority::HIGH
(0) is scheduled ahead of tf::TaskPriority::NORMAL (1), which is scheduled ahead
of tf::TaskPriority::LOW (2).

@attention
Priority controls only the @em order in which @em queued tasks are picked up. The
scheduler is non-preemptive: a task already running on a worker is never
interrupted, so a high-priority task can still wait for an in-flight lower-priority
task to finish. Reserve worker capacity if you need a stronger latency guarantee.
*/
enum class TaskPriority : unsigned {
  /** @brief highest priority; scheduled before all others */
  HIGH = 0,
  /** @brief default priority */
  NORMAL = 1,
  /** @brief lowest priority; scheduled only when no higher-priority task is ready */
  LOW = 2
};

// ----------------------------------------------------------------------------
// taskflow
// ----------------------------------------------------------------------------

class Algorithm;
class Node;
class Graph;
class FlowBuilder;
class Semaphore;
class Subflow;
class Runtime;
class NonpreemptiveRuntime;
class TaskGroup;
class Task;
class TaskView;
class Taskflow;
class AsyncTask;
class Topology;
class Executor;
class Worker;
class WorkerView;
class ObserverInterface;
class ChromeTracingObserver;
class TFProfObserver;
class TFProfManager;
class ExplicitAnchorGuard;
#if TF_ISOLATION
class IsolationScope;
#endif

template <typename T>
class Future;

template <typename...Fs>
class Pipeline;

// ----------------------------------------------------------------------------
// cudaFlow
// ----------------------------------------------------------------------------
class cudaFlowNode;
class cudaFlowGraph;
class cudaTask;
class cudaFlow;
class cudaFlowCapturer;
class cudaFlowOptimizerBase;
class cudaFlowLinearOptimizer;
class cudaFlowSequentialOptimizer;
class cudaFlowRoundRobinOptimizer;

template <typename C, typename D>
class cudaGraphExecBase;

// ----------------------------------------------------------------------------
// struct 
// ----------------------------------------------------------------------------
class TaskParams;
class DefaultTaskParams;


}  // end of namespace tf -----------------------------------------------------




