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




