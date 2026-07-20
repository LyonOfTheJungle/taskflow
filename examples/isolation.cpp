// Isolation-scope test & demonstration suite (fork feature, see isolation.hpp).
//
// The central scenario replicates the MeshLib livelock this feature exists to
// fix: many "outer" tasks race to use a lazily built shared cache; one thread
// is elected to construct it (the construction is internally parallel) and the
// rest wait. Without isolation, a thread corunning inside the construction can
// steal a pending outer task, which then waits — on the same stack — for the
// very construction beneath it: an unrecoverable all-core livelock.
//
//   isolation.exe                run the full safe test suite (must pass)
//   isolation.exe --unsafe       run the repro WITHOUT isolation (expected to
//                                hang — run under a watchdog/timeout; used to
//                                prove the repro actually detects the bug)
//   isolation.exe --stress N     repeat the livelock repro N times per worker
//                                count (default suite uses a smaller count)
//
// Exit code 0 = all tests passed. The unsafe mode never exits on its own when
// the bug is present.

#include <taskflow/taskflow.hpp>
#include <taskflow/algorithm/for_each.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

// ----------------------------------------------------------------------------
// tiny harness
// ----------------------------------------------------------------------------

int g_failed = 0;

void check(bool cond, const char* msg) {
  if(!cond) {
    std::printf("      CHECK FAILED: %s\n", msg);
    ++g_failed;
    throw std::runtime_error(msg);
  }
}

template <typename F>
void run_test(const char* name, F&& f) {
  using clock = std::chrono::steady_clock;
  auto t0 = clock::now();
  try {
    f();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - t0).count();
    std::printf("  [PASS] %s (%lld ms)\n", name, static_cast<long long>(ms));
  }
  catch(const std::exception& e) {
    std::printf("  [FAIL] %s: %s\n", name, e.what());
    ++g_failed;
  }
  catch(...) {
    std::printf("  [FAIL] %s: unknown exception\n", name);
    ++g_failed;
  }
}

// set while a thread is inside an isolated corun of a construction; an outer
// task must never execute nested beneath such a frame (that nesting is the
// self-steal that livelocks). Checked by every outer task body.
thread_local int tl_inside_construction = 0;

// ----------------------------------------------------------------------------
// LazyCache — the MeshLib SharedThreadSafeOwner/TbbTaskArenaAndGroup pattern,
// rebuilt on isolation scopes (safe) or raw corun_until (unsafe = the bug)
// ----------------------------------------------------------------------------

struct LazyCache {

  std::atomic<bool> elected {false};
  std::promise<void> promise;
  std::shared_future<void> ready {promise.get_future().share()};

  std::mutex scope_mutex;
  std::shared_ptr<tf::IsolationScope> scope;   // set by the elected thread

  std::atomic<long long> value {0};            // the "expensive" result
  std::atomic<int> built_count {0};            // must end up exactly 1

  // The internally parallel construction: a corun'd flow whose tasks record
  // the executing thread and sum a range. `inner` controls the fan-out.
  void build(tf::Executor& ex, int inner, std::set<std::thread::id>* builders,
             std::mutex* builders_mutex) {
    tf::Taskflow flow;
    for(int i = 0; i < inner; ++i) {
      flow.emplace([this, i, builders, builders_mutex] {
        if(builders) {
          std::scoped_lock lk(*builders_mutex);
          builders->insert(std::this_thread::get_id());
        }
        // small but nonzero work so helpers have time to join
        long long acc = 0;
        for(int k = 0; k < 20000; ++k) acc += (k ^ i);
        value.fetch_add(acc, std::memory_order_relaxed);
      });
    }
    if(ex.this_worker_id() != -1) {
      ex.corun(flow);
    }
    else {
      ex.run(flow).wait();
    }
    built_count.fetch_add(1, std::memory_order_relaxed);
  }

  // safe = the isolation-scope rendezvous this fork feature provides
  void get_or_create_safe(tf::Executor& ex, int inner,
                          std::set<std::thread::id>* builders = nullptr,
                          std::mutex* builders_mutex = nullptr) {
    if(ready.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
      return;
    }
    if(!elected.exchange(true)) {
      auto s = ex.make_isolation_scope();
      {
        std::scoped_lock lk(scope_mutex);
        scope = s;
      }
      ex.isolate(*s, [&] {
        ++tl_inside_construction;
        build(ex, inner, builders, builders_mutex);
        --tl_inside_construction;
      });
      promise.set_value();
    }
    else {
      std::shared_ptr<tf::IsolationScope> s;
      {
        std::scoped_lock lk(scope_mutex);
        s = scope;
      }
      if(ex.this_worker_id() != -1 && s) {
        // worker waiter: help the construction, and only the construction
        ex.isolate(*s, [&] {
          ++tl_inside_construction;
          ex.corun_until([&] {
            return ready.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
          });
          --tl_inside_construction;
        });
      }
      ready.wait();
    }
  }

  // unsafe = the pre-isolation pattern: elected thread builds on its own
  // corun-capable stack, waiters corun_until unscoped. This is the bug.
  void get_or_create_unsafe(tf::Executor& ex, int inner) {
    if(ready.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
      return;
    }
    if(!elected.exchange(true)) {
      build(ex, inner, nullptr, nullptr);
      promise.set_value();
    }
    else if(ex.this_worker_id() != -1) {
      ex.corun_until([&] {
        return ready.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
      });
    }
    else {
      ready.wait();
    }
  }
};

// One round of the poison scenario: `chunks` outer async tasks all demand the
// lazy cache while their siblings are still pending in the executor.
void poison_round(tf::Executor& ex, int chunks, int inner, bool safe,
                  std::set<std::thread::id>* builders = nullptr,
                  std::mutex* builders_mutex = nullptr) {
  LazyCache cache;
  std::atomic<int> done {0};
  auto tg = std::make_shared<std::promise<void>>();
  auto all_done = tg->get_future();

  for(int c = 0; c < chunks; ++c) {
    ex.silent_async([&, tg] {
      // an outer task must never run nested inside a construction frame —
      // that nesting is precisely the self-steal deadlock
      if(tl_inside_construction != 0) {
        std::printf("      VIOLATION: outer chunk executed inside an isolated construction\n");
        std::abort();
      }
      if(safe) cache.get_or_create_safe(ex, inner, builders, builders_mutex);
      else     cache.get_or_create_unsafe(ex, inner);
      if(done.fetch_add(1, std::memory_order_acq_rel) + 1 == chunks) {
        tg->set_value();
      }
    });
  }
  all_done.wait();
  check(cache.built_count.load() == 1, "cache must be built exactly once");
  check(cache.value.load() != 0, "cache value must be computed");
}

// ----------------------------------------------------------------------------
// tests (safe mode)
// ----------------------------------------------------------------------------

void test_basic_isolate(tf::Executor& ex) {
  auto s = ex.make_isolation_scope();
  std::atomic<int> n {0};
  auto fut = ex.async([&] {
    ex.isolate(*s, [&] {
      tf::Taskflow flow;
      for(int i = 0; i < 64; ++i) flow.emplace([&] { ++n; });
      ex.corun(flow);
    });
  });
  fut.wait();
  check(n.load() == 64, "all isolated tasks ran");
  check(tf::pt::this_isolation == nullptr, "scope restored on main thread");
}

void test_livelock_repro(tf::Executor& ex, int rounds) {
  for(int r = 0; r < rounds; ++r) {
    poison_round(ex, /*chunks=*/256, /*inner=*/64, /*safe=*/true);
  }
}

void test_single_worker_deterministic() {
  // 1 worker + pending sibling chunks is the deterministic form of the bug;
  // with isolation it must complete
  tf::Executor ex1(1);
  for(int r = 0; r < 10; ++r) {
    poison_round(ex1, /*chunks=*/32, /*inner=*/16, /*safe=*/true);
  }
}

void test_helpers_participate(tf::Executor& ex) {
  // free workers must be able to join a scoped construction; with many inner
  // tasks and idle workers, more than one thread should build. A 1-worker
  // executor has no helpers by definition — require exactly one there.
  const size_t want = ex.num_workers() >= 2 ? 2 : 1;
  size_t distinct = 0;
  for(int attempt = 0; attempt < 20 && distinct < want; ++attempt) {
    std::set<std::thread::id> builders;
    std::mutex m;
    poison_round(ex, /*chunks=*/8, /*inner=*/256, /*safe=*/true, &builders, &m);
    distinct = builders.size();
  }
  std::printf("      (%zu distinct builder threads)\n", distinct);
  check(distinct >= want, "expected number of workers helped the isolated construction");
}

void test_nested_scopes(tf::Executor& ex) {
  // construction A internally requires construction B (MeshLib: dipoles
  // require the AABB tree) — both isolated, B nested inside A
  LazyCache inner_cache;
  std::atomic<int> outer_done {0};
  constexpr int CH = 64;
  std::promise<void> all;
  auto all_fut = all.get_future();

  LazyCache outer_cache;
  // hijack build: outer's construction demands inner first
  auto outer_get = [&](tf::Executor& e) {
    if(outer_cache.ready.wait_for(std::chrono::seconds(0)) == std::future_status::ready) return;
    if(!outer_cache.elected.exchange(true)) {
      auto s = e.make_isolation_scope();
      {
        std::scoped_lock lk(outer_cache.scope_mutex);
        outer_cache.scope = s;
      }
      e.isolate(*s, [&] {
        inner_cache.get_or_create_safe(e, 32);   // nested isolated construction
        outer_cache.build(e, 32, nullptr, nullptr);
      });
      outer_cache.promise.set_value();
    }
    else {
      std::shared_ptr<tf::IsolationScope> s;
      {
        std::scoped_lock lk(outer_cache.scope_mutex);
        s = outer_cache.scope;
      }
      if(e.this_worker_id() != -1 && s) {
        e.isolate(*s, [&] {
          e.corun_until([&] {
            return outer_cache.ready.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
          });
        });
      }
      outer_cache.ready.wait();
    }
  };

  for(int c = 0; c < CH; ++c) {
    ex.silent_async([&] {
      outer_get(ex);
      if(outer_done.fetch_add(1, std::memory_order_acq_rel) + 1 == CH) all.set_value();
    });
  }
  all_fut.wait();
  check(outer_cache.built_count.load() == 1, "outer built once");
  check(inner_cache.built_count.load() == 1, "inner built once");
}

void test_exception_propagation(tf::Executor& ex) {
  auto s = ex.make_isolation_scope();
  bool caught = false;
  auto prev = tf::pt::this_isolation;
  auto fut = ex.async([&] {
    try {
      ex.isolate(*s, [&] {
        tf::Taskflow flow;
        flow.emplace([] { throw std::runtime_error("boom"); });
        for(int i = 0; i < 16; ++i) flow.emplace([] {});
        ex.corun(flow);
      });
    }
    catch(const std::runtime_error&) {
      caught = true;
    }
  });
  fut.wait();
  check(caught, "exception propagated out of isolated corun");
  check(tf::pt::this_isolation == prev, "thread scope restored after exception");
}

void test_nonworker_isolate(tf::Executor& ex) {
  // a non-worker (this thread) submits the whole construction under a scope;
  // workers must pick it up from the scope queue and complete it
  auto s = ex.make_isolation_scope();
  std::atomic<int> n {0};
  ex.isolate(*s, [&] {
    tf::Taskflow flow;
    flow.emplace([&](tf::Runtime& rt) {
      for(int i = 0; i < 32; ++i) rt.silent_async([&] { ++n; });
      rt.corun();
    });
    ex.run(flow).wait();
  });
  check(n.load() == 32, "non-worker-submitted isolated flow completed");
}

void test_scope_pool(tf::Executor& ex) {
  // sequential churn far past the slot limit: pooling must recycle
  for(int i = 0; i < 500; ++i) {
    auto s = ex.make_isolation_scope();
    check(s != nullptr, "scope created");
  }
  // concurrent limit: 64 live scopes is the documented cap
  std::vector<std::shared_ptr<tf::IsolationScope>> live;
  for(int i = 0; i < 64; ++i) live.push_back(ex.make_isolation_scope());
  bool threw = false;
  try { auto s65 = ex.make_isolation_scope(); }
  catch(const std::runtime_error&) { threw = true; }
  check(threw, "65th concurrent scope throws");
  live.clear();
  auto again = ex.make_isolation_scope();
  check(again != nullptr, "slots reusable after release");
}

void test_priority_interplay(tf::Executor& ex) {
#if TF_MAX_PRIORITY > 1
  // HIGH-priority outer tasks keep flowing while an isolated construction
  // runs; everything completes
  std::atomic<int> high_done {0};
  tf::Taskflow high;
  for(int i = 0; i < 64; ++i) {
    high.emplace([&] { ++high_done; }).priority(tf::TaskPriority::HIGH);
  }
  auto fut = ex.run(high);
  poison_round(ex, 64, 64, /*safe=*/true);
  fut.wait();
  check(high_done.load() == 64, "high-priority tasks completed alongside isolation");
#else
  (void)ex;
#endif
}

void test_task_group_under_isolation(tf::Executor& ex) {
  // tf::TaskGroup fan-out inside an isolated construction (the MeshLib
  // tbb::task_group shim pattern) must stay inside the scope and complete
  auto s = ex.make_isolation_scope();
  std::atomic<int> n {0};
  auto fut = ex.async([&] {
    ex.isolate(*s, [&] {
      tf::Taskflow flow;
      flow.emplace([&](tf::Runtime& rt) {
        for(int i = 0; i < 64; ++i) rt.silent_async([&] { ++n; });
        rt.corun();
      });
      ex.corun(flow);
    });
  });
  fut.wait();
  check(n.load() == 64, "task-group style fan-out completed under isolation");
}

// ----------------------------------------------------------------------------
// wave 2: documented feature surfaces × isolation
// (subflows, composition, condition loops, dependent asyncs, semaphores,
//  repeat runs, cancellation, reserved workers, multi-scope fuzz)
// ----------------------------------------------------------------------------

// generic poisoned rendezvous around an arbitrary construction body: `chunks`
// outer tasks race; one is elected and runs build(ex, scope) under isolation;
// the rest help via scoped corun_until. Outer bodies re-assert they never run
// nested inside a construction.
template <typename BuildFn>
void poison_round_custom(tf::Executor& ex, int chunks, BuildFn&& build) {

  struct Rendezvous {
    std::atomic<bool> elected {false};
    std::promise<void> promise;
    std::shared_future<void> ready;
    std::mutex m;
    std::shared_ptr<tf::IsolationScope> scope;
    Rendezvous() : ready(promise.get_future().share()) {}
  } rv;

  std::atomic<int> done {0};
  std::promise<void> all;
  auto all_fut = all.get_future();

  for(int c = 0; c < chunks; ++c) {
    ex.silent_async([&] {
      if(tl_inside_construction != 0) {
        std::printf("      VIOLATION: outer chunk nested inside a construction\n");
        std::abort();
      }
      if(rv.ready.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
        if(!rv.elected.exchange(true)) {
          auto s = ex.make_isolation_scope();
          {
            std::scoped_lock lk(rv.m);
            rv.scope = s;
          }
          ex.isolate(*s, [&] {
            ++tl_inside_construction;
            build(ex, s);
            --tl_inside_construction;
          });
          rv.promise.set_value();
        }
        else {
          std::shared_ptr<tf::IsolationScope> s;
          {
            std::scoped_lock lk(rv.m);
            s = rv.scope;
          }
          if(ex.this_worker_id() != -1 && s) {
            ex.isolate(*s, [&] {
              ex.corun_until([&] {
                return rv.ready.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
              });
            });
          }
          rv.ready.wait();
        }
      }
      if(done.fetch_add(1, std::memory_order_acq_rel) + 1 == chunks) {
        all.set_value();
      }
    });
  }
  all_fut.wait();
}

// asserts the executing thread currently runs under `expected`
void check_scope(const std::shared_ptr<tf::IsolationScope>& expected, const char* where) {
  if(tf::pt::this_isolation != expected.get()) {
    std::printf("      SCOPE VIOLATION in %s\n", where);
    std::abort();
  }
}

void test_subflow_under_isolation(tf::Executor& ex) {
  // recursive subflows ("a subflow can be nested or recursive") inside a
  // poisoned construction; every leaf asserts it inherited the scope
  std::atomic<int> leaves {0};
  poison_round_custom(ex, 64, [&](tf::Executor& e, const std::shared_ptr<tf::IsolationScope>& s) {
    tf::Taskflow flow;
    // fib-style recursion, depth 5 → 2^5 leaves
    std::function<void(tf::Subflow&, int)> rec = [&](tf::Subflow& sf, int d) {
      check_scope(s, "subflow body");
      if(d == 0) {
        ++leaves;
        return;
      }
      sf.emplace([&rec, d](tf::Subflow& sf2) { rec(sf2, d - 1); });
      sf.emplace([&rec, d](tf::Subflow& sf2) { rec(sf2, d - 1); });
      sf.join();
    };
    flow.emplace([&rec](tf::Subflow& sf) { rec(sf, 5); });
    if(e.this_worker_id() != -1) e.corun(flow); else e.run(flow).wait();
  });
  check(leaves.load() == 32, "all recursive subflow leaves ran (32 = 2^5)");
}

void test_module_under_isolation(tf::Executor& ex) {
  // a module task ("runs the entire dependency graph ... within the execution
  // schedule of the parent") corun'd inside a construction — exercises the
  // thread-scope stamping branch of _set_up_graph where tpg is non-null but
  // parent is the module node
  std::atomic<int> n {0};
  poison_round_custom(ex, 64, [&](tf::Executor& e, const std::shared_ptr<tf::IsolationScope>& s) {
    tf::Taskflow inner;
    for(int i = 0; i < 32; ++i) {
      inner.emplace([&] {
        check_scope(s, "module inner task");
        ++n;
      });
    }
    tf::Taskflow outer;
    outer.composed_of(inner);
    if(e.this_worker_id() != -1) e.corun(outer); else e.run(outer).wait();
  });
  check(n.load() == 32, "module-composed tasks all ran under isolation");
}

void test_condition_loop_under_isolation(tf::Executor& ex) {
  // condition-task cycle (retry loop) inside a construction — exercises the
  // continuation-cache re-adoption per iteration
  std::atomic<int> iters {0};
  poison_round_custom(ex, 64, [&](tf::Executor& e, const std::shared_ptr<tf::IsolationScope>& s) {
    tf::Taskflow flow;
    int local = 0;
    auto init = flow.emplace([] {});   // the loop needs a source node; body's
                                       // only other predecessor is the weak
                                       // back-edge from the condition task
    auto body = flow.emplace([&, s] {
      check_scope(s, "condition-loop body");
      ++local;
      iters.fetch_add(1, std::memory_order_relaxed);
    });
    auto cond = flow.emplace([&, s]() -> int {
      check_scope(s, "condition task");
      return local < 10 ? 0 : 1;   // 0 → loop back to body, 1 → exit
    });
    auto done = flow.emplace([] {});
    init.precede(body);
    body.precede(cond);
    cond.precede(body, done);
    if(e.this_worker_id() != -1) e.corun(flow); else e.run(flow).wait();
  });
  check(iters.load() == 10, "condition loop iterated exactly 10 times under isolation");
}

void test_dependent_async_under_isolation(tf::Executor& ex) {
  // dependent-async chains created inside a construction: stamped at creation,
  // scheduled later by whichever worker finishes the predecessor. Joined via a
  // scoped corun (never a blocking get() — that would strand a 1-worker pool)
  std::atomic<int> ran {0};
  poison_round_custom(ex, 64, [&](tf::Executor& e, const std::shared_ptr<tf::IsolationScope>& s) {
    constexpr int CHAINS = 8, LEN = 4;
    std::atomic<int> chain_done {0};
    for(int c = 0; c < CHAINS; ++c) {
      tf::AsyncTask prev;
      for(int i = 0; i < LEN; ++i) {
        auto fn = [&, s, i] {
          check_scope(s, "dependent async");
          ran.fetch_add(1, std::memory_order_relaxed);
          if(i == LEN - 1) chain_done.fetch_add(1, std::memory_order_acq_rel);
        };
        prev = (i == 0) ? e.silent_dependent_async(fn)
                        : e.silent_dependent_async(fn, prev);
      }
    }
    if(e.this_worker_id() != -1) {
      e.corun_until([&] { return chain_done.load(std::memory_order_acquire) == CHAINS; });
    }
    else {
      while(chain_done.load(std::memory_order_acquire) != CHAINS) std::this_thread::yield();
    }
  });
  check(ran.load() == 8 * 4, "all dependent-async chain links ran under isolation");
}

void test_semaphore_cross_context(tf::Executor& ex) {
  // a semaphore contended by isolated construction tasks AND outer tasks:
  // releasing inside the scope must schedule the foreign waiter into the
  // normal queues (routing is by node tag, not by the releasing thread)
  tf::Semaphore sem(1);
  std::atomic<int> outer_n {0}, iso_n {0};

  tf::Taskflow outer_flow;
  for(int i = 0; i < 16; ++i) {
    auto t = outer_flow.emplace([&] { ++outer_n; });
    t.acquire(sem);
    t.release(sem);
  }
  auto outer_fut = ex.run(outer_flow);

  poison_round_custom(ex, 32, [&](tf::Executor& e, const std::shared_ptr<tf::IsolationScope>& s) {
    tf::Taskflow flow;
    for(int i = 0; i < 16; ++i) {
      auto t = flow.emplace([&] {
        check_scope(s, "semaphored isolated task");
        ++iso_n;
      });
      t.acquire(sem);
      t.release(sem);
    }
    if(e.this_worker_id() != -1) e.corun(flow); else e.run(flow).wait();
  });

  outer_fut.wait();
  check(outer_n.load() == 16, "outer semaphored tasks all ran");
  check(iso_n.load() == 16, "isolated semaphored tasks all ran");
}

void test_run_n_repeats_isolated(tf::Executor& ex) {
  // repeat runs: the topology is re-set-up by whichever worker tears down the
  // previous iteration — nodes must be re-stamped from the scope captured at
  // submission, not from that worker's live scope
  auto s = ex.make_isolation_scope();
  std::atomic<int> n {0};
  tf::Taskflow flow;
  for(int i = 0; i < 8; ++i) {
    flow.emplace([&, s] {
      check_scope(s, "run_n body");
      ++n;
    });
  }
  ex.isolate(*s, [&] { ex.run_n(flow, 5).wait(); });
  check(n.load() == 8 * 5, "all 5 repeat runs executed isolated");
}

void test_cancel_isolated(tf::Executor& ex) {
  // cancel a large isolated topology mid-flight ("remaining tasks ... not
  // scheduled; running tasks continue to finish"), then prove the scope
  // machinery is healthy with a full poison round
  auto s = ex.make_isolation_scope();
  std::atomic<int> ran {0};
  tf::Taskflow flow;
  for(int i = 0; i < 4000; ++i) {
    flow.emplace([&] {
      ran.fetch_add(1, std::memory_order_relaxed);
      volatile int x = 0;
      for(int k = 0; k < 1000; ++k) x += k;
    });
  }
  tf::Future<void> fut;
  ex.isolate(*s, [&] { fut = ex.run(flow); });
  fut.cancel();
  fut.wait();
  const int after_cancel = ran.load();
  s.reset();
  poison_round(ex, 64, 32, /*safe=*/true);
  std::printf("      (%d of 4000 tasks ran before cancellation took effect)\n", after_cancel);
}

#if TF_MAX_PRIORITY > 1
void test_reserved_worker_stays_strict() {
  // under healthy load a strict reserved worker must never execute isolated
  // work (isolation is ordinary work to it; only the deadlock valve may widen
  // its sweep). Identify tasks via names through an observer.
  struct Recorder : tf::ObserverInterface {
    std::mutex m;
    std::vector<std::pair<size_t, std::string>> entries;
    void set_up(size_t) override {}
    void on_entry(tf::WorkerView w, tf::TaskView tv) override {
      std::scoped_lock lk(m);
      entries.emplace_back(w.id(), tv.name());
    }
    void on_exit(tf::WorkerView, tf::TaskView) override {}
  };

  tf::Executor exr(4, 1);   // worker 3 is reserved
  auto rec = exr.make_observer<Recorder>();

  // keep general workers busy but progressing (valve must not open)
  std::atomic<bool> stop {false};
  std::atomic<int> churn {0};
  std::thread feeder([&] {
    while(!stop.load(std::memory_order_relaxed)) {
      tf::Taskflow tfw;
      for(int i = 0; i < 8; ++i) tfw.emplace([&] { ++churn; }).name("general");
      exr.run(tfw).wait();
    }
  });

  auto s = exr.make_isolation_scope();
  std::atomic<int> iso_n {0};
  for(int round = 0; round < 20; ++round) {
    tf::Taskflow flow;
    for(int i = 0; i < 64; ++i) {
      flow.emplace([&] { ++iso_n; }).name("iso");
    }
    exr.isolate(*s, [&] { exr.run(flow).wait(); });
  }
  stop = true;
  feeder.join();
  exr.wait_for_all();

  size_t iso_on_reserved = 0;
  {
    std::scoped_lock lk(rec->m);
    for(auto& [wid, name] : rec->entries) {
      if(name == "iso" && wid == 3) ++iso_on_reserved;
    }
  }
  check(iso_n.load() == 20 * 64, "all isolated tasks ran");
  std::printf("      (churn=%d, iso-on-reserved=%zu)\n", churn.load(), iso_on_reserved);
  check(iso_on_reserved == 0, "strict reserved worker never executed isolated work");
}
#endif

void test_multi_scope_fuzz() {
  // several external threads churn scopes concurrently (registry claim/release
  // races, pool reuse, helpers crossing scopes) while the executor also runs
  // poisoned lazy-cache rounds; every body asserts its scope
  tf::Executor ex(4);
  constexpr int THREADS = 4, ROUNDS = 25;
  std::atomic<int> total {0};

  std::vector<std::thread> ts;
  for(int t = 0; t < THREADS; ++t) {
    ts.emplace_back([&, t] {
      std::mt19937 rng(1234 + t);
      for(int r = 0; r < ROUNDS; ++r) {
        auto s = ex.make_isolation_scope();
        const int width = 8 + int(rng() % 56);
        std::atomic<int> n {0};
        ex.isolate(*s, [&] {
          tf::Taskflow flow;
          flow.emplace([&, s, width](tf::Runtime& rt) {
            check_scope(s, "fuzz runtime task");
            for(int i = 0; i < width; ++i) {
              rt.silent_async([&, s] {
                check_scope(s, "fuzz async");
                ++n;
              });
            }
            rt.corun();
          });
          ex.run(flow).wait();
        });
        if(n.load() != width) {
          std::printf("      FUZZ VIOLATION: %d != %d\n", n.load(), width);
          std::abort();
        }
        total.fetch_add(n.load());
      }
    });
  }
  for(int r = 0; r < 10; ++r) {
    poison_round(ex, 64, 32, /*safe=*/true);
  }
  for(auto& th : ts) th.join();
  check(total.load() > 0, "fuzz did work");
}

void test_many_caches_concurrent(tf::Executor& ex) {
  // eight independent lazy caches (the many-meshes case) racing under one
  // outer task storm; every cache must be built exactly once
  constexpr int CACHES = 8, CHUNKS = 128;
  std::vector<std::unique_ptr<LazyCache>> caches;
  for(int i = 0; i < CACHES; ++i) caches.push_back(std::make_unique<LazyCache>());

  std::atomic<int> done {0};
  std::promise<void> all;
  auto all_fut = all.get_future();
  for(int c = 0; c < CHUNKS; ++c) {
    ex.silent_async([&, c] {
      for(int i = 0; i < CACHES; ++i) {
        caches[(c + i) % CACHES]->get_or_create_safe(ex, 32);
      }
      if(done.fetch_add(1, std::memory_order_acq_rel) + 1 == CHUNKS) all.set_value();
    });
  }
  all_fut.wait();
  for(auto& cp : caches) {
    check(cp->built_count.load() == 1, "each cache built exactly once");
  }
}

// ----------------------------------------------------------------------------
// driver
// ----------------------------------------------------------------------------

int run_suite(unsigned workers, int repro_rounds) {
  std::printf("=== isolation suite, %u workers ===\n", workers);
  tf::Executor ex(workers);
  run_test("basic isolate + corun",              [&] { test_basic_isolate(ex); });
  run_test("livelock repro (safe mode)",         [&] { test_livelock_repro(ex, repro_rounds); });
  run_test("helpers participate",                [&] { test_helpers_participate(ex); });
  run_test("nested scopes (A needs B)",          [&] { test_nested_scopes(ex); });
  run_test("exception propagation",              [&] { test_exception_propagation(ex); });
  run_test("non-worker isolate",                 [&] { test_nonworker_isolate(ex); });
  run_test("scope pool + 64-slot cap",           [&] { test_scope_pool(ex); });
  run_test("priority interplay",                 [&] { test_priority_interplay(ex); });
  run_test("task-group fan-out under isolation", [&] { test_task_group_under_isolation(ex); });
  // wave 2: documented feature surfaces × isolation
  run_test("recursive subflows under isolation",  [&] { test_subflow_under_isolation(ex); });
  run_test("module composition under isolation",  [&] { test_module_under_isolation(ex); });
  run_test("condition loop under isolation",      [&] { test_condition_loop_under_isolation(ex); });
  run_test("dependent-async chains under isolation", [&] { test_dependent_async_under_isolation(ex); });
  run_test("semaphore across contexts",           [&] { test_semaphore_cross_context(ex); });
  run_test("run_n repeat runs isolated",          [&] { test_run_n_repeats_isolated(ex); });
  run_test("cancel isolated topology",            [&] { test_cancel_isolated(ex); });
  run_test("many caches concurrently",            [&] { test_many_caches_concurrent(ex); });
  return g_failed;
}

} // namespace

int main(int argc, char** argv) {

  bool unsafe = false;
  int stress = 0;
  for(int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if(a == "--unsafe") unsafe = true;
    else if(a == "--stress" && i + 1 < argc) stress = std::atoi(argv[++i]);
  }

  const unsigned HW = std::thread::hardware_concurrency();

  if(unsafe) {
    // Demonstrates the bug the feature fixes: this is EXPECTED TO HANG (run
    // under an external timeout). If it returns, the machine got lucky —
    // rerun; the deterministic form uses 1 worker.
    std::printf("running UNSAFE repro (expected to hang; ctrl-c / timeout to stop)\n");
    std::printf("-- 1 worker (deterministic) --\n");
    tf::Executor ex1(1);
    poison_round(ex1, 32, 16, /*safe=*/false);
    std::printf("survived 1-worker round (unexpected but possible if no sibling was pending)\n");
    std::printf("-- %u workers, 50 rounds --\n", HW);
    tf::Executor exN(HW);
    for(int r = 0; r < 50; ++r) {
      poison_round(exN, 256, 64, /*safe=*/false);
      std::printf("round %d survived\n", r);
    }
    std::printf("UNSAFE mode completed 50 rounds without hanging — bug not reproduced\n");
    return 3;
  }

  if(stress > 0) {
    std::printf("stress: %d rounds per worker count\n", stress);
    for(unsigned w : {1u, 2u, 4u, HW}) {
      tf::Executor ex(w);
      for(int r = 0; r < stress; ++r) {
        poison_round(ex, 256, 64, /*safe=*/true);
      }
      std::printf("  %u workers: %d rounds OK\n", w, stress);
    }
    std::printf("stress PASSED\n");
    return 0;
  }

  run_suite(1, 5);
  run_suite(4, 10);
  run_suite(HW, 20);

  // standalone tests managing their own executors
  run_test("single-worker deterministic repro", [] { test_single_worker_deterministic(); });
  run_test("multi-scope external-thread fuzz",  [] { test_multi_scope_fuzz(); });
#if TF_MAX_PRIORITY > 1
  run_test("reserved worker stays strict",      [] { test_reserved_worker_stays_strict(); });
#endif

  std::printf("=== total failures: %d ===\n", g_failed);
  return g_failed == 0 ? 0 : 1;
}
