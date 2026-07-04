// This example simulates a latency-critical "UI render" workload competing
// with a throughput-oriented "background IO" workload on the same executor.
//
// Motivation
// ----------
// A common real-world pattern is to render a window at a steady frame rate
// (e.g., 60 FPS -> one frame every ~16 ms) while a pile of background IO tasks
// (loading, (de)serialization, streaming, compression, ...) runs on the same
// executor. When the IO tasks are large, they occupy every worker thread, and a
// freshly-submitted render task must wait in the work-stealing queues behind
// them. The frame is delivered late and the UI visibly stalls.
//
// Taskflow's scheduler is *non-preemptive*: once a worker picks up a task it
// runs it to completion. Historically there was also no notion of task
// priority, so a render task had no way to jump ahead of the queued IO work and
// the only recourse was a second, dedicated executor for the UI. This fork adds
// both remedies: tf::TaskPriority (which queued task a freed worker picks next)
// and reserved worker capacity (tf::Executor(N, num_reserved) keeps workers
// exclusively for HIGH tasks).
//
// This example measures per-frame render latency under four conditions:
//   (1) render loop alone (baseline: what "good" looks like),
//   (2) render loop while the executor is flooded with large IO tasks,
//   (3) the same flood, but render tagged HIGH and IO tagged LOW,
//   (4) same as (3) on an executor with one reserved high-priority worker.
//
// On a build with priority scheduling compiled out (/DTF_MAX_PRIORITY=1) the
// priority tags and the reserved count are ignored, so (3) and (4) look like
// (2) -- that gate is how the fork keeps priority support zero-cost when unused.
//
// NOTE ON EXPECTATIONS
// --------------------
// Priority ordering only controls *which queued task a freed worker picks next*.
// It cannot preempt an IO task that is already mid-flight. With W workers busy on
// long IO tasks, a new high-priority render task still waits (worst case) for the
// earliest of those W tasks to finish. So priority alone (3) shrinks the queueing
// delay to roughly one IO-task duration; getting below that requires keeping IO
// tasks small/chunked or reserving worker capacity (4), because a reserved worker
// is parked (not running IO) when the render task arrives. This example is built
// to make that distinction visible in the numbers.

#include <taskflow/taskflow.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------
// Tunable scenario parameters
// ----------------------------------------------------------------------------

constexpr int    NUM_WORKERS   = 4;      // executor worker threads
constexpr int    NUM_FRAMES    = 48;     // frames to render per scenario
constexpr double FRAME_MS      = 16.0;   // target frame period (60 FPS)
constexpr double RENDER_MS     = 2.0;    // CPU work per render task
constexpr double IO_MS         = 40.0;   // CPU work per background IO task

// Cap the number of IO tasks in flight at any moment. We keep a backlog several
// times the worker count so a newly-submitted render task must queue behind a
// deep run of IO work -- this makes the stall large and stable (so it dominates
// run-to-run scheduling noise). A small, bounded cap also means that when we
// stop flooding, the executor drains in ~one IO-task duration instead of
// leaving hundreds of queued 40 ms tasks to grind through (which looks like a
// hang). Expected stall ~ (MAX_INFLIGHT_IO / NUM_WORKERS) * IO_MS.
constexpr int    MAX_INFLIGHT_IO = 8 * NUM_WORKERS;

using clock_type = std::chrono::steady_clock;

// ----------------------------------------------------------------------------
// busy_wait: occupy the calling worker for approximately `ms` milliseconds.
//
// We intentionally burn CPU rather than sleep(): a large IO task keeps its
// worker thread occupied while it copies/parses/compresses data, and that is
// exactly the "worker is unavailable" condition that stalls the UI. A sleeping
// task would also hold the worker, but busy-waiting keeps the timing
// independent of the OS timer granularity and makes the demo reproducible.
// ----------------------------------------------------------------------------
inline void busy_wait(double ms) {
  const auto span = std::chrono::duration<double, std::milli>(ms);
  const auto stop = clock_type::now() + std::chrono::duration_cast<clock_type::duration>(span);
  // volatile sink so the compiler cannot optimize the loop away
  volatile std::uint64_t sink = 0;
  while(clock_type::now() < stop) {
    sink += 1;
  }
  (void)sink;
}

// ----------------------------------------------------------------------------
// Latency statistics helper
// ----------------------------------------------------------------------------
struct Stats {
  double min, avg, p50, p95, max;
  int    missed;   // frames that missed the FRAME_MS deadline
};

Stats summarize(std::vector<double> lat) {
  Stats s{};
  std::sort(lat.begin(), lat.end());
  const size_t n = lat.size();
  double sum = 0.0;
  s.missed = 0;
  for(double v : lat) {
    sum += v;
    if(v > FRAME_MS) ++s.missed;
  }
  s.min = lat.front();
  s.max = lat.back();
  s.avg = sum / n;
  s.p50 = lat[std::min(n - 1, static_cast<size_t>(n * 0.50))];
  s.p95 = lat[std::min(n - 1, static_cast<size_t>(n * 0.95))];
  return s;
}

void report(const char* title, const Stats& s) {
  std::printf(
    "%-40s min=%6.2f  avg=%6.2f  p50=%6.2f  p95=%7.2f  max=%8.2f  missed=%2d/%d\n",
    title, s.min, s.avg, s.p50, s.p95, s.max, s.missed, NUM_FRAMES
  );
}

// ----------------------------------------------------------------------------
// Drive one render loop.
//
// Each "frame", the main thread waits until the next 16 ms deadline, submits a
// single render task to the executor, and blocks on its future. The recorded
// latency is (render-complete-time - frame-deadline): the time the user waits
// past the moment the frame was due. Under a saturated executor this is
// dominated by how long the render task sits queued behind IO work.
//
// `flood` controls whether the background IO tasks are submitted alongside the
// render loop. `high_priority` tags render HIGH and IO LOW.
//
// Submission model: the IO backlog is topped up by a dedicated producer thread
// running concurrently with the render loop -- the realistic pattern (a UI
// thread submitting-and-waiting while an IO subsystem floods the executor from
// another thread). This pattern used to starve intermittently: the sticky-
// victim steal loop could pin every worker to the buffer the IO flood spills
// into, leaving a render task spilled into another buffer unseen forever. The
// priority-major stealing sweep fixed that (see PRIORITY_SCHEDULING_NOTES.md).
// ----------------------------------------------------------------------------
Stats run_scenario(tf::Executor& executor, bool flood, bool high_priority) {

  // When we want the UI prioritized, background IO drops to LOW so a freed
  // worker prefers a queued render task; otherwise everything is NORMAL and
  // shares one priority (the priority-agnostic behavior).
  const tf::TaskPriority io_prio     = high_priority ? tf::TaskPriority::LOW
                                                     : tf::TaskPriority::NORMAL;
  const tf::TaskPriority render_prio = high_priority ? tf::TaskPriority::HIGH
                                                     : tf::TaskPriority::NORMAL;

  // number of IO tasks currently queued/running; the render thread refills this
  // toward MAX_INFLIGHT_IO so the workers stay saturated for the whole loop.
  std::atomic<int> io_inflight{0};
  auto submit_io = [&]{
    io_inflight.fetch_add(1, std::memory_order_relaxed);
    executor.silent_async(tf::TaskParams{.priority = io_prio}, [&]{
      busy_wait(IO_MS);
      io_inflight.fetch_sub(1, std::memory_order_relaxed);
    });
  };

  // concurrent producer: keeps the IO backlog saturated for the whole loop
  std::atomic<bool> stop_producer{false};
  std::thread producer;
  if(flood) {
    producer = std::thread([&]{
      while(!stop_producer.load(std::memory_order_relaxed)) {
        if(io_inflight.load(std::memory_order_relaxed) < MAX_INFLIGHT_IO) {
          submit_io();
        }
        else {
          std::this_thread::yield();
        }
      }
    });
    // give the flood a head start so the executor is already saturated
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  std::vector<double> latencies;
  latencies.reserve(NUM_FRAMES);

  const auto start = clock_type::now();

  for(int frame = 0; frame < NUM_FRAMES; ++frame) {

    // Pace frames at the target rate, but never chase a deadline that is
    // already in the past. This models a vsync-blocked render loop that simply
    // *drops* frames under load rather than accumulating an ever-growing
    // backlog. We timestamp the submit instant AFTER waking, so the metric is
    // the true per-frame cost (queue wait + execution) and is not polluted by
    // Windows' coarse (~15 ms) sleep granularity or cumulative lateness.
    const auto deadline = start + std::chrono::duration_cast<clock_type::duration>(
      std::chrono::duration<double, std::milli>(FRAME_MS * frame)
    );
    if(deadline > clock_type::now()) {
      std::this_thread::sleep_until(deadline);
    }

    // submit the render task at its priority and block until it completes.
    // With render_prio == HIGH and io_prio == LOW, a worker that finishes an
    // IO task drains its HIGH queue first and picks this render task ahead of
    // the queued IO backlog.
    const auto submit = clock_type::now();
    auto fu = executor.async(tf::TaskParams{.priority = render_prio},
                             []{ busy_wait(RENDER_MS); });
    fu.wait();
    const auto done = clock_type::now();

    // per-frame render latency: time from wanting the frame to having it
    const double lat = std::chrono::duration<double, std::milli>(done - submit).count();
    latencies.push_back(lat);
  }

  // stop the flood and drain the remaining IO backlog before returning
  stop_producer.store(true, std::memory_order_relaxed);
  if(producer.joinable()) {
    producer.join();
  }
  executor.wait_for_all();

  return summarize(std::move(latencies));
}

// ----------------------------------------------------------------------------
int main() {

  std::setvbuf(stdout, nullptr, _IONBF, 0);  // unbuffered: show progress live

  std::printf(
    "priority_scheduling: %d workers, %d frames @ %.0f ms, "
    "render=%.0f ms, io=%.0f ms (<=%d in flight)\n\n",
    NUM_WORKERS, NUM_FRAMES, FRAME_MS, RENDER_MS, IO_MS, MAX_INFLIGHT_IO
  );

  tf::Executor executor(NUM_WORKERS);

  // (1) render loop alone -- this is the target behavior
  auto baseline = run_scenario(executor, /*flood=*/false, /*high_priority=*/false);
  report("(1) render only (baseline)", baseline);

  // (2) render loop while flooded with large IO tasks -- the stall
  auto flooded = run_scenario(executor, /*flood=*/true, /*high_priority=*/false);
  report("(2) render + IO flood (no priority)", flooded);

  // (3) same flood, but render is HIGH priority and IO is LOW. A freed worker
  //     now picks the queued render task ahead of the IO backlog, so the stall
  //     collapses toward one in-flight IO duration (~IO_MS) instead of the full
  //     queue depth. (Getting below IO_MS additionally needs reserved workers.)
  auto prioritized = run_scenario(executor, /*flood=*/true, /*high_priority=*/true);
  report("(3) render HIGH + IO LOW", prioritized);

  // (4) same flood and priorities, but on an executor that reserves one worker
  //     exclusively for HIGH tasks. The reserved worker sits parked while only
  //     IO exists and picks up each render task the moment it is submitted, so
  //     latency drops to roughly the render cost itself -- below one IO
  //     duration, which pure priority ordering cannot achieve on a
  //     non-preemptive scheduler. (On a TF_MAX_PRIORITY=1 build the reserved
  //     count is ignored and this looks like scenario (2).)
  tf::Executor reserved_executor(NUM_WORKERS, /*num_reserved=*/1);
  auto reserved = run_scenario(reserved_executor, /*flood=*/true, /*high_priority=*/true);
  report("(4) render HIGH + IO LOW + 1 reserved", reserved);

  std::printf(
    "\nInterpretation:\n"
    "  * (1) shows near-zero latency: a free executor renders on time.\n"
    "  * (2) shows the stall: render tasks queue behind large IO tasks.\n"
    "  * (3) tags render HIGH and IO LOW: a freed worker picks the render task\n"
    "    ahead of the IO backlog, so latency collapses toward one in-flight IO\n"
    "    duration (~%.0f ms). Priority cannot beat that last ~%.0f ms without\n"
    "    reserving worker capacity (a non-preemptive scheduler cannot evict\n"
    "    an IO task already running).\n"
    "  * (4) additionally reserves one worker for HIGH tasks: the reserved\n"
    "    worker idles through the IO flood and runs each render task on\n"
    "    arrival, so latency approaches the render cost (~%.0f ms) at the\n"
    "    price of one worker's IO throughput.\n",
    IO_MS, IO_MS, RENDER_MS
  );

  return 0;
}
