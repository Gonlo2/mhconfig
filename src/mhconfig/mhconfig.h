#ifndef MHCONFIG__MHCONFIG_H
#define MHCONFIG__MHCONFIG_H

#include <string>

#include "spdlog/spdlog.h"

#include "mhconfig/api/service.h"
#include "mhconfig/scheduler.h"
#include "mhconfig/worker.h"
#include "mhconfig/scheduler/run_gc_command.h"
#include "mhconfig/metrics/sync_metrics_service.h"
#include "mhconfig/metrics/async_metrics_service.h"
#include "mhconfig/metrics/metrics_worker.h"

#include "jmutils/parallelism/time_worker.h"
#include "jmutils/time.h"

namespace mhconfig
{
  class MHConfig
  {
  public:
    MHConfig(
      const std::string& server_address,
      const std::string& prometheus_address,
      size_t num_threads_api,
      size_t num_threads_workers
    );

    virtual ~MHConfig();

    bool run();
    bool join();

  private:
    enum class TimeWorkerTag {
      RUN_GC_CACHE_GENERATION_0,
      RUN_GC_CACHE_GENERATION_1,
      RUN_GC_CACHE_GENERATION_2,
      RUN_GC_DEAD_POINTERS,
      RUN_GC_NAMESPACES,
      RUN_GC_VERSIONS,
    };

    std::string server_address_;
    metrics::SyncMetricsService sync_metrics_service_;
    size_t num_threads_api_;
    size_t num_threads_workers_;

    SchedulerQueue scheduler_queue_;
    WorkerQueue worker_queue_;
    ::mhconfig::metrics::MetricsQueue metrics_queue_;

    std::unique_ptr<Scheduler> scheduler_;
    std::vector<std::unique_ptr<Worker>> workers_;
    std::unique_ptr<api::Service> service_;
    std::unique_ptr<metrics::MetricsWorker> metrics_worker_;
    jmutils::TimeWorker time_worker_;

    bool running_{false};

    bool run_time_worker();

  };
} /* mhconfig */

#endif /* ifndef MHCONFIG__MHCONFIG_H */
