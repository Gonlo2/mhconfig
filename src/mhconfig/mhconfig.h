#ifndef MHCONFIG__MHCONFIG_H
#define MHCONFIG__MHCONFIG_H

#include <string>

#include "spdlog/spdlog.h"

#include "mhconfig/api/service.h"
#include "mhconfig/auth/acl.h"
#include "mhconfig/worker.h"
#include "mhconfig/metrics.h"
#include "mhconfig/gc.h"

#include "mhconfig/worker/gc_merged_configs_command.h"
#include "mhconfig/worker/gc_dead_pointers_command.h"
#include "mhconfig/worker/gc_config_namespaces_command.h"
#include "mhconfig/worker/gc_raw_config_versions_command.h"

#include "jmutils/parallelism/time_worker.h"
#include "jmutils/time.h"

namespace mhconfig
{
  class MHConfig
  {
  public:
    MHConfig(
      const std::string& config_path,
      const std::string& server_address,
      const std::string& prometheus_address,
      size_t num_threads_api,
      size_t num_threads_workers
    );

    virtual ~MHConfig();

    MHConfig(const MHConfig& o) = delete;
    MHConfig(MHConfig&& o) = delete;

    MHConfig& operator=(const MHConfig& o) = delete;
    MHConfig& operator=(MHConfig&& o) = delete;

    bool run();
    bool reload();
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

    std::string config_path_;
    std::string server_address_;
    std::string prometheus_address_;
    size_t num_threads_api_;
    size_t num_threads_workers_;

    std::vector<std::unique_ptr<Worker>> workers_;
    std::unique_ptr<api::Service> service_;
    jmutils::TimeWorker time_worker_;
    std::unique_ptr<context_t> ctx_;

    bool running_{false};

    bool run_time_worker();

  };
} /* mhconfig */

#endif /* ifndef MHCONFIG__MHCONFIG_H */
