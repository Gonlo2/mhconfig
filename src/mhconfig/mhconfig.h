#ifndef MHCONFIG__MHCONFIG_H
#define MHCONFIG__MHCONFIG_H

#include <string>

#include "jmutils/container/queue.h"

#include "mhconfig/api/service.h"
#include "mhconfig/scheduler/mod.h"
#include "mhconfig/worker/mod.h"
//#include "mhconfig/worker/common.h"

#include "spdlog/spdlog.h"

#include "mhconfig/metrics/sync_metrics_service.h"
#include "mhconfig/metrics/async_metrics_service.h"

namespace mhconfig
{
  using jmutils::container::Queue;

  class MHConfig
  {
  public:
    MHConfig(
      const std::string& server_address,
      const std::string& prometheus_address,
      size_t num_threads_api,
      size_t num_threads_workers
    ) :
      server_address_(server_address),
      sync_metrics_service_(prometheus_address),
      async_metrics_service_(worker_queue_),
      num_threads_api_(num_threads_api),
      num_threads_workers_(num_threads_workers)
    {};

    virtual ~MHConfig() {};

    bool run() {
      if (running_) return false;

      sync_metrics_service_.init();

      worker_ = std::make_unique<mhconfig::worker::Worker>(
        worker_queue_,
        num_threads_workers_,
        mhconfig::worker::command::Command::context_t(
          scheduler_queue_,
          async_metrics_service_,
          sync_metrics_service_
        )
      );
      worker_->start();

      scheduler_ = std::make_unique<mhconfig::scheduler::Scheduler>(
        scheduler_queue_,
        worker_queue_,
        async_metrics_service_
      );
      scheduler_->start();

      service_ = std::make_unique<api::Service>(
        server_address_,
        num_threads_api_,
        scheduler_queue_,
        async_metrics_service_
      );
      service_->start();

      gc_thread_ = std::make_unique<std::thread>(&MHConfig::run_gc, this);

      running_ = true;
      return true;
    }

    bool join() {
      if (!running_) return false;

      gc_thread_->join();
      service_->join();
      scheduler_->join();
      worker_->join();

      return true;
    }

  private:
    std::string server_address_;
    metrics::SyncMetricsService sync_metrics_service_;
    metrics::AsyncMetricsService async_metrics_service_;
    size_t num_threads_api_;
    size_t num_threads_workers_;

    Queue<mhconfig::scheduler::command::CommandRef> scheduler_queue_;
    Queue<mhconfig::worker::command::CommandRef> worker_queue_;

    std::unique_ptr<mhconfig::scheduler::Scheduler> scheduler_;
    std::unique_ptr<mhconfig::worker::Worker> worker_;
    std::unique_ptr<api::Service> service_;
    std::unique_ptr<std::thread> gc_thread_;

    volatile bool running_{false};

    void run_gc() {
      uint32_t default_remaining_checks[6] = {1, 5, 17, 7, 11, 3};
      uint32_t remaining_checks[6] = {1, 5, 17, 7, 11, 3};

      uint32_t seconds_between_checks = 20;
      uint32_t max_live_in_seconds = 10;

      auto next_check_time = std::chrono::system_clock::now();
      while (true) {
        for (int i = 0; i < 6; ++i) {
          if (remaining_checks[i] == 0) {
            auto api_run_gc_command = std::make_shared<scheduler::command::RunGcCommand>(
              static_cast<::mhconfig::api::request::run_gc::Type>(i),
              max_live_in_seconds
            );
            scheduler_queue_.push(api_run_gc_command);

            remaining_checks[i] = default_remaining_checks[i];
          } else {
            --remaining_checks[i];
          }
        }

        next_check_time += std::chrono::seconds(seconds_between_checks);
        std::this_thread::sleep_until(next_check_time);
      }

    }
  };
} /* mhconfig */

#endif /* ifndef MHCONFIG__MHCONFIG_H */
