#ifndef MHCONFIG__MHCONFIG_H
#define MHCONFIG__MHCONFIG_H

#include <string>

#include "mhconfig/common.h"
#include "mhconfig/api/service.h"
#include "mhconfig/scheduler/mod.h"
#include "mhconfig/worker/mod.h"

#include "spdlog/spdlog.h"

#include "mhconfig/scheduler/command/run_gc_command.h"
#include "mhconfig/scheduler/command/obtain_usage_metrics_command.h"
#include "mhconfig/metrics/sync_metrics_service.h"
#include "mhconfig/metrics/async_metrics_service.h"
#include "mhconfig/metrics/metrics_worker.h"

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
    ) :
      server_address_(server_address),
      sync_metrics_service_(prometheus_address),
      num_threads_api_(num_threads_api),
      num_threads_workers_(num_threads_workers)
    {};

    virtual ~MHConfig() {};

    bool run() {
      if (running_) return false;

      sync_metrics_service_.init();

      workers_.reserve(num_threads_workers_);
      for (size_t i = 0; i < num_threads_workers_; ++i) {
        auto worker = std::make_unique<mhconfig::worker::Worker>(
          worker_queue_.new_receiver(),
          mhconfig::worker::command::Command::context_t(
            scheduler_queue_.new_sender(),
            std::make_unique<metrics::AsyncMetricsService>(metrics_queue_.new_sender()),
            sync_metrics_service_
          )
        );
        if (!worker->start()) return false;
        workers_.push_back(std::move(worker));
      }

      std::vector<std::pair<SchedulerQueue::SenderRef, metrics::AsyncMetricsService>> thread_vars;
      thread_vars.reserve(num_threads_api_);
      for (size_t i = 0; i < num_threads_api_; ++i) {
        thread_vars.emplace_back(
          scheduler_queue_.new_sender(),
          metrics::AsyncMetricsService(metrics_queue_.new_sender())
        );
      }
      service_ = std::make_unique<api::Service>(
        server_address_,
        std::move(thread_vars)
      );
      service_->start();

      gc_thread_ = std::make_unique<std::thread>(
        &MHConfig::run_gc,
        this,
        scheduler_queue_.new_sender()
      );

      scheduler_ = std::make_unique<mhconfig::scheduler::Scheduler>(
        scheduler_queue_,
        worker_queue_,
        std::make_unique<metrics::AsyncMetricsService>(metrics_queue_.new_sender())
      );
      if (!scheduler_->start()) return false;

      metrics_worker_ = std::make_unique<::mhconfig::metrics::MetricsWorker>(
        metrics_queue_,
        sync_metrics_service_
      );
      metrics_worker_->start();

      running_ = true;
      return true;
    }

    bool join() {
      if (!running_) return false;

      gc_thread_->join();
      service_->join();
      scheduler_->join();
      for (auto& worker : workers_) {
        worker->join();
      }

      return true;
    }

  private:
    std::string server_address_;
    metrics::SyncMetricsService sync_metrics_service_;
    size_t num_threads_api_;
    size_t num_threads_workers_;

    SchedulerQueue scheduler_queue_;
    WorkerQueue worker_queue_;
    ::mhconfig::metrics::MetricsQueue metrics_queue_;

    std::unique_ptr<mhconfig::scheduler::Scheduler> scheduler_;
    std::vector<std::unique_ptr<mhconfig::worker::Worker>> workers_;
    std::unique_ptr<api::Service> service_;
    std::unique_ptr<::mhconfig::metrics::MetricsWorker> metrics_worker_;
    std::unique_ptr<std::thread> gc_thread_;

    volatile bool running_{false};

    void run_gc(SchedulerQueue::SenderRef sender) {
      uint32_t default_remaining_checks[7] = {1, 5, 17, 7, 11, 3, 4};
      uint32_t remaining_checks[7] = {1, 5, 17, 7, 11, 3, 4};

      uint32_t seconds_between_checks = 20;
      uint32_t max_live_in_seconds = 10;

      auto next_check_time = std::chrono::system_clock::now();
      while (true) {
        for (int i = 0; i < 7; ++i) {
          if (remaining_checks[i] == 0) {
            if (i == 6) { // TODO Extract this logic to a time worker
              sender->push(
                std::make_unique<scheduler::command::ObtainUsageMetricsCommand>()
              );
            } else {
              sender->push(
                std::make_unique<scheduler::command::RunGcCommand>(
                  static_cast<scheduler::command::RunGcCommand::Type>(i),
                  max_live_in_seconds
                )
              );
            }

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
