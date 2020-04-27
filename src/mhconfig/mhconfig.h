#ifndef MHCONFIG__MHCONFIG_H
#define MHCONFIG__MHCONFIG_H

#include <string>

#include "jmutils/container/queue.h"

#include "mhconfig/api/service.h"
//#include "mhconfig/worker/scheduler.h"
//#include "mhconfig/worker/builder.h"
//#include "mhconfig/worker/common.h"

#include "spdlog/spdlog.h"

#include "mhconfig/metrics.h"

namespace mhconfig
{
  using jmutils::container::Queue;

  class MHConfig
  {
  public:
    MHConfig(
      const std::string& server_address,
      const std::string& prometheus_address
    ) :
      server_address_(server_address),
      metrics_(prometheus_address)
    {};

    virtual ~MHConfig() {};

    bool run() {
      if (running_) return false;

      metrics_.init();
//
//      workers_.reserve(2);
//      workers_.emplace_back(scheduler_queue_, worker_queue_, 8, metrics_);
//
//      for (auto& w : workers_) w.start();
//
//      scheduler_ = std::make_unique<worker::Scheduler>(
//        scheduler_queue_,
//        worker_queue_,
//        metrics_
//      );
//      scheduler_->start();
//
//      service_ = std::make_unique<api::Service>(
//        server_address_,
//        scheduler_queue_,
//        metrics_
//      );
//      service_->start();
//
//      gc_thread_ = std::make_unique<std::thread>(&MHConfig::run_gc, this);
//
      running_ = true;
      return true;
    }

    bool join() {
      if (!running_) return false;

      //gc_thread_->join();
      //service_->join();
      //scheduler_->join();
      //for (auto& w : workers_) w.join();

      return true;
    }

  private:
    std::string server_address_;

    Metrics metrics_;

    Queue<mhconfig::scheduler::command::CommandRef> scheduler_queue_;
    Queue<mhconfig::worker::command::CommandRef> worker_queue_;

    //std::vector<worker::Builder> workers_;
    //std::unique_ptr<worker::Scheduler> scheduler_;
    //std::unique_ptr<api::Service> service_;
    //std::unique_ptr<std::thread> gc_thread_;

    volatile bool running_{false};

    void run_gc() {
//
//      uint32_t default_remaining_checks[6] = {1, 5, 17, 7, 11, 3};
//      uint32_t remaining_checks[6] = {1, 5, 17, 7, 11, 3};
//
//      uint32_t seconds_between_checks = 20;
//      uint32_t max_live_in_seconds = 10;
//
//      auto next_check_time = std::chrono::system_clock::now();
//      while (true) {
//        for (int i = 0; i < 6; ++i) {
//          if (remaining_checks[i] == 0) {
//            worker::command::command_t command;
//            command.type = worker::command::CommandType::RUN_GC_REQUEST;
//            command.run_gc_request = std::make_shared<worker::command::run_gc::request_t>();
//            command.run_gc_request->max_live_in_seconds = max_live_in_seconds;
//            command.run_gc_request->type = static_cast<worker::command::run_gc::Type>(i);
//
//            scheduler_queue_.push(command);
//
//            remaining_checks[i] = default_remaining_checks[i];
//          } else {
//            --remaining_checks[i];
//          }
//        }
//
//        next_check_time += std::chrono::seconds(seconds_between_checks);
//        std::this_thread::sleep_until(next_check_time);
//      }
//
    }
  };
} /* mhconfig */

#endif /* ifndef MHCONFIG__MHCONFIG_H */
