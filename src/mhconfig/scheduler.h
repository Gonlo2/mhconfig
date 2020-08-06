#ifndef MHCONFIG__SCHEDULER_H
#define MHCONFIG__SCHEDULER_H

#include <thread>
#include <chrono>

#include "jmutils/container/queue.h"
#include "jmutils/time.h"
#include "jmutils/parallelism/worker.h"
#include "jmutils/string/pool.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/command.h"
#include "mhconfig/worker/setup_command.h"
#include "mhconfig/worker/unregister_watchers_command.h"

namespace mhconfig
{

using namespace mhconfig::api::request;

class Scheduler : public jmutils::Worker<Scheduler, SchedulerCommandRef>
{
public:
  Scheduler(
    SchedulerQueue& scheduler_queue,
    WorkerQueue& worker_queue,
    std::unique_ptr<metrics::MetricsService>&& metrics
  );

  virtual ~Scheduler();

private:
  friend class jmutils::Worker<Scheduler, SchedulerCommandRef>;

  enum class ConfigNamespaceState {
    OK,
    BUILDING,
    ERROR
  };

  SchedulerQueue& scheduler_queue_;
  SchedulerCommand::context_t context_;

  void on_start() noexcept {
  }

  inline bool pop(
    SchedulerCommandRef& command
  ) noexcept {
    scheduler_queue_.pop(command);
    return true;
  }

  inline bool metricate(
    SchedulerCommandRef& command,
    uint_fast32_t sequential_id
  ) noexcept {
    return (sequential_id & 0xfff) == 0;
  }

  inline std::string event_name(
    SchedulerCommandRef& command
  ) noexcept {
    return command->name();
  }

  bool execute(SchedulerCommandRef&& command) {
    switch (command->type()) {
      case SchedulerCommand::CommandType::ADD_NAMESPACE: {
        auto search = context_.commands_waiting_for_namespace_by_path
          .find(command->namespace_path());

        auto config_namespace = command->config_namespace().get();
        if ((config_namespace == nullptr) || !config_namespace->ok) {
          spdlog::trace(
            "Some error take place building the namespace {} with root_path {}",
            command->namespace_id(),
            command->namespace_path()
          );

          for (auto& command: search->second) {
            command->on_get_namespace_error(context_.worker_queue);
          }
        } else {
          spdlog::trace(
            "Adding namespace {} with root_path {}",
            command->namespace_id(),
            command->namespace_path()
          );
          auto inserted_value = context_.namespace_by_id.emplace(
            command->namespace_id(),
            command->config_namespace()
          );
          assert(inserted_value.second);

          context_.namespace_by_path[command->namespace_path()] = command->config_namespace();

          for (auto& x: search->second) {
            scheduler_queue_.push(std::move(x));
          }
        }

        context_.commands_waiting_for_namespace_by_path.erase(search);

        return true;
      }

      case SchedulerCommand::CommandType::GET_NAMESPACE_BY_PATH: {
        auto result = get_or_build_namespace(command);
        switch (result.first) {
          case ConfigNamespaceState::OK: {
            auto execution_result = command->execute_on_namespace(
              *result.second,
              scheduler_queue_,
              context_.worker_queue
            );
            switch (execution_result) {
              case SchedulerCommand::CommandResult::OK:
                return true;
              case SchedulerCommand::CommandResult::ERROR:
                return false;
              case SchedulerCommand::CommandResult::SOFTDELETE_NAMESPACE: {
                softdelete_namespace(*result.second);
                return true;
              }
            }
            return false;
          }
          case ConfigNamespaceState::BUILDING:
            return true;
          case ConfigNamespaceState::ERROR:
            return command->on_get_namespace_error(
              context_.worker_queue
            );
        }

        return false;
      }

      case SchedulerCommand::CommandType::GET_NAMESPACE_BY_ID: {
        auto search = context_.namespace_by_id.find(command->namespace_id());
        if (search == context_.namespace_by_id.end()) {
          return command->on_get_namespace_error(
            context_.worker_queue
          );
        }

        auto execution_result = command->execute_on_namespace(
          *search->second,
          scheduler_queue_,
          context_.worker_queue
        );
        switch (execution_result) {
          case SchedulerCommand::CommandResult::OK:
            return true;
          case SchedulerCommand::CommandResult::ERROR:
            return false;
          case SchedulerCommand::CommandResult::SOFTDELETE_NAMESPACE: {
            softdelete_namespace(*search->second);
            return true;
          }
        }
        return false;
      }

      case SchedulerCommand::CommandType::GENERIC:
        return command->execute(context_);
    }

    return false;
  }

  inline void loop_stats(
    std::string& name,
    jmutils::MonotonicTimePoint start_time,
    jmutils::MonotonicTimePoint end_time
  ) noexcept {
    double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      end_time - start_time
    ).count();

    context_.metrics->add(
      metrics::MetricsService::MetricId::SCHEDULER_DURATION_NANOSECONDS,
      {{"type", name}},
      duration_ns
    );
  }

  void on_stop() noexcept {
  }

  inline std::pair<ConfigNamespaceState, std::shared_ptr<config_namespace_t>> get_or_build_namespace(
    SchedulerCommandRef& command
  ) {
    // First we search for the namespace
    auto search = context_.namespace_by_path.find(command->namespace_path());
    if (search == context_.namespace_by_path.end()) {
      // If it isn't present we check if some another command ask for it
      auto search_commands_waiting = context_.commands_waiting_for_namespace_by_path
        .find(command->namespace_path());

      if (search_commands_waiting == context_.commands_waiting_for_namespace_by_path.end()) {
        context_.worker_queue.push(
          std::make_unique<worker::SetupCommand>(
            command->namespace_path()
          )
        );

        context_.commands_waiting_for_namespace_by_path[command->namespace_path()]
          .push_back(std::move(command));
      } else {
        // In other case we wait for the namespace
        search_commands_waiting->second.push_back(std::move(command));
      }

      // In this case we need to wait
      return std::make_pair(ConfigNamespaceState::BUILDING, nullptr);
    }

    // If some namespace is present we check if it's well formed
    if (search->second == nullptr || !search->second->ok) {
      return std::make_pair(ConfigNamespaceState::ERROR, nullptr);
    }

    // If we are here then we have the namespace and before return it
    // we update the last access timestamp
    search->second->last_access_timestamp = jmutils::monotonic_now_sec();

    return std::make_pair(ConfigNamespaceState::OK, search->second);
  }

  void softdelete_namespace(
    config_namespace_t& config_namespace
  );

};

} /* mhconfig */

#endif
