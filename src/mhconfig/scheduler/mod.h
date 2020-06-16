#ifndef MHCONFIG__SCHEDULER__MOD_H
#define MHCONFIG__SCHEDULER__MOD_H

#include <thread>
#include <chrono>

#include "jmutils/container/queue.h"
#include "jmutils/time.h"
#include "jmutils/parallelism/worker.h"
#include "string_pool/pool.h"
#include "mhconfig/common.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/worker/command/setup_command.h"
#include "mhconfig/worker/command/unregister_watchers_command.h"

namespace mhconfig
{
namespace scheduler
{

using namespace mhconfig::api::request;
using namespace mhconfig::ds::config_namespace;

class Scheduler : public jmutils::parallelism::Worker<Scheduler, command::CommandRef>
{
public:
  Scheduler(
    SchedulerQueue& scheduler_queue,
    WorkerQueue& worker_queue,
    std::unique_ptr<metrics::MetricsService>&& metrics
  );

  virtual ~Scheduler();

private:
  friend class jmutils::parallelism::Worker<Scheduler, command::CommandRef>;

  enum class ConfigNamespaceState {
    OK,
    BUILDING,
    ERROR
  };

  SchedulerQueue& scheduler_queue_;
  scheduler_context_t context_;

  inline void pop(
    command::CommandRef& command
  ) {
    scheduler_queue_.pop(command);
  }

  inline bool metricate(
    command::CommandRef& command,
    uint_fast32_t sequential_id
  ) {
    return (sequential_id & 0xfff) == 0;
  }

  inline void loop_stats(
    std::string& command_name,
    jmutils::time::MonotonicTimePoint start_time,
    jmutils::time::MonotonicTimePoint end_time
  ) {
    double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      end_time - start_time
    ).count();

    context_.metrics->add(
      metrics::MetricsService::MetricId::SCHEDULER_DURATION_NANOSECONDS,
      {{"type", command_name}},
      duration_ns
    );
  }

  inline bool process_command(
    command::CommandRef&& command
  ) {
    switch (command->command_type()) {
      case command::CommandType::ADD_NAMESPACE: {
        auto inserted_value = context_.namespace_by_id.emplace(
          command->namespace_id(),
          command->config_namespace()
        );
        assert(inserted_value.second);

        context_.namespace_by_path[command->namespace_path()] = command->config_namespace();

        auto search = context_.commands_waiting_for_namespace_by_path
          .find(command->namespace_path());

        for (auto& x: search->second) {
          scheduler_queue_.push(std::move(x));
        }
        context_.commands_waiting_for_namespace_by_path.erase(search);

        return true;
      }

      case command::CommandType::GET_NAMESPACE_BY_PATH: {
        auto result = get_or_build_namespace(command);
        switch (result.first) {
          case ConfigNamespaceState::OK: {
            auto execution_result = command->execute_on_namespace(
              *result.second,
              scheduler_queue_,
              context_.worker_queue
            );
            switch (execution_result) {
              case command::NamespaceExecutionResult::OK:
                return true;
              case command::NamespaceExecutionResult::ERROR:
                return false;
              case command::NamespaceExecutionResult::SOFTDELETE_NAMESPACE: {
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

      case command::CommandType::GET_NAMESPACE_BY_ID: {
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
          case command::NamespaceExecutionResult::OK:
            return true;
          case command::NamespaceExecutionResult::ERROR:
            return false;
          case command::NamespaceExecutionResult::SOFTDELETE_NAMESPACE: {
            softdelete_namespace(*search->second);
            return true;
          }
        }
        return false;
      }

      case command::CommandType::GENERIC:
        return command->execute(context_);
    }

    return false;
  }

  inline std::pair<ConfigNamespaceState, std::shared_ptr<config_namespace_t>> get_or_build_namespace(
    command::CommandRef& command
  ) {
    // First we search for the namespace
    auto search = context_.namespace_by_path.find(command->namespace_path());
    if (search == context_.namespace_by_path.end()) {
      // If it isn't present we check if some another command ask for it
      auto search_commands_waiting = context_.commands_waiting_for_namespace_by_path
        .find(command->namespace_path());

      if (search_commands_waiting == context_.commands_waiting_for_namespace_by_path.end()) {
        context_.worker_queue.push(
          std::make_unique<::mhconfig::worker::command::SetupCommand>(
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
    search->second->last_access_timestamp = jmutils::time::monotonic_now_sec();

    return std::make_pair(ConfigNamespaceState::OK, search->second);
  }

  void softdelete_namespace(
    config_namespace_t& config_namespace
  );

};

} /* scheduler */
} /* mhconfig */

#endif
