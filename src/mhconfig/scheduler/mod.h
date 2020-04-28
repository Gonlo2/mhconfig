#ifndef MHCONFIG__SCHEDULER__MOD_H
#define MHCONFIG__SCHEDULER__MOD_H

#include <thread>
#include <chrono>

#include "jmutils/container/queue.h"
#include "jmutils/time.h"
#include "jmutils/parallelism/worker.h"
#include "string_pool/pool.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/worker/command/setup_command.h"
#include "mhconfig/metrics.h"

namespace mhconfig
{
namespace scheduler
{

using jmutils::container::Queue;
using namespace mhconfig::api::request;
using namespace mhconfig::ds::config_namespace;

class Scheduler : public jmutils::parallelism::Worker<Scheduler, command::CommandRef>
{
public:
  Scheduler(
    Queue<command::CommandRef>& scheduler_queue,
    Queue<mhconfig::worker::command::CommandRef>& worker_queue,
    Metrics& metrics
  );

  virtual ~Scheduler();

private:
  friend class jmutils::parallelism::Worker<Scheduler, command::CommandRef>;

  enum ConfigNamespaceState {
    OK,
    BUILDING,
    ERROR
  };

  Queue<mhconfig::worker::command::CommandRef>& worker_queue_;
  Metrics& metrics_;

  std::unordered_map<std::string, std::shared_ptr<config_namespace_t>> namespace_by_path_;
  std::unordered_map<uint64_t, std::shared_ptr<config_namespace_t>> namespace_by_id_;

  std::unordered_map<std::string, std::vector<command::CommandRef>> commands_waiting_for_namespace_by_path_;


  inline void loop_stats(
    command::CommandRef command,
    jmutils::time::MonotonicTimePoint start_time,
    jmutils::time::MonotonicTimePoint end_time
  ) {
    double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      end_time - start_time
    ).count();

    metrics_.scheduler_duration(command->name(), duration_ns);
  }

  inline bool process_command(
    command::CommandRef command
  ) {
    switch (command->command_type()) {
      case command::CommandType::ADD_NAMESPACE: {
        auto inserted_value = namespace_by_id_.emplace(
          command->namespace_id(),
          command->config_namespace()
        );
        assert(inserted_value.second);

        namespace_by_path_[command->namespace_path()] = command->config_namespace();

        auto search = commands_waiting_for_namespace_by_path_.find(command->namespace_path());

        input_queue_.push_all(search->second);
        commands_waiting_for_namespace_by_path_.erase(search);

        return true;
      }

      case command::CommandType::GET_NAMESPACE_BY_PATH: {
        auto result = get_or_build_namespace(command);
        switch (result.first) {
          case ConfigNamespaceState::OK:
            return command->execute_on_namespace(
              result.second,
              worker_queue_
            );
          case ConfigNamespaceState::BUILDING:
            return true;
          case ConfigNamespaceState::ERROR:
            return command->on_get_namespace_error(worker_queue_);
        }

        return false;
      }

      case command::CommandType::GET_NAMESPACE_BY_ID: {
        auto search = namespace_by_id_.find(command->namespace_id());
        if (search == namespace_by_id_.end()) {
          return command->on_get_namespace_error(worker_queue_);
        }

        return command->execute_on_namespace(
          search->second,
          worker_queue_
        );
      }

      case command::CommandType::GENERIC:
        return command->execute(worker_queue_);
    }

    return false;
  }

  std::pair<ConfigNamespaceState, std::shared_ptr<config_namespace_t>> get_or_build_namespace(
    command::CommandRef command
  );

};

} /* scheduler */
} /* mhconfig */


#endif
