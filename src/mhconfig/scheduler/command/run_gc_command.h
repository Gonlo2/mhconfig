#ifndef MHCONFIG__SCHEDULER__COMMAND__API_RUN_GC_COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__API_RUN_GC_COMMAND_H

#include <memory>

#include "mhconfig/api/request/run_gc_request.h"
#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/worker/command/optimize_merged_config_command.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

namespace run_gc {
  enum Type {
    CACHE_GENERATION_0 = 0,
    CACHE_GENERATION_1 = 1,
    CACHE_GENERATION_2 = 2,
    DEAD_POINTERS = 3,
    NAMESPACES = 4,
    VERSIONS = 5
  };
}

class RunGcCommand : public Command
{
public:
  RunGcCommand(
    run_gc::Type type,
    uint32_t max_live_in_seconds
  );
  virtual ~RunGcCommand();

  std::string name() const override;

  CommandType command_type() const override;

  bool execute(
    scheduler_context_t& context
  ) override;

private:
  run_gc::Type type_;
  uint32_t max_live_in_seconds_;

  void remove_merge_configs(
    scheduler_context_t& context,
    uint32_t generation
  );

  void remove_dead_pointers(
    scheduler_context_t& context
  );

  void remove_namespaces(
    scheduler_context_t& context
  );

  void remove_versions(
    scheduler_context_t& context
  );

  inline size_t remove_expired_watchers(
    std::vector<std::weak_ptr<::mhconfig::api::stream::WatchInputMessage>>& watchers
  ) {
    size_t removed = 0;
    for (size_t i = 0; i < watchers.size();) {
      if (watchers[i].expired()) {
        spdlog::trace("Removing a watcher");
        jmutils::swap_delete(watchers, i);
        ++removed;
      } else {
        ++i;
      }
    }
    return removed;
  }

};

} /* command */
} /* scheduler */
} /* mhconfig */

#endif
