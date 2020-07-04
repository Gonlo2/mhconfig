#ifndef MHCONFIG__SCHEDULER__API_RUN_GC_COMMAND_H
#define MHCONFIG__SCHEDULER__API_RUN_GC_COMMAND_H

#include <memory>

#include "mhconfig/builder.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/request/run_gc_request.h"
#include "mhconfig/worker/update_command.h"
#include "mhconfig/command.h"
#include "jmutils/time.h"
#include "jmutils/common.h"

namespace mhconfig
{
namespace scheduler
{

class RunGcCommand : public SchedulerCommand
{
public:
  enum class Type {
    CACHE_GENERATION_0 = 0,
    CACHE_GENERATION_1 = 1,
    CACHE_GENERATION_2 = 2,
    DEAD_POINTERS = 3,
    NAMESPACES = 4,
    VERSIONS = 5
  };

  RunGcCommand(
    Type type,
    uint32_t max_live_in_seconds
  );
  virtual ~RunGcCommand();

  std::string name() const override;

  CommandType type() const override;

  bool execute(
    context_t& context
  ) override;

private:
  Type type_;
  uint32_t max_live_in_seconds_;

  void remove_merge_configs(
    context_t& context,
    uint32_t generation
  );

  void remove_dead_pointers(
    context_t& context
  );

  void remove_namespaces(
    context_t& context
  );

  void remove_versions(
    context_t& context
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

} /* scheduler */
} /* mhconfig */

#endif
