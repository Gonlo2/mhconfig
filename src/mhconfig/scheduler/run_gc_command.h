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

  inline bool is_namespace_in_use(
    const config_namespace_t& config_namespace
  ) {
    return !config_namespace.watchers.empty()
      || !config_namespace.traces_by_override.empty()
      || !config_namespace.traces_by_flavor.empty()
      || !config_namespace.traces_by_document.empty()
      || !config_namespace.to_trace_always.empty();
  }

};

} /* scheduler */
} /* mhconfig */

#endif
