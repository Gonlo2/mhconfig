#ifndef MHCONFIG__SCHEDULER__COMMAND__API_RUN_GC_COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__API_RUN_GC_COMMAND_H

#include <memory>

#include "mhconfig/api/request/run_gc_request.h"
#include "mhconfig/scheduler/command/command.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

class ApiRunGCCommand : public Command
{
public:
  ApiRunGCCommand(
    ::mhconfig::api::request::run_gc::Type type,
    uint32_t max_live_in_seconds
  );
  virtual ~ApiRunGCCommand();

  std::string name() const override;

  CommandType command_type() const override;

  bool execute(
    scheduler_context_t& context
  ) override;

private:
  ::mhconfig::api::request::run_gc::Type type_;
  uint32_t max_live_in_seconds_;

  void remove_merge_configs(
    scheduler_context_t& context,
    uint32_t limit_timestamp,
    uint32_t generation
  );

  void remove_dead_pointers(
    scheduler_context_t& context
  );

  void remove_namespaces(
    scheduler_context_t& context,
    uint32_t limit_timestamp
  );

  void remove_versions(
    scheduler_context_t& context,
    uint32_t limit_timestamp
  );

};

} /* command */
} /* scheduler */
} /* mhconfig */

#endif
