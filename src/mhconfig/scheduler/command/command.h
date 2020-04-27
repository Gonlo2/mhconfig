#ifndef MHCONFIG__SCHEDULER__COMMAND__COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__COMMAND_H

#include <memory>

#include "jmutils/container/queue.h"
#include "mhconfig/ds/config_namespace.h"
#include "mhconfig/worker/command/command.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

using jmutils::container::Queue;

class Command
{
public:
  Command();
  virtual ~Command();

  virtual std::string name() const = 0;

  virtual bool has_namespace_name() const = 0;
  virtual std::string namespace_name() const = 0;

  virtual bool has_namespace_id() const = 0;
  virtual uint64_t namespace_id() const = 0;

  virtual bool execute_on_namespace(
    std::shared_ptr<config_namespace_t> config_namespace,
    Queue<worker::command::CommandRef>& worker_queue
  ) = 0;

  virtual bool on_get_namespace_error(
    Queue<worker::command::CommandRef>& worker_queue
  ) = 0;

  virtual bool execute(
    Queue<worker::command::CommandRef>& worker_queue
  ) = 0;

private:
  /* data */
};

typedef std::shared_ptr<Command> CommandRef;

} /* command */
} /* scheduler */
} /* mhconfig */

#endif
