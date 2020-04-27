#ifndef MHCONFIG__SCHEDULER__COMMAND__COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__COMMAND_H

#include <memory>

#include "jmutils/container/queue.h"
#include "mhconfig/ds/config_namespace.h"
#include "mhconfig/worker/command/command.h"
#include "mhconfig/metrics.h"

namespace mhconfig
{

namespace worker
{
namespace command
{

class Command;
typedef std::shared_ptr<Command> CommandRef;

} /* command */
} /* worker */


namespace scheduler
{
namespace command
{

using jmutils::container::Queue;

static const std::string EMPTY_STRING{""};

enum CommandType {
  ADD_NAMESPACE,
  GET_NAMESPACE_BY_PATH,
  GET_NAMESPACE_BY_ID,
  GENERIC
};

class Command
{
public:
  Command();
  virtual ~Command();

  virtual std::string name() const = 0;

  virtual CommandType command_type() const = 0;

  virtual const std::string& namespace_path() const;
  virtual uint64_t namespace_id() const;
  virtual const std::shared_ptr<config_namespace_t> config_namespace() const;

  virtual bool execute_on_namespace(
    std::shared_ptr<config_namespace_t> config_namespace,
    Queue<worker::command::CommandRef>& worker_queue
  );

  virtual bool on_get_namespace_error(
    Queue<worker::command::CommandRef>& worker_queue
  );

  virtual bool execute(
    Queue<worker::command::CommandRef>& worker_queue
  );

private:
};

typedef std::shared_ptr<Command> CommandRef;

} /* command */
} /* scheduler */


namespace worker
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

  virtual bool execute(
    Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue,
    Metrics& metrics
  ) = 0;

private:
  /* data */
};

} /* command */
} /* worker */

} /* mhconfig */

#endif
