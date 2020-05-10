#ifndef MHCONFIG__SCHEDULER__COMMAND__COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__COMMAND_H

#include <memory>

#include "jmutils/container/queue.h"
#include "mhconfig/ds/config_namespace.h"
#include "mhconfig/worker/command/command.h"
#include "mhconfig/metrics/metrics_service.h"

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

class Command;
typedef std::shared_ptr<Command> CommandRef;

} /* command */
} /* scheduler */


namespace scheduler
{

using jmutils::container::Queue;
using namespace mhconfig::ds::config_namespace;

struct scheduler_context_t {
  Queue<mhconfig::worker::command::CommandRef>& worker_queue;
  metrics::MetricsService& metrics;

  std::unordered_map<std::string, std::shared_ptr<config_namespace_t>> namespace_by_path;
  std::unordered_map<uint64_t, std::shared_ptr<config_namespace_t>> namespace_by_id;
  std::unordered_map<std::string, std::vector<command::CommandRef>> commands_waiting_for_namespace_by_path;

  scheduler_context_t(
    Queue<mhconfig::worker::command::CommandRef>& worker_queue_,
    metrics::MetricsService& metrics_
  )
    : worker_queue(worker_queue_),
    metrics(metrics_)
  {}
};

namespace command
{

static const std::string EMPTY_STRING{""};

enum CommandType {
  ADD_NAMESPACE,
  GET_NAMESPACE_BY_PATH,
  GET_NAMESPACE_BY_ID,
  GENERIC
};

enum NamespaceExecutionResult {
  OK,
  ERROR,
  SOFTDELETE_NAMESPACE
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

  virtual NamespaceExecutionResult execute_on_namespace(
    config_namespace_t& config_namespace,
    Queue<CommandRef>& scheduler_queue,
    Queue<worker::command::CommandRef>& worker_queue
  );

  virtual bool on_get_namespace_error(
    Queue<worker::command::CommandRef>& worker_queue
  );

  virtual bool execute(
    scheduler_context_t& context
  );

private:
};

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
  struct context_t {
    Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue;
    metrics::MetricsService& async_metrics_service;
    metrics::MetricsService& metrics_service;

    context_t(
      Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue_,
      metrics::MetricsService& async_metrics_service_,
      metrics::MetricsService& metrics_service_
    )
      : scheduler_queue(scheduler_queue_),
      async_metrics_service(async_metrics_service_),
      metrics_service(metrics_service_)
    {}
  };

  Command();
  virtual ~Command();

  virtual std::string name() const = 0;

  virtual bool force_take_metric() const;

  virtual bool execute(context_t& context) = 0;
};

} /* command */
} /* worker */

} /* mhconfig */

#endif
