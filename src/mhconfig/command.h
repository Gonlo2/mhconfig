#ifndef MHCONFIG__COMMAND_H
#define MHCONFIG__COMMAND_H

#include <memory>

#include "jmutils/container/queue.h"

#include "mhconfig/config_namespace.h"
#include "mhconfig/metrics.h"
#include "mhconfig/auth/acl.h"

namespace mhconfig
{

namespace
{
  static const std::string EMPTY_STRING{""};
}


class WorkerCommand;
typedef std::unique_ptr<WorkerCommand> WorkerCommandRef;

typedef jmutils::container::Queue<WorkerCommandRef> WorkerQueue;

struct context_t {
  absl::Mutex mutex;
  absl::flat_hash_map<std::string, std::shared_ptr<config_namespace_t>> cn_by_root_path;
  auth::Acl acl;
  Metrics metrics;
  WorkerQueue worker_queue;
};

class WorkerCommand
{
public:
  virtual ~WorkerCommand();

  virtual std::string name() const = 0;

  virtual bool force_take_metric() const;

  virtual bool execute(context_t* context) = 0;
};

} /* mhconfig */

#endif
