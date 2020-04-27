#include "mhconfig/worker/mod.h"

namespace mhconfig
{
namespace worker
{

Worker::Worker(
  Queue<command::CommandRef>& worker_queue,
  size_t num_threads,
  Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue,
  Metrics& metrics
) :
  jmutils::parallelism::Worker<Worker, command::CommandRef>(worker_queue, num_threads),
  scheduler_queue_(scheduler_queue),
  metrics_(metrics)
{
}

Worker::~Worker() {
}

} /* worker */
} /* mhconfig */
