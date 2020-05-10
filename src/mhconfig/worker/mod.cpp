#include "mhconfig/worker/mod.h"

namespace mhconfig
{
namespace worker
{

Worker::Worker(
  Queue<command::CommandRef>& worker_queue,
  size_t num_threads,
  command::Command::context_t context
) :
  jmutils::parallelism::Worker<Worker, command::CommandRef>(worker_queue, num_threads),
  context_(context)
{
}

Worker::~Worker() {
}

} /* worker */
} /* mhconfig */
