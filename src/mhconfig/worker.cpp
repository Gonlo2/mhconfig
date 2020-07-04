#include "mhconfig/worker.h"

namespace mhconfig
{

Worker::Worker(
  WorkerQueue::ReceiverRef&& worker_queue,
  WorkerCommand::context_t&& context
)
  : worker_queue_(std::move(worker_queue)),
  context_(std::move(context))
{
}

Worker::~Worker() {
}

} /* mhconfig */
