#include "mhconfig/worker/mod.h"

namespace mhconfig
{
namespace worker
{

Worker::Worker(
  WorkerQueue::ReceiverRef&& worker_queue,
  command::Command::context_t&& context
)
  : worker_queue_(std::move(worker_queue)),
  context_(std::move(context))
{
}

Worker::~Worker() {
}

} /* worker */
} /* mhconfig */
