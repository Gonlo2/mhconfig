#include "mhconfig/api/session.h"

namespace mhconfig
{
namespace api
{

std::shared_ptr<Session> Session::proceed(
  uint8_t status,
  CustomService* service,
  grpc::ServerCompletionQueue* cq,
  auth::Acl* acl,
  SchedulerQueue::Sender* scheduler_sender,
  metrics::MetricsService* metrics,
  uint_fast32_t& sequential_id
) {
  mutex_.ReaderLock();
  bool proceed = !closed_;
  mutex_.ReaderUnlock();

  if (proceed) {
    on_proceed(status, service, cq, acl, scheduler_sender, metrics, sequential_id);
  }

  mutex_.Lock();
  auto result = decrement_cq_refcount(scheduler_sender, metrics);
  mutex_.Unlock();
  return result;
}

std::shared_ptr<Session> Session::error(
  SchedulerQueue::Sender* scheduler_sender,
  metrics::MetricsService* metrics
) {
  mutex_.Lock();
  closed_ = true;
  auto result = decrement_cq_refcount(scheduler_sender, metrics);
  mutex_.Unlock();
  return result;
}

} /* api */
} /* mhconfig */
