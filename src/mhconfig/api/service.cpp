#include "mhconfig/api/service.h"

namespace mhconfig
{
namespace api
{


Service::Service(
  const std::string& server_address,
  std::vector<std::pair<SchedulerQueue::SenderRef, metrics::AsyncMetricsService>>&& thread_vars
) :
  server_address_(server_address),
  thread_vars_(std::move(thread_vars))
{
}

Service::~Service() {
  server_->Shutdown();
  for (auto& t : threads_) t->stop();
  for (auto& t : threads_) t->join();
}

bool Service::start() {
  grpc::ServerBuilder builder;

  builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
  builder.RegisterService(&service_);

  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs;
  cqs.reserve(thread_vars_.size());
  for (size_t i = 0; i < thread_vars_.size(); ++i) {
    cqs.push_back(builder.AddCompletionQueue());
  }

  server_ = builder.BuildAndStart();
  spdlog::info("Server listening on '{}'", server_address_);

  threads_.reserve(thread_vars_.size());
  for (size_t i = 0; i < thread_vars_.size(); ++i) {
    // TODO remove the reclaimed threads in case of error
    auto thread = std::make_unique<ServiceThread>(
      &service_,
      std::move(cqs[i]),
      std::move(thread_vars_[i].first),
      std::move(thread_vars_[i].second)
    );
    if (!thread->start()) return false;
    threads_.push_back(std::move(thread));
  }

  return true;
}

void Service::join() {
  for (auto& thread: threads_) {
    thread->join();
  }
}

} /* api */
} /* mhconfig */
