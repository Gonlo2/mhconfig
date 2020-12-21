#include "mhconfig/api/service.h"

namespace mhconfig
{
struct context_t;

namespace api
{


Service::Service(
  const std::string& server_address,
  size_t num_threads,
  std::shared_ptr<context_t>& ctx
) :
  server_address_(server_address),
  num_threads_(num_threads),
  ctx_(ctx)
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
  cqs.reserve(num_threads_);
  for (size_t i = 0; i < num_threads_; ++i) {
    cqs.push_back(builder.AddCompletionQueue());
  }

  server_ = builder.BuildAndStart();
  spdlog::info("Server listening on '{}'", server_address_);

  threads_.reserve(num_threads_);
  for (size_t i = 0; i < num_threads_; ++i) {
    // TODO remove the reclaimed threads in case of error
    auto thread = std::make_unique<ServiceThread>(
      &service_,
      std::move(cqs[i]),
      ctx_
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
