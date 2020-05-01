#include "mhconfig/api/service.h"

namespace mhconfig
{
namespace api
{


Service::Service(
  const std::string& server_address,
  size_t num_threads,
  Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue,
  Metrics& metrics
) :
  server_address_(server_address),
  num_threads_(num_threads),
  scheduler_queue_(scheduler_queue),
  metrics_(metrics)
{
}

Service::~Service() {
  server_->Shutdown();
  cq_->Shutdown();
}

bool Service::start() {
  grpc::ServerBuilder builder;

  builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
  builder.RegisterService(&service_);
  cq_ = builder.AddCompletionQueue();
  server_ = builder.BuildAndStart();
  spdlog::info("Server listening on '{}'", server_address_);

  subscribe_requests();

  threads_.reserve(num_threads_);
  for (size_t i = 0; i < num_threads_; ++i) {
    threads_.push_back(
      std::make_unique<std::thread>(&Service::handle_request, this)
    );
  }

  return true;
}

void Service::join() {
  for (auto& thread: threads_) thread->join();
}

void Service::subscribe_requests() {
  auto get_request = new request::GetRequestImpl(
    &service_,
    cq_.get(),
    metrics_,
    scheduler_queue_
  );
  get_request->subscribe();

  auto update_request = new request::UpdateRequestImpl(
    &service_,
    cq_.get(),
    metrics_,
    scheduler_queue_
  );
  update_request->subscribe();

  auto run_gc_request = new request::RunGCRequestImpl(
    &service_,
    cq_.get(),
    metrics_,
    scheduler_queue_
  );
  run_gc_request->subscribe();
}

void Service::handle_request() {
  void* tag;
  bool got_event;
  bool ok;
  do {
    got_event = cq_->Next(&tag, &ok);
    if (!got_event) {
      spdlog::info("The completion queue has been closed");
    } else if (!ok) {
      spdlog::error("Can't read a completion queue event {}", tag);
      // It's neccesary drop the event here?
      delete static_cast<request::Request*>(tag);
    } else {
      try {
        static_cast<request::Request*>(tag)->proceed();
      } catch (const std::exception &e) {
        spdlog::error("Some error take place processing the gRPC input: {}", e.what());
      } catch (...) {
        spdlog::error("Some unknown error take place processing the gRPC input");
      }
    }
  } while (got_event);
}


} /* api */
} /* mhconfig */
