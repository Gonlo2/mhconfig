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
  for (auto& cq : cqs_) {
    cq->Shutdown();
  }

  void* ignored_tag;
  bool ignored_ok;
  for (auto& cq : cqs_) {
    while (cq->Next(&ignored_tag, &ignored_ok)) {
    }
  }
}

bool Service::start() {
  grpc::ServerBuilder builder;

  builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
  builder.RegisterService(&service_);

  cqs_.reserve(thread_vars_.size());
  for (size_t i = 0; i < thread_vars_.size(); ++i) {
    cqs_.push_back(std::move(builder.AddCompletionQueue()));
  }

  server_ = builder.BuildAndStart();

  spdlog::info("Server listening on '{}'", server_address_);

  threads_.reserve(thread_vars_.size());
  for (size_t i = 0; i < thread_vars_.size(); ++i) {
    threads_.push_back(
      std::make_unique<std::thread>(
        &Service::handle_requests,
        this,
        cqs_[i].get(),
        thread_vars_[i].first,
        thread_vars_[i].second
      )
    );
  }

  return true;
}

void Service::join() {
  for (auto& thread: threads_) {
    thread->join();
  }
}

void Service::handle_requests(
  grpc::ServerCompletionQueue* cq,
  SchedulerQueue::SenderRef scheduler_sender,
  metrics::AsyncMetricsService metrics
) {
  subscribe_requests(cq);

  void* tag;
  bool got_event;
  bool ok;
  do {
    got_event = cq->Next(&tag, &ok);
    if (!got_event) {
      spdlog::info("The completion queue has been closed");
    } else {
      try {
        spdlog::trace("Obtained the completion queue event {}", tag);
        auto session = static_cast<Session*>(tag);
        if (ok) {
          auto _ = session->proceed(
            &service_,
            cq,
            scheduler_sender.get(),
            metrics
          );
        } else {
          spdlog::debug("The completion queue event {} isn't ok, destroying it", tag);
          auto _ = session->destroy();
        }
        spdlog::trace("Processed the completion queue event {}", tag);
      } catch (const std::exception &e) {
        spdlog::error("Some error take place processing the gRPC session: {}", e.what());
      } catch (...) {
        spdlog::error("Some unknown error take place processing the gRPC session");
      }
    }
  } while (got_event);
}

void Service::subscribe_requests(
  grpc::ServerCompletionQueue* cq
) {
  for (size_t i = 0; i < 100; ++i) { //TODO configure the number of requests
    auto get_request = make_session<request::GetRequestImpl>();
    get_request->subscribe(&service_, cq);

    auto update_request = make_session<request::UpdateRequestImpl>();
    update_request->subscribe(&service_, cq);

    auto run_gc_request = make_session<request::RunGCRequestImpl>();
    run_gc_request->subscribe(&service_, cq);

    auto watch_stream = make_session<stream::WatchStreamImpl>();
    watch_stream->subscribe(&service_, cq);
  }
}


} /* api */
} /* mhconfig */
