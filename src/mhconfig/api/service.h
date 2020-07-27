#ifndef MHCONFIG__API__SERVICE_H
#define MHCONFIG__API__SERVICE_H

#include <memory>
#include <iostream>
#include <string>
#include <chrono>

#include "jmutils/parallelism/worker.h"

#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>
#include <google/protobuf/arena.h>

#include "mhconfig/command.h"
#include "mhconfig/proto/mhconfig.grpc.pb.h"
#include "mhconfig/metrics/metrics_service.h"
#include "mhconfig/metrics/async_metrics_service.h"

#include "mhconfig/api/session.h"
#include "mhconfig/api/request/get_request_impl.h"
#include "mhconfig/api/request/update_request_impl.h"
#include "mhconfig/api/request/run_gc_request_impl.h"
#include "mhconfig/api/stream/watch_stream_impl.h"
#include "mhconfig/api/stream/trace_stream_impl.h"

#include <prometheus/summary.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

namespace mhconfig
{
namespace api
{

class Service final
{
public:
  Service(
    const std::string& server_address,
    std::vector<std::pair<SchedulerQueue::SenderRef, metrics::AsyncMetricsService>>&& senders,
    auth::Acl* acl
  );

  ~Service();

  Service(const Service& o) = delete;
  Service(Service&& o) = delete;

  Service& operator=(const Service& o) = delete;
  Service& operator=(Service&& o) = delete;

  bool start();
  void join();

private:
  class ServiceThread final : public jmutils::Worker<ServiceThread, std::pair<bool, void*>>
  {
  public:
    ServiceThread(
      CustomService* service,
      std::unique_ptr<grpc::ServerCompletionQueue>&& cq,
      SchedulerQueue::SenderRef&& sender,
      metrics::AsyncMetricsService&& metrics,
      auth::Acl* acl
    ) : service_(service),
      cq_(std::move(cq)),
      sender_(std::move(sender)),
      metrics_(std::move(metrics)),
      acl_(acl)
    {}

    void stop() {
      cq_->Shutdown();
    }

    void set_acl(auth::Acl* acl) {
      acl_ = acl;
    }

  private:
    friend class jmutils::Worker<ServiceThread, std::pair<bool, void*>>;

    CustomService* service_;
    std::unique_ptr<grpc::ServerCompletionQueue> cq_;
    SchedulerQueue::SenderRef sender_;
    metrics::AsyncMetricsService metrics_;
    auth::Acl* acl_;
    uint_fast32_t request_id_{0};

    void on_start() noexcept {
      for (size_t i = 0; i < 32; ++i) { //TODO configure the number of requests
        auto get_request = make_session<request::GetRequestImpl>();
        get_request->subscribe(service_, cq_.get());

        auto update_request = make_session<request::UpdateRequestImpl>();
        update_request->subscribe(service_, cq_.get());

        auto run_gc_request = make_session<request::RunGCRequestImpl>();
        run_gc_request->subscribe(service_, cq_.get());

        auto watch_stream = make_session<stream::WatchStreamImpl>();
        watch_stream->subscribe(service_, cq_.get());

        auto trace_stream = make_session<stream::TraceStreamImpl>();
        trace_stream->subscribe(service_, cq_.get());
      }
    }

    inline bool pop(
      std::pair<bool, void*>& event
    ) noexcept {
      return cq_->Next(&event.second, &event.first);
    }

    inline bool metricate(
      std::pair<bool, void*>& event,
      uint_fast32_t sequential_id
    ) noexcept {
      return false;
    }

    inline std::string event_name(
      std::pair<bool, void*>& event
    ) noexcept {
      return "";
    }

    inline bool execute(std::pair<bool, void*>&& event) noexcept {
      try {
        uintptr_t ptr = (uintptr_t) event.second;
        auto session = (Session*) (ptr & 0xfffffffffffffff8ull);
        uint8_t status = ptr & 7;
        if (event.first) {
          spdlog::trace(
            "Obtained the ok gRPC event {} with the status {}",
            (void*) session,
            status
          );
          auto _ = session->proceed(
            status,
            service_,
            cq_.get(),
            acl_,
            sender_.get(),
            &metrics_,
            request_id_
          );
        } else {
          spdlog::trace(
            "Obtained the error gRPC event {} with the status {}",
            (void*) session,
            status
          );
          auto _ = session->error(sender_.get(), &metrics_);
        }
        return true;
      } catch (const std::exception &e) {
        spdlog::error("Some error take place processing the gRPC session: {}", e.what());
      } catch (...) {
        spdlog::error("Some unknown error take place processing the gRPC session");
      }

      return false;
    }

    inline void loop_stats(
      std::string& name,
      jmutils::MonotonicTimePoint start_time,
      jmutils::MonotonicTimePoint end_time
    ) noexcept {
    }

    void on_stop() noexcept {
      void* tag;
      bool ignored_ok;
      while (cq_->Next(&tag, &ignored_ok)) {
        execute(std::make_pair(false, tag));
      }
    }
  };

  std::string server_address_;
  std::vector<std::pair<SchedulerQueue::SenderRef, metrics::AsyncMetricsService>> thread_vars_;
  std::vector<std::unique_ptr<ServiceThread>> threads_;

  std::unique_ptr<grpc::Server> server_;
  CustomService service_;
  auth::Acl* acl_;
};

} /* api */
} /* mhconfig */

#endif
