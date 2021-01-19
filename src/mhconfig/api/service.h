#ifndef MHCONFIG__API__SERVICE_H
#define MHCONFIG__API__SERVICE_H

#include <bits/exception.h>
#include <bits/stdint-uintn.h>
#include <google/protobuf/arena.h>
#include <grpc/grpc.h>
#include <grpcpp/impl/codegen/completion_queue.h>
#include <grpcpp/impl/codegen/completion_queue_impl.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include <prometheus/summary.h>
#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>
#include <stddef.h>
#include <stdint.h>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "jmutils/parallelism/worker.h"
#include "jmutils/time.h"
#include "mhconfig/api/request/get_request_impl.h"
#include "mhconfig/api/request/run_gc_request_impl.h"
#include "mhconfig/api/request/update_request_impl.h"
#include "mhconfig/api/session.h"
#include "mhconfig/api/stream/trace_stream_impl.h"
#include "mhconfig/api/stream/watch_stream_impl.h"
#include "mhconfig/context.h"
#include "mhconfig/metrics.h"
#include "mhconfig/proto/mhconfig.grpc.pb.h"

namespace mhconfig
{
namespace api
{

class Service final
{
public:
  Service(
    const std::string& server_address,
    size_t num_threads,
    std::shared_ptr<context_t>& ctx
  );

  ~Service();

  Service(const Service& o) = delete;
  Service(Service&& o) = delete;

  Service& operator=(const Service& o) = delete;
  Service& operator=(Service&& o) = delete;

  bool start();
  void join();

private:
  class ServiceThread final
    : public jmutils::Worker<ServiceThread, std::pair<bool, void*>>
  {
  public:
    ServiceThread(
      CustomService* service,
      std::unique_ptr<grpc::ServerCompletionQueue>&& cq,
      std::shared_ptr<context_t>& ctx
    ) : service_(service),
      cq_(std::move(cq)),
      ctx_(ctx)
    {}

    void stop() {
      cq_->Shutdown();
    }

  private:
    friend class jmutils::Worker<ServiceThread, std::pair<bool, void*>>;

    CustomService* service_;
    std::unique_ptr<grpc::ServerCompletionQueue> cq_;
    std::shared_ptr<context_t> ctx_;
    uint32_t request_id_{0};

    void on_start() noexcept {
      for (size_t i = 0; i < 32; ++i) { //TODO configure the number of requests
        make_session<request::GetRequestImpl>(ctx_)->subscribe(service_, cq_.get());
        make_session<request::UpdateRequestImpl>(ctx_)->subscribe(service_, cq_.get());
        make_session<request::RunGCRequestImpl>(ctx_)->subscribe(service_, cq_.get());
        make_session<stream::WatchStreamImpl>(ctx_)->subscribe(service_, cq_.get());
        make_session<stream::TraceStreamImpl>(ctx_)->subscribe(service_, cq_.get());
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
          auto _ = session->proceed(status, service_, cq_.get());
        } else {
          spdlog::trace(
            "Obtained the error gRPC event {} with the status {}",
            (void*) session,
            status
          );
          auto _ = session->error();
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
  size_t num_threads_;
  std::vector<std::unique_ptr<ServiceThread>> threads_;
  std::unique_ptr<grpc::Server> server_;
  CustomService service_;
  std::shared_ptr<context_t> ctx_;
};

} /* api */
} /* mhconfig */

#endif
