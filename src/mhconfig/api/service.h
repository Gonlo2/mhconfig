#ifndef MHCONFIG__API__SERVICE_H
#define MHCONFIG__API__SERVICE_H

#include <memory>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>

#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>

#include "mhconfig/common.h"
#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/proto/mhconfig.grpc.pb.h"
#include "mhconfig/metrics/metrics_service.h"
#include "mhconfig/metrics/async_metrics_service.h"

#include "mhconfig/api/session.h"
#include "mhconfig/api/request/get_request_impl.h"
#include "mhconfig/api/request/update_request_impl.h"
#include "mhconfig/api/request/run_gc_request_impl.h"
#include "mhconfig/api/stream/watch_stream_impl.h"

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
    std::vector<std::pair<SchedulerQueue::SenderRef, metrics::AsyncMetricsService>>&& senders
  );

  virtual ~Service();

  bool start();
  void join();

private:
  std::string server_address_;
  std::vector<std::pair<SchedulerQueue::SenderRef, metrics::AsyncMetricsService>> thread_vars_;
  std::vector<std::unique_ptr<std::thread>> threads_;

  std::unique_ptr<grpc::Server> server_;
  CustomService service_;
  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;

  void subscribe_requests(
    grpc::ServerCompletionQueue* cq
  );

  void handle_requests(
    grpc::ServerCompletionQueue* cq,
    SchedulerQueue::SenderRef scheduler_sender,
    metrics::AsyncMetricsService metrics
  );
};

} /* api */
} /* mhconfig */

#endif
