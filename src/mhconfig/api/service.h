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

#include "jmutils/container/queue.h"
//#include "mhconfig/worker/common.h"
#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/proto/mhconfig.grpc.pb.h"

#include "mhconfig/api/request/request.h"
#include "mhconfig/api/request/get_request_impl.h"
#include "mhconfig/api/request/update_request_impl.h"
#include "mhconfig/api/request/run_gc_request_impl.h"

#include <prometheus/summary.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

namespace mhconfig
{
namespace api
{

using jmutils::container::Queue;
//using namespace mhconfig::worker;

class Service final
{
public:
  Service(
    const std::string& server_address,
    size_t num_threads,
    Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue,
    Metrics& metrics
  );

  virtual ~Service();

  bool start();
  void join();

private:
  size_t num_threads_;

  std::string server_address_;
  Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue_;

  std::vector<std::unique_ptr<std::thread>> threads_;

  std::unique_ptr<grpc::Server> server_;
  mhconfig::proto::MHConfig::AsyncService service_;
  std::unique_ptr<grpc::ServerCompletionQueue> cq_;

  Metrics& metrics_;

  void subscribe_requests();
  void handle_request();
};

} /* api */
} /* mhconfig */

#endif
