#ifndef MHCONFIG__SCHEDULER__COMMAND__UPDATE_DOCUMENTS_COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__UPDATE_DOCUMENTS_COMMAND_H

#include <memory>

#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/api/stream/watch_stream_impl.h"
#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/worker/command/api_reply_command.h"
#include "mhconfig/worker/command/build_command.h"
#include "mhconfig/scheduler/command/api_get_command.h"
#include "mhconfig/builder.h"
#include "jmutils/time.h"
#include "jmutils/common.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

using namespace mhconfig::ds::config_namespace;
using namespace mhconfig::builder;

class UpdateDocumentsCommand : public Command
{
public:
  UpdateDocumentsCommand(
    uint64_t namespace_id,
    std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request,
    std::vector<load_raw_config_result_t> items
  );

  virtual ~UpdateDocumentsCommand();

  std::string name() const override;

  CommandType command_type() const override;
  uint64_t namespace_id() const override;

  NamespaceExecutionResult execute_on_namespace(
    config_namespace_t& config_namespace,
    SchedulerQueue& scheduler_queue,
    WorkerQueue& worker_queue
  ) override;

  bool on_get_namespace_error(
    WorkerQueue& worker_queue
  ) override;

private:
  uint64_t namespace_id_;
  std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request_;
  std::vector<load_raw_config_result_t> items_;

  void send_api_response(
    WorkerQueue& worker_queue
  );

  void get_affected_documents(
    const config_namespace_t& config_namespace,
    std::unordered_set<std::string>& affected_documents
  );

};

} /* command */
} /* scheduler */
} /* mhconfig */

#endif
