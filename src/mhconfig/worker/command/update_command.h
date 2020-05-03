#ifndef MHCONFIG__WORKER__COMMAND__UPDATE_COMMAND_H
#define MHCONFIG__WORKER__COMMAND__UPDATE_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/scheduler/command/update_documents_command.h"
#include "mhconfig/worker/command/command.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/builder.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

using namespace mhconfig::ds::config_namespace;
using namespace mhconfig::builder;

class UpdateCommand : public Command
{
public:
  UpdateCommand(
    uint64_t namespace_id,
    std::shared_ptr<string_pool::Pool> pool,
    std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request
  );
  virtual ~UpdateCommand();

  std::string name() const override;

  bool execute(
    Queue<scheduler::command::CommandRef>& scheduler_queue,
    Metrics& metrics
  ) override;

private:
  uint64_t namespace_id_;
  std::shared_ptr<string_pool::Pool> pool_;
  std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request_;

  bool add_items(
    std::vector<load_raw_config_result_t>& items
  );
};

} /* command */
} /* worker */
} /* mhconfig */

#endif
