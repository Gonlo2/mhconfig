#ifndef MHCONFIG__WORKER__UPDATE_COMMAND_H
#define MHCONFIG__WORKER__UPDATE_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/scheduler/update_documents_command.h"
#include "mhconfig/command.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/builder.h"

namespace mhconfig
{
namespace worker
{

using namespace mhconfig::builder;

class UpdateCommand : public WorkerCommand
{
public:
  UpdateCommand(
    uint64_t namespace_id,
    std::shared_ptr<jmutils::string::Pool> pool,
    std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request
  );
  virtual ~UpdateCommand();

  std::string name() const override;

  bool force_take_metric() const override;

  bool execute(
    context_t& context
  ) override;

private:
  uint64_t namespace_id_;
  std::shared_ptr<jmutils::string::Pool> pool_;
  std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request_;

  bool add_items(
    absl::flat_hash_map<std::string, load_raw_config_result_t>& items
  );
};

} /* worker */
} /* mhconfig */

#endif
