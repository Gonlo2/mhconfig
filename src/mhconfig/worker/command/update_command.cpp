#include "mhconfig/worker/command/update_command.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

UpdateCommand::UpdateCommand(
  uint64_t namespace_id,
  std::shared_ptr<::string_pool::Pool> pool,
  std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request
)
  : Command(),
  namespace_id_(namespace_id),
  pool_(pool),
  update_request_(update_request)
{
}

UpdateCommand::~UpdateCommand() {
}

std::string UpdateCommand::name() const {
  return "UPDATE";
}

bool UpdateCommand::force_take_metric() const {
  return true;
}

bool UpdateCommand::execute(
  context_t& context
) {
  std::vector<load_raw_config_result_t> items;
  items.reserve(update_request_->relative_paths().size());

  if (!add_items(items)) {
    update_request_->set_namespace_id(namespace_id_);
    update_request_->set_status(::mhconfig::api::request::update_request::Status::ERROR);
    update_request_->commit();
  } else {
    context.scheduler_queue->push(
      std::make_unique<::mhconfig::scheduler::command::UpdateDocumentsCommand>(
        namespace_id_,
        update_request_,
        items
      )
    );
  }

  return true;
}

bool UpdateCommand::add_items(
  std::vector<load_raw_config_result_t>& items
) {
  std::filesystem::path root_path(update_request_->root_path());
  for (const std::string& x : update_request_->relative_paths()) {
    // TODO check the format of the input files when the
    // new update api is ready
    std::filesystem::path relative_path(x);
    auto result = load_raw_config(
      pool_,
      root_path / relative_path,
      relative_path
    );
    switch (result.status) {
      case LoadRawConfigStatus::OK: // Fallback
      case LoadRawConfigStatus::FILE_DONT_EXISTS:
        items.push_back(result);
        continue;

      case LoadRawConfigStatus::ERROR:
        return false;
    }

    return false;
  }

  return true;
}

} /* command */
} /* worker */
} /* mhconfig */
