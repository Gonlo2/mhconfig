#include "mhconfig/worker/update_command.h"

namespace mhconfig
{
namespace worker
{

UpdateCommand::UpdateCommand(
  uint64_t namespace_id,
  std::shared_ptr<jmutils::string::Pool> pool,
  std::shared_ptr<::mhconfig::api::request::UpdateRequest> update_request
)
  : WorkerCommand(),
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
  absl::flat_hash_map<std::string, load_raw_config_result_t> items;
  items.reserve(update_request_->relative_paths().size());

  bool ok;
  if (update_request_->reload()) {
    ok = index_files(
      pool_.get(),
      update_request_->root_path(),
      [&items](const auto& override_path, auto&& result) {
        if (result.status != LoadRawConfigStatus::OK) {
          return false;
        }
        items.emplace(override_path, std::move(result));
        return true;
      }
    );
  } else {
    ok = add_items(items);
  }

  if (ok) {
    context.scheduler_queue->push(
      std::make_unique<::mhconfig::scheduler::UpdateDocumentsCommand>(
        namespace_id_,
        update_request_,
        std::move(items)
      )
    );
  } else {
    update_request_->set_namespace_id(namespace_id_);
    update_request_->set_status(::mhconfig::api::request::UpdateRequest::Status::ERROR);
    update_request_->commit();
  }

  return true;
}

bool UpdateCommand::add_items(
  absl::flat_hash_map<std::string, load_raw_config_result_t>& items
) {
  std::filesystem::path root_path(update_request_->root_path());
  std::string override_path;
  for (const std::string& x : update_request_->relative_paths()) {
    std::filesystem::path relative_file_path(x);
    auto path = root_path / relative_file_path;
    auto result = index_file(pool_.get(), root_path, path);
    switch (result.status) {
      case LoadRawConfigStatus::OK: // Fallback
      case LoadRawConfigStatus::FILE_DONT_EXISTS:
        make_override_path(
          result.override_,
          result.document,
          result.flavor,
          override_path
        );
        items.emplace(override_path, std::move(result));
        continue;

      case LoadRawConfigStatus::INVALID_FILENAME: // Fallback
      case LoadRawConfigStatus::ERROR:
        return false;
    }

    return false;
  }

  return true;
}

} /* worker */
} /* mhconfig */
