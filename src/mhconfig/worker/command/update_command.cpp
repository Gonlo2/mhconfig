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

  bool ok;
  if (update_request_->reload()) {
    ok = index_files(
      pool_.get(),
      update_request_->root_path(),
      [&](load_raw_config_result_t&& result) {
        if (result.status != LoadRawConfigStatus::OK) {
          return false;
        }
        items.emplace_back(std::move(result));
        return true;
      }
    );
  } else {
    ok = add_items(items);
  }

  if (ok) {
    context.scheduler_queue->push(
      std::make_unique<::mhconfig::scheduler::command::UpdateDocumentsCommand>(
        namespace_id_,
        update_request_,
        std::move(items)
      )
    );
  } else {
    update_request_->set_namespace_id(namespace_id_);
    update_request_->set_status(::mhconfig::api::request::update_request::Status::ERROR);
    update_request_->commit();
  }

  return true;
}

bool UpdateCommand::add_items(
  std::vector<load_raw_config_result_t>& items
) {
  std::filesystem::path root_path(update_request_->root_path());
  for (const std::string& x : update_request_->relative_paths()) {
    std::filesystem::path relative_file_path(x);
    auto path = root_path / relative_file_path;
    if (!is_a_valid_filename(path)) {
      return false;
    }

    load_raw_config_result_t result;

    if (path.filename().native()[0] == '_') {
      result = load_template_raw_config(
        path.filename().string(),
        relative_file_path.parent_path().string(),
        path
      );
    } else if (path.extension() == ".yaml") {
      result = load_yaml_raw_config(
        path.stem().string(),
        relative_file_path.parent_path().string(),
        path,
        pool_.get()
      );
    } else {
      assert(false);
    }

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
