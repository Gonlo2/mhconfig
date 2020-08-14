#ifndef MHCONFIG__WORKER__UPDATE_COMMAND_H
#define MHCONFIG__WORKER__UPDATE_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/command.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/api/stream/watch_stream_impl.h"
#include "mhconfig/builder.h"
#include "mhconfig/provider.h"
#include "mhconfig/worker/setup_command.h"

namespace mhconfig
{
namespace worker
{

class UpdateCommand final : public WorkerCommand
{
public:
  template <typename T>
  UpdateCommand(T&& cn) : cn_(std::forward<T>(cn)) {
  }

  std::string name() const override;

  bool force_take_metric() const override;

  bool execute(
    context_t* ctx
  ) override;

private:
  typedef absl::flat_hash_map<
    std::string,
    absl::flat_hash_map<std::string, load_raw_config_result_t>
  > files_to_update_t;

  std::shared_ptr<config_namespace_t> cn_;

  bool process(
    context_t* ctx,
    api::request::UpdateRequest* request
  );

  bool index_request_files(
    const api::request::UpdateRequest* request,
    files_to_update_t& files_to_update
  );

  files_to_update_t obtain_missing_files(
    files_to_update_t& files_to_update
  );

  void filter_existing_documents(
    files_to_update_t& files_to_update
  );

  bool update_reference_counters(
    files_to_update_t& files_to_update
  );

  bool update_documents(
    files_to_update_t& files_to_update
  );

  void trigger_watchers(
    context_t* ctx,
    const absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, AffectedDocumentStatus>>& dep_by_doc
  );

};

} /* worker */
} /* mhconfig */

#endif
