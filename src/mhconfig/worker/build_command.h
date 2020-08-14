#ifndef MHCONFIG__WORKER__BUILD_COMMAND_H
#define MHCONFIG__WORKER__BUILD_COMMAND_H

#include <memory>
#include <string>

#include <absl/container/flat_hash_map.h>

#include "mhconfig/api/config/common.h"
#include "mhconfig/command.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/builder.h"

namespace mhconfig
{
namespace worker
{

class BuildCommand final : public WorkerCommand
{
public:
  BuildCommand(
    std::shared_ptr<config_namespace_t> cn,
    std::shared_ptr<pending_build_t>&& pending_build
  );

  std::string name() const override;

  bool force_take_metric() const override;

  bool execute(
    context_t* ctx
  ) override;

private:
  enum class CheckDependenciesStatus {
    OK,
    REF_GRAPH_IS_NOT_DAG,
    MISSING_DEPENDENCY,
    INVALID_VERSION
  };

  std::shared_ptr<config_namespace_t> cn_;
  std::shared_ptr<pending_build_t> pending_build_;

  CheckDependenciesStatus check_dependencies();

  CheckDependenciesStatus check_dependencies_rec(
    build_element_t* build_element,
    absl::flat_hash_set<std::string>& dfs_document_names,
    absl::flat_hash_set<std::string>& all_document_names,
    std::string& overrides_key,
    bool is_root
  );

  void decrease_pending_elements(
    context_t* ctx,
    pending_build_t* pending_build
  );

  CheckDependenciesStatus finish_build_elements_rec(
    context_t* ctx,
    build_element_t* build_element,
    absl::flat_hash_map<std::string, merged_config_t*>& merged_config_by_document_name
  );

  void build(
    context_t* ctx,
    build_element_t* build_element,
    absl::flat_hash_map<std::string, Element>& element_by_document_name,
    bool is_root
  );
};

} /* worker */
} /* mhconfig */

#endif
