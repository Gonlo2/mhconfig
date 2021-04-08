#ifndef MHCONFIG__WORKER__BUILD_COMMAND_H
#define MHCONFIG__WORKER__BUILD_COMMAND_H

#include <absl/container/flat_hash_map.h>
#include <memory>
#include <optional>
#include <string>

#include "absl/container/flat_hash_set.h"
#include "mhconfig/builder.h"
#include "mhconfig/config_namespace.h"
#include "mhconfig/context.h"
#include "mhconfig/provider.h"
#include "mhconfig/element_merger.h"

namespace mhconfig
{
namespace worker
{

using api::request::GetRequest;

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
    enum class CheckStatus {
        OK,
        REF_GRAPH_IS_NOT_DAG,
        MISSING_DEPENDENCY,
        INVALID_VERSION
    };

    std::shared_ptr<config_namespace_t> cn_;
    std::shared_ptr<pending_build_t> pending_build_;

    void prepare_pending_build();

    void prepare_pending_build_rec(
        build_element_t& build_element,
        std::vector<std::string>& dfs_doc_names,
        absl::flat_hash_set<std::string>& dfs_doc_names_set,
        absl::flat_hash_set<std::string>& all_doc_names_set,
        const Element& cfg
    );

    void decrease_pending_elements(
        context_t* ctx,
        pending_build_t* pending_build
    );

    void build(
        context_t* ctx,
        pending_build_t* pending_build
    );

    void log_cycle(
        build_element_t& cycle_end_be,
        const std::string& cycle_start_doc_name,
        const std::vector<std::string>& dfs_doc_names,
        const absl::flat_hash_set<std::string>& dfs_doc_names_set
    );

};

} /* worker */
} /* mhconfig */

#endif
