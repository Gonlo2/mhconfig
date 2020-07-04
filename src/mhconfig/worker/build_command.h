#ifndef MHCONFIG__WORKER__BUILD_COMMAND_H
#define MHCONFIG__WORKER__BUILD_COMMAND_H

#include <memory>
#include <string>

#include <absl/container/flat_hash_map.h>

#include "mhconfig/scheduler/set_documents_command.h"
#include "mhconfig/command.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/builder.h"

#include <nlohmann/json.hpp>

namespace mhconfig
{
namespace worker
{

bool fill_json(
  const mhconfig::Element& root,
  nlohmann::json& output
);

class BuildCommand : public WorkerCommand
{
public:

  BuildCommand(
    uint64_t namespace_id,
    std::shared_ptr<::string_pool::Pool> pool,
    std::shared_ptr<build::wait_built_t>&& wait_build
  );
  virtual ~BuildCommand();

  std::string name() const override;

  bool force_take_metric() const override;

  bool execute(
    context_t& context
  ) override;

private:
  uint64_t namespace_id_;
  std::shared_ptr<::string_pool::Pool> pool_;
  std::shared_ptr<build::wait_built_t> wait_build_;
};

} /* worker */
} /* mhconfig */

#endif
