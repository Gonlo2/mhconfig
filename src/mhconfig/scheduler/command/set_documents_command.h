#ifndef MHCONFIG__SCHEDULER__COMMAND__SET_DOCUMENTS_COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__SET_DOCUMENTS_COMMAND_H

#include <memory>

#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/config/basic_merged_config.h"
#include "mhconfig/scheduler/command/command.h"
#include "mhconfig/worker/command/api_reply_command.h"
#include "mhconfig/worker/command/build_command.h"
#include "mhconfig/builder.h"
#include "jmutils/time.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

using namespace mhconfig::ds::config_namespace;

class SetDocumentsCommand : public Command
{
public:
  SetDocumentsCommand(
    uint64_t namespace_id,
    std::shared_ptr<build::wait_built_t> wait_build,
    std::unordered_map<std::string, build::built_element_t> built_elements_by_document
  );

  virtual ~SetDocumentsCommand();

  std::string name() const override;

  CommandType command_type() const override;
  uint64_t namespace_id() const override;

  NamespaceExecutionResult execute_on_namespace(
    std::shared_ptr<config_namespace_t> config_namespace,
    Queue<worker::command::CommandRef>& worker_queue
  ) override;

  bool on_get_namespace_error(
    Queue<worker::command::CommandRef>& worker_queue
  ) override;

private:
  uint64_t namespace_id_;
  std::shared_ptr<build::wait_built_t> wait_build_;
  std::unordered_map<std::string, build::built_element_t> built_elements_by_document_;
};

} /* command */
} /* scheduler */
} /* mhconfig */

#endif
