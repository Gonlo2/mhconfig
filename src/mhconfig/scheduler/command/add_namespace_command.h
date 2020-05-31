#ifndef MHCONFIG__SCHEDULER__COMMAND__ADD_NAMESPACE_COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__ADD_NAMESPACE_COMMAND_H

#include <memory>

#include "mhconfig/scheduler/command/command.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

class AddNamespaceCommand : public Command
{
public:
  explicit AddNamespaceCommand(
    std::shared_ptr<config_namespace_t> config_namespace
  );
  virtual ~AddNamespaceCommand();

  std::string name() const override;

  CommandType command_type() const override;

  const std::string& namespace_path() const override;
  uint64_t namespace_id() const override;
  const std::shared_ptr<config_namespace_t> config_namespace() const override;

private:
  std::shared_ptr<config_namespace_t> config_namespace_;
};

} /* command */
} /* scheduler */
} /* mhconfig */

#endif
