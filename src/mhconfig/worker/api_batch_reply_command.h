#ifndef MHCONFIG__WORKER__API_BATCH_REPLY_COMMAND_H
#define MHCONFIG__WORKER__API_BATCH_REPLY_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/command.h"
#include "mhconfig/api/commitable.h"

namespace mhconfig
{
namespace worker
{

class ApiBatchReplyCommand : public WorkerCommand
{
public:
  ApiBatchReplyCommand(
    std::vector<std::shared_ptr<api::Commitable>>&& commitables
  );
  virtual ~ApiBatchReplyCommand();

  std::string name() const override;

  bool execute(
    context_t& context
  ) override;

private:
  std::vector<std::shared_ptr<api::Commitable>> commitables_;
};

} /* worker */
} /* mhconfig */

#endif
