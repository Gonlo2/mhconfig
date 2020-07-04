#ifndef MHCONFIG__WORKER__API_REPLY_COMMAND_H
#define MHCONFIG__WORKER__API_REPLY_COMMAND_H

#include <memory>
#include <string>

#include "mhconfig/command.h"
#include "mhconfig/api/request/request.h"

namespace mhconfig
{
namespace worker
{

class ApiReplyCommand : public WorkerCommand
{
public:
  ApiReplyCommand(
    std::shared_ptr<::mhconfig::api::Commitable>&& commitable
  );
  virtual ~ApiReplyCommand();

  std::string name() const override;

  bool execute(
    context_t& context
  ) override;

private:
  std::shared_ptr<::mhconfig::api::Commitable> commitable_;
};

} /* worker */
} /* mhconfig */

#endif
