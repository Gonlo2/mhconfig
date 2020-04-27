#ifndef MHCONFIG__WORKER__COMMAND__COMMAND_H
#define MHCONFIG__WORKER__COMMAND__COMMAND_H

#include <memory>

namespace mhconfig
{
namespace worker
{
namespace command
{

class Command
{
public:
  Command();
  virtual ~Command();

  virtual bool execute() = 0;

private:
  /* data */
};

typedef std::shared_ptr<Command> CommandRef;

} /* command */
} /* worker */
} /* mhconfig */

#endif
