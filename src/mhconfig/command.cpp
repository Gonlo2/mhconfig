#include "mhconfig/command.h"

namespace mhconfig
{

WorkerCommand::~WorkerCommand() {
}

bool WorkerCommand::force_take_metric() const {
  return false;
}

} /* mhconfig */
