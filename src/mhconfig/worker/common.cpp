#include "mhconfig/worker/common.h"

namespace mhconfig
{
namespace worker
{
namespace command
{

std::string to_string(CommandType type) {
  switch (type) {
    case API: return "API";
    case SETUP_REQUEST: return "SETUP_REQUEST";
    case SETUP_RESPONSE: return "SETUP_RESPONSE";
    case GET_REQUEST: return "GET_REQUEST";
    case GET_RESPONSE: return "GET_RESPONSE";
    case UPDATE_REQUEST: return "UPDATE_REQUEST";
    case UPDATE_RESPONSE: return "UPDATE_RESPONSE";
    case BUILD_REQUEST: return "BUILD_REQUEST";
    case BUILD_RESPONSE: return "BUILD_RESPONSE";
    case RUN_GC_REQUEST: return "RUN_GC_REQUEST";
    case RUN_GC_RESPONSE: return "RUN_GC_RESPONSE";
  }

  return "unknown";
}

} /* command */
} /* worker */
} /* mhconfig */
