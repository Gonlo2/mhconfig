#include "mhconfig/api/stream/stream.h"

namespace mhconfig
{
namespace api
{
namespace stream
{

bool OutputMessage::commit() {
  return send();
}

} /* stream */
} /* api */
} /* mhconfig */
