#include "mhconfig/api/session.h"

namespace mhconfig
{
namespace api
{

bool parse_from_byte_buffer(
  const grpc::ByteBuffer& buffer,
  grpc::protobuf::Message& message
) {
  std::vector<grpc::Slice> slices;
  buffer.Dump(&slices);
  grpc::string buf;
  buf.reserve(buffer.Length());
  for (auto s = slices.begin(); s != slices.end(); ++s) {
    buf.append(reinterpret_cast<const char*>(s->begin()), s->size());
  }
  return message.ParseFromString(buf);
}

} /* api */
} /* mhconfig */
