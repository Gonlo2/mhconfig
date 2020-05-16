#ifndef MHCONFIG__API__STREAM__OUTPUT_STREAM_H
#define MHCONFIG__API__STREAM__OUTPUT_STREAM_H

#include "mhconfig/api/commitable.h"

namespace mhconfig
{
namespace api
{
namespace stream
{

class OutputMessage : public Commitable
{
public:
  OutputMessage() {
  }
  virtual ~OutputMessage() {
  }

  bool commit() override final;

  virtual bool send(bool finish = false) = 0;
};

} /* stream */
} /* api */
} /* mhconfig */

#endif
