#ifndef MHCONFIG__API__COMMITABLE_H
#define MHCONFIG__API__COMMITABLE_H

namespace mhconfig
{
namespace api
{

class Commitable
{
public:
  Commitable() {
  }
  virtual ~Commitable() {
  }

  virtual bool commit() = 0;
};

} /* api */
} /* mhconfig */

#endif
