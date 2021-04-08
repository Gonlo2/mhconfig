#ifndef MHCONFIG__LOGGER__LOGGER_H
#define MHCONFIG__LOGGER__LOGGER_H

#include "mhconfig/element.h"
#include "jmutils/string/pool.h"

#define define_logger_method(LEVEL) \
  virtual void LEVEL( \
    const char* message \
  ) = 0; \
\
  virtual void LEVEL( \
    const char* message, \
    const Element& element \
  ) = 0; \
\
  virtual void LEVEL( \
    const char* message, \
    const Element& element, \
    const Element& origin \
  ) = 0;

namespace mhconfig
{
namespace logger
{

class Logger
{
public:
  virtual ~Logger() {
  }

  define_logger_method(error)
  define_logger_method(warn)
  define_logger_method(debug)
  define_logger_method(trace)
};

} /* logger */
} /* mhconfig */

#endif
