#ifndef MHCONFIG__LOGGER__COUNTER_LOGGER_H
#define MHCONFIG__LOGGER__COUNTER_LOGGER_H

#include "mhconfig/logger/logger.h"

#define define_counter_logger_method(LEVEL) \
  void LEVEL( \
    const char* message \
  ) override { \
    LEVEL ## _counter_ += 1; \
  } \
\
  void LEVEL( \
    const char* message, \
    const Element& element \
  ) override { \
    LEVEL ## _counter_ += 1; \
  } \
\
  void LEVEL( \
    const char* message, \
    const Element& element, \
    const Element& origin \
  ) override { \
    LEVEL ## _counter_ += 1; \
  } \
\
  size_t num_ ## LEVEL ## _logs() { \
    return LEVEL ## _counter_; \
  }


namespace mhconfig
{
namespace logger
{

class CounterLogger final : public Logger
{
public:
  define_counter_logger_method(error)
  define_counter_logger_method(warn)
  define_counter_logger_method(debug)
  define_counter_logger_method(trace)

private:
  uint32_t error_counter_{0};
  uint32_t warn_counter_{0};
  uint32_t debug_counter_{0};
  uint32_t trace_counter_{0};
};

} /* logger */
} /* mhconfig */

#endif
