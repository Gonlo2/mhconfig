#ifndef MHCONFIG__LOGGER__MULTI_LOGGER_H
#define MHCONFIG__LOGGER__MULTI_LOGGER_H

#include "mhconfig/logger/logger.h"

#define define_multi_logger_method(LEVEL) \
  void LEVEL( \
    const char* message \
  ) override { \
    logger_1_->LEVEL(message); \
    logger_2_->LEVEL(message); \
  } \
\
  void LEVEL( \
    const char* message, \
    const Element& element \
  ) override { \
    logger_1_->LEVEL(message, element); \
    logger_2_->LEVEL(message, element); \
  } \
\
  void LEVEL( \
    const char* message, \
    const Element& element, \
    const Element& origin \
  ) override { \
    logger_1_->LEVEL(message, element, origin); \
    logger_2_->LEVEL(message, element, origin); \
  }

namespace mhconfig
{
namespace logger
{

template <typename T>
class BiLogger final : public Logger
{
public:
  BiLogger(
    T logger_1,
    T logger_2
  ) : logger_1_(logger_1),
    logger_2_(logger_2)
  {
  }

  define_multi_logger_method(error)
  define_multi_logger_method(warn)
  define_multi_logger_method(info)
  define_multi_logger_method(debug)
  define_multi_logger_method(trace)

private:
  T logger_1_;
  T logger_2_;
};

} /* logger */
} /* mhconfig */

#endif
