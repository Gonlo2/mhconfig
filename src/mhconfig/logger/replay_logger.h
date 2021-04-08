#ifndef MHCONFIG__LOGGER__REPLAY_LOGGER_H
#define MHCONFIG__LOGGER__REPLAY_LOGGER_H

#include "mhconfig/logger/logger.h"

#define LOGGER_NUM_LEVELS_BITS 2
#define LOGGER_NUM_LEVELS (1<<LOGGER_NUM_LEVELS_BITS)
#define LOGGER_NUM_LEVELS_MASK (LOGGER_NUM_LEVELS-1)

#define define_replay_logger_method(LEVEL) \
  using Logger::LEVEL; \
\
  virtual void LEVEL( \
    jmutils::string::String&& message \
  ) = 0; \
\
  virtual void LEVEL( \
    jmutils::string::String&& message, \
    const Element& element \
  ) = 0; \
\
  virtual void LEVEL( \
    jmutils::string::String&& message, \
    const Element& element, \
    const Element& origin \
  ) = 0;

namespace mhconfig
{
namespace logger
{

class ReplayLogger : public Logger
{
public:
  enum class Level : uint8_t {
    error = 0,
    warn = 1,
    debug = 2,
    trace = 3
  };

  virtual ~ReplayLogger() {
  }

  define_replay_logger_method(error)
  define_replay_logger_method(warn)
  define_replay_logger_method(debug)
  define_replay_logger_method(trace)

  virtual void freeze() = 0;

  virtual void replay(
    Logger& logger,
    Level lower_or_equal_that = Level::trace
  ) const = 0;

  virtual bool empty() const = 0;
};

} /* logger */
} /* mhconfig */

#endif
