#ifndef MHCONFIG__LOGGER__API_LOGGER_H
#define MHCONFIG__LOGGER__API_LOGGER_H

#include "mhconfig/logger/logger.h"
#include "mhconfig/api/common.h"

#define define_api_logger_method(LEVEL, LEVEL_MAYUS) \
  void LEVEL( \
    const char* message \
  ) override { \
    std::string_view sv(message); \
    message_->add_log( \
      api::LogLevel::LEVEL_MAYUS, \
      sv \
    ); \
  } \
\
  void LEVEL( \
    const char* message, \
    const Element& element \
  ) override { \
    std::string_view sv(message); \
    message_->add_log( \
      api::LogLevel::LEVEL_MAYUS, \
      sv, \
      get_position(element) \
    ); \
  } \
\
  void LEVEL( \
    const char* message, \
    const Element& element, \
    const Element& origin \
  ) override { \
    std::string_view sv(message); \
    message_->add_log( \
      api::LogLevel::LEVEL_MAYUS, \
      sv, \
      get_position(element), \
      get_position(origin) \
    ); \
  }


namespace mhconfig
{
namespace logger
{

template <typename T>
class ApiLogger final : public Logger
{
public:
  template <typename V>
  ApiLogger(
    V&& message
  ) : message_(std::forward<V>(message)) {
  }

  define_api_logger_method(error, ERROR)
  define_api_logger_method(warn, WARN)
  define_api_logger_method(info, INFO)
  define_api_logger_method(debug, DEBUG)
  define_api_logger_method(trace, TRACE)

private:
  T message_;

  inline api::position_t get_position(
    const Element& element
  ) {
    auto id = api::make_source_id(
      element.document_id(),
      element.raw_config_id()
    );

    return api::position_t{
      .source_id = id,
      .line = element.line(),
      .col = element.col()
    };
  }
};

} /* logger */
} /* mhconfig */

#endif
