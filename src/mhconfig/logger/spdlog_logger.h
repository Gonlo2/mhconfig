#ifndef MHCONFIG__LOGGER__SPDLOG_LOGGER_H
#define MHCONFIG__LOGGER__SPDLOG_LOGGER_H

#include "spdlog/spdlog.h"
#include "mhconfig/logger/logger.h"

#define define_spdlog_logger_method(LEVEL) \
  void LEVEL( \
    const char* message \
  ) override { \
    spdlog::LEVEL(message); \
  } \
\
  void LEVEL( \
    const char* message, \
    const Element& element \
  ) override { \
    spdlog::LEVEL( \
      "[doc_id: {}, rc_id: {}, line: {}, col: {}] : {}", \
      element.document_id(), \
      element.raw_config_id(), \
      element.line(), \
      (uint32_t) element.col(), \
      message \
    ); \
  } \
\
  void LEVEL( \
    const char* message, \
    const Element& element, \
    const Element& origin \
  ) override { \
    spdlog::LEVEL( \
      "[doc_id: {}, rc_id: {}, line: {}, col: {}] <- [doc_id: {}, rc_id: {}, line: {}, col: {}] : {}", \
      element.document_id(), \
      element.raw_config_id(), \
      element.line(), \
      (uint32_t) element.col(), \
      origin.document_id(), \
      origin.raw_config_id(), \
      origin.line(), \
      (uint32_t) origin.col(), \
      message \
    ); \
  }

namespace mhconfig
{
namespace logger
{

class SpdlogLogger final : public Logger
{
public:
  define_spdlog_logger_method(error)
  define_spdlog_logger_method(warn)
  define_spdlog_logger_method(debug)
  define_spdlog_logger_method(trace)
};

} /* logger */
} /* mhconfig */

#endif
