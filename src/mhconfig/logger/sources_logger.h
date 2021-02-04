#ifndef MHCONFIG__LOGGER__SOURCES_LOGGER_H
#define MHCONFIG__LOGGER__SOURCES_LOGGER_H

#include "mhconfig/logger/logger.h"
#include "mhconfig/api/common.h"

#define define_sources_logger_method(LEVEL) \
  void LEVEL( \
    const char* message \
  ) override { \
  } \
\
  void LEVEL( \
    const char* message, \
    const Element& element \
  ) override { \
    sources_.emplace(element.document_id(), element.raw_config_id()); \
  } \
\
  void LEVEL( \
    const char* message, \
    const Element& element, \
    const Element& origin \
  ) override { \
    sources_.emplace(element.document_id(), element.raw_config_id()); \
    sources_.emplace(origin.document_id(), origin.raw_config_id()); \
  }

namespace mhconfig
{
namespace logger
{

class SourcesLogger final : public Logger
{
public:
  define_sources_logger_method(error)
  define_sources_logger_method(warn)
  define_sources_logger_method(info)
  define_sources_logger_method(debug)
  define_sources_logger_method(trace)

  api::SourceIds& sources() {
    return sources_;
  };

private:
  api::SourceIds sources_;
};

} /* logger */
} /* mhconfig */

#endif
