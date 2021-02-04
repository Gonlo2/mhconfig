#ifndef MHCONFIG__ELEMENT_BUILDER_H
#define MHCONFIG__ELEMENT_BUILDER_H

#include "jmutils/base64.h"
#include "mhconfig/element.h"
#include "mhconfig/string_pool.h"
#include "yaml-cpp/exceptions.h"
#include "mhconfig/logger/replay_logger.h"
#include "mhconfig/constants.h"

namespace mhconfig
{

class ElementBuilder final
{
public:
  ElementBuilder(
    logger::ReplayLogger& logger,
    jmutils::string::Pool* pool,
    const std::string& document,
    absl::flat_hash_set<std::string>& reference_to
  );

  Element make_and_check(YAML::Node &node);

private:
  logger::ReplayLogger& logger_;
  jmutils::string::Pool* pool_;
  const std::string& document_;
  absl::flat_hash_set<std::string>& reference_to_;

  inline Element make(YAML::Node &node);
  Element make_from_scalar(YAML::Node &node);
  Element make_from_plain_scalar(YAML::Node &node);
  Element make_from_format(YAML::Node &node);
  Element make_from_int64(YAML::Node &node);
  Element make_from_double(YAML::Node &node);
  Element make_from_bool(YAML::Node &node);
  Element make_from_map(YAML::Node &node);
  Element make_from_seq(YAML::Node &node);

  bool is_a_valid_path(
    const Element& element
  );

  std::optional<std::string> parse_format_slice(
    const Element& element,
    const std::string& tmpl,
    size_t& idx
  );

  template <typename... Args>
  inline Element make_element(YAML::Node& node, Args&&... args)
  {
    Element element(std::forward<Args>(args)...);
    auto mark = node.Mark();
    element.set_position(mark.line, mark.column);
    return element;
  }

  template <typename M, typename... Args>
  inline Element make_element_and_trace(
    M message,
    Args&&... args
  ) {
    auto element = make_element(std::forward<Args>(args)...);
    logger_.trace(message, element);
    return element;
  }

  template <typename M, typename... Args>
  inline Element make_element_and_error(
    M message,
    Args&&... args
  ) {
    auto element = make_element(std::forward<Args>(args)...);
    logger_.trace(message, element);
    return element;
  }

};

} /* mhconfig */

#endif
