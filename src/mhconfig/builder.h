#ifndef MHCONFIG__BUILDER_H
#define MHCONFIG__BUILDER_H

#include "mhconfig/worker/command/command.h"
#include "mhconfig/ds/config_namespace.h"
#include "jmutils/filesystem/common.h"
#include "jmutils/common.h"

#include "spdlog/spdlog.h"

namespace mhconfig
{
namespace builder
{

const static std::string TAG_FORMAT{"!format"};
const static std::string TAG_SREF{"!sref"};
const static std::string TAG_REF{"!ref"};
const static std::string TAG_DELETE{"!delete"};
const static std::string TAG_OVERRIDE{"!override"};

enum LoadRawConfigStatus {
  OK,
  INVALID_FILE,
  FILE_DONT_EXISTS,
  ERROR
};

struct load_raw_config_result_t {
  LoadRawConfigStatus status;
  std::string document;
  std::string override_;
  std::shared_ptr<raw_config_t> raw_config{nullptr};
};

std::shared_ptr<config_namespace_t> index_files(
  const std::string& root_path
);

load_raw_config_result_t load_raw_config(
  std::shared_ptr<string_pool::Pool> pool,
  const std::string& root_path,
  const std::string& relative_path
);

ElementRef override_with(
  ElementRef a,
  ElementRef b,
  const std::unordered_map<std::string, ElementRef> &ref_elements_by_document
);

NodeType get_virtual_node_type(ElementRef element);

ElementRef apply_tags(
  std::shared_ptr<string_pool::Pool> pool,
  ElementRef element,
  ElementRef root,
  const std::unordered_map<std::string, ElementRef> &ref_elements_by_document
);

ElementRef apply_tag_format(
  std::shared_ptr<string_pool::Pool> pool,
  ElementRef element
);

std::string format_str(
  const std::string& templ,
  uint32_t num_arguments,
  const std::vector<std::pair<std::string, std::string>>& template_arguments
);

ElementRef apply_tag_ref(
  ElementRef element,
  const std::unordered_map<std::string, ElementRef> &ref_elements_by_document
);

ElementRef apply_tag_sref(
  ElementRef child,
  ElementRef root
);

/*
 * All the structure checks must be done here
 */
ElementRef make_and_check_element(
    std::shared_ptr<string_pool::Pool> pool,
    YAML::Node &node,
    std::unordered_set<std::string> &reference_to
);

ElementRef make_element(
    std::shared_ptr<string_pool::Pool> pool,
    YAML::Node &node,
    std::unordered_set<std::string> &reference_to
);

} /* builder */
} /* mhconfig */

#endif
