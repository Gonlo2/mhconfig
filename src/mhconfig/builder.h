#ifndef MHCONFIG__BUILDER_H
#define MHCONFIG__BUILDER_H

#include <filesystem>
#include <fstream>

#include <zlib.h>

#include "mhconfig/string_pool.h"
#include "mhconfig/worker/command/command.h"
#include "mhconfig/ds/config_namespace.h"
#include "yaml-cpp/exceptions.h"
#include "jmutils/common.h"
#include "jmutils/time.h"

#include "spdlog/spdlog.h"

#include <inja/inja.hpp>

namespace mhconfig
{
namespace builder
{

namespace {
  class ForbiddenIncludeException : public std::exception
  {
    const char * what() const noexcept override {
      return "Include a template is forbidden";
    }
  };

  class ForbiddenIncludeStrategy final : public inja::IncludeStrategy
  {
  public:
    inja::Template parse_template(
      const inja::ParserConfig& parser_config,
      const inja::LexerConfig& lexer_config,
      inja::TemplateStorage& included_templates,
      inja::IncludeStrategy& include_strategy,
      nonstd::string_view filename
    ) override {
      throw ForbiddenIncludeException();
    }
  };
}

using namespace mhconfig::ds::config_namespace;

const static std::string TAG_FORMAT{"!format"};
const static std::string TAG_SREF{"!sref"};
const static std::string TAG_REF{"!ref"};
const static std::string TAG_DELETE{"!delete"};
const static std::string TAG_OVERRIDE{"!override"};

enum LoadRawConfigStatus {
  OK,
  FILE_DONT_EXISTS,
  ERROR
};

struct load_raw_config_result_t {
  LoadRawConfigStatus status;
  std::string document;
  std::string override_;
  std::shared_ptr<raw_config_t> raw_config{nullptr};
};

bool is_a_valid_filename(
  const std::filesystem::path& path
);

std::shared_ptr<config_namespace_t> index_files(
  const std::filesystem::path& root_path,
  metrics::MetricsService& metrics
);

template <typename T>
load_raw_config_result_t load_raw_config(
  const std::string& document,
  const std::string& override_,
  const std::filesystem::path& path,
  T lambda
) {
  load_raw_config_result_t result;
  result.status = LoadRawConfigStatus::ERROR;
  result.document = document;
  result.override_ = override_;

  try {
    if (!std::filesystem::exists(path)) {
      spdlog::debug("The file '{}' don't exists", path.string());
      result.status = LoadRawConfigStatus::FILE_DONT_EXISTS;
      return result;
    }

    spdlog::debug("Loading file '{}'", path.string());
    std::ifstream fin(path.string());
    if (!fin.good()) {
      spdlog::error("Some error take place reading the file '{}'", path.string());
      return result;
    }

    // TODO Move this to a function
    std::string data;
    fin.seekg(0, std::ios::end);
    data.reserve(fin.tellg());
    fin.seekg(0, std::ios::beg);
    data.assign(std::istreambuf_iterator<char>(fin), std::istreambuf_iterator<char>());

    // TODO Avoid import zlib only to calculate the crc32
    result.raw_config = std::make_shared<raw_config_t>();
    result.raw_config->crc32 = crc32(0, (const unsigned char*)data.c_str(), data.size());
    lambda(data, result);
  } catch(const std::exception &e) {
    spdlog::error(
      "Error making the element (path: '{}'): {}",
      path.string(),
      e.what()
    );
    return result;
  } catch(...) {
    spdlog::error(
      "Unknown error making the element (path: '{}')",
      path.string()
    );
    return result;
  }

  result.status = LoadRawConfigStatus::OK;
  return result;
}

load_raw_config_result_t load_yaml_raw_config(
  const std::string& document,
  const std::string& override_,
  const std::filesystem::path& path,
  ::string_pool::Pool* pool
);

load_raw_config_result_t load_template_raw_config(
  const std::string& document,
  const std::string& override_,
  const std::filesystem::path& path
);

template <typename T>
bool index_files(
  ::string_pool::Pool* pool,
  const std::filesystem::path& root_path,
  T lambda
) {
  spdlog::debug("To index the files in the path '{}'", root_path.string());

  std::error_code error_code;
  for (
    std::filesystem::recursive_directory_iterator it(root_path, error_code), end;
    !error_code && (it != end);
    ++it
  ) {
    auto first_filename_char = it->path().filename().native()[0];
    if (first_filename_char == '.') {
      it.disable_recursion_pending();
    } else if (it->is_regular_file()) {
      auto relative_path = std::filesystem::relative(it->path(), root_path)
        .parent_path()
        .string();

      if (first_filename_char == '_') {
        bool ok = lambda(
          load_template_raw_config(
            it->path().filename().string(),
            relative_path,
            it->path()
          )
        );
        if (!ok) {
          return false;
        }
      } else if (it->path().extension() == ".yaml") {
        bool ok = lambda(
          load_yaml_raw_config(
            it->path().stem().string(),
            relative_path,
            it->path(),
            pool
          )
        );
        if (!ok) {
          return false;
        }
      }
    }
  }

  if (error_code) {
    spdlog::error(
      "Some error take place obtaining the files on '{}': {}",
      root_path.string(),
      error_code.message()
    );
  }

  return !error_code;
}

ElementRef override_with(
  ElementRef a,
  ElementRef b,
  const std::unordered_map<std::string, ElementRef> &ref_elements_by_document
);

NodeType get_virtual_node_type(ElementRef element);

ElementRef apply_tags(
  ::string_pool::Pool* pool,
  ElementRef element,
  ElementRef root,
  const std::unordered_map<std::string, ElementRef> &ref_elements_by_document
);

ElementRef apply_tag_format(
  ::string_pool::Pool* pool,
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
  ::string_pool::Pool* pool,
  YAML::Node &node,
  std::unordered_set<std::string> &reference_to
);

ElementRef make_element(
  ::string_pool::Pool* pool,
  YAML::Node &node,
  std::unordered_set<std::string> &reference_to
);

// Get logic

std::shared_ptr<merged_config_t> get_or_build_merged_config(
  config_namespace_t& config_namespace,
  const std::string& overrides_key
);

std::shared_ptr<merged_config_t> get_merged_config(
  config_namespace_t& config_namespace,
  const std::string& overrides_key
);

template<typename F>
inline void with_raw_config(
  const document_metadata_t* document_metadata,
  const std::string& override_,
  uint32_t version,
  F lambda
) {
  spdlog::trace(
    "Obtaining the raw config of the override '{}' with version '{}'",
    override_,
    version
  );

  auto override_search = document_metadata->override_by_key
    .find(override_);

  if (override_search == document_metadata->override_by_key.end()) {
    spdlog::trace("Don't exists the override '{}'", override_);
    return;
  }

  auto& raw_config_by_version = override_search->second
    .raw_config_by_version;

  auto raw_config_search = (version == 0)
    ? raw_config_by_version.end()
    : raw_config_by_version.upper_bound(version);

  if (raw_config_search == raw_config_by_version.begin()) {
    spdlog::trace("Don't exists a version lower or equal to {}", version);
    return;
  }

  --raw_config_search;
  if (raw_config_search->second == nullptr) {
    spdlog::trace(
      "The raw_config value is deleted for the version {}",
      raw_config_search->first
    );
    return;
  }

  spdlog::trace(
    "Obtained a raw config with the version {}",
    raw_config_search->first
  );

  lambda(raw_config_search->second);
}

bool has_last_version(
  const override_metadata_t& override_metadata
);

void sanitize_tag(std::string& tag);

} /* builder */
} /* mhconfig */

#endif
