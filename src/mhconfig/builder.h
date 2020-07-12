#ifndef MHCONFIG__BUILDER_H
#define MHCONFIG__BUILDER_H

#include <filesystem>
#include <fstream>

#include <zlib.h>

#include <absl/container/flat_hash_set.h>

#include "mhconfig/string_pool.h"
#include "mhconfig/command.h"
#include "mhconfig/config_namespace.h"
#include "yaml-cpp/exceptions.h"
#include "jmutils/common.h"
#include "jmutils/box.h"
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

const static std::string TAG_NO_PLAIN_SCALAR{"!"};
const static std::string TAG_PLAIN_SCALAR{"?"};
const static std::string TAG_NULL{"tag:yaml.org,2002:null"};
const static std::string TAG_STR{"tag:yaml.org,2002:str"};
const static std::string TAG_INT{"tag:yaml.org,2002:int"};
const static std::string TAG_FLOAT{"tag:yaml.org,2002:float"};
const static std::string TAG_BOOL{"tag:yaml.org,2002:bool"};

const static std::string TAG_FORMAT{"!format"};
const static std::string TAG_SREF{"!sref"};
const static std::string TAG_REF{"!ref"};
const static std::string TAG_DELETE{"!delete"};
const static std::string TAG_OVERRIDE{"!override"};

enum class LoadRawConfigStatus {
  OK,
  INVALID_FILENAME,
  FILE_DONT_EXISTS,
  ERROR
};

struct load_raw_config_result_t {
  LoadRawConfigStatus status;
  std::string override_;
  std::string document;
  std::string flavor;
  std::shared_ptr<raw_config_t> raw_config{nullptr};
};

std::shared_ptr<config_namespace_t> make_config_namespace(
  const std::filesystem::path& root_path,
  metrics::MetricsService& metrics
);

inline void make_override_path(
  const std::string& override_,
  const std::string& document,
  const std::string& flavor,
  std::string& output
) {
  output.clear();
  output.reserve(override_.size() + 1 + document.size() + 1 + flavor.size());
  output += override_;
  output.push_back('/');
  output += document;
  output.push_back('.');
  output += flavor;
}

inline void split_override_path(
  const std::string& override_path,
  std::string& override_,
  std::string& document,
  std::string& flavor
) {
  assert(!override_path.empty());
  size_t end_override_idx = override_path.size()-1;
  size_t end_document_idx = end_override_idx;
  for (; override_path[end_override_idx] != '/'; --end_override_idx) {
    if (override_path[end_override_idx] == '.') {
      end_document_idx = end_override_idx;
    }
  }

  override_.clear();
  override_.insert(0, override_path, 0, end_override_idx);

  document.clear();
  document.insert(0, override_path, end_override_idx+1, end_document_idx-end_override_idx);

  flavor.clear();
  document.insert(0, override_path, end_document_idx+1, override_path.size()-end_document_idx);
}

template <typename T>
void load_raw_config(
  const std::filesystem::path& path,
  T lambda,
  load_raw_config_result_t& result
) {
  try {
    if (!std::filesystem::exists(path)) {
      spdlog::debug("The file '{}' don't exists", path.string());
      result.status = LoadRawConfigStatus::FILE_DONT_EXISTS;
      return;
    }

    spdlog::debug("Loading file '{}'", path.string());
    std::ifstream fin(path.string());
    if (!fin.good()) {
      spdlog::error("Some error take place reading the file '{}'", path.string());
      return;
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
    result.raw_config->has_content = true;
    lambda(data, result);
  } catch(const std::exception &e) {
    spdlog::error(
      "Error making the element (path: '{}'): {}",
      path.string(),
      e.what()
    );
    return;
  } catch(...) {
    spdlog::error(
      "Unknown error making the element (path: '{}')",
      path.string()
    );
    return;
  }

  result.status = LoadRawConfigStatus::OK;
}

load_raw_config_result_t index_file(
  ::string_pool::Pool* pool,
  const std::filesystem::path& root_path,
  const std::filesystem::path& path
);

template <typename T>
bool index_files(
  ::string_pool::Pool* pool,
  const std::filesystem::path& root_path,
  T lambda
) {
  spdlog::debug("To index the files in the path '{}'", root_path.string());

  std::string override_path;

  std::error_code error_code;
  for (
    std::filesystem::recursive_directory_iterator it(root_path, error_code), end;
    !error_code && (it != end);
    ++it
  ) {
    if (it->path().filename().native()[0] == '.') {
      it.disable_recursion_pending();
    } else if (it->is_regular_file()) {
      auto result = index_file(pool, root_path, it->path());
      if (result.status != LoadRawConfigStatus::INVALID_FILENAME) {
        make_override_path(
          result.override_,
          result.document,
          result.flavor,
          override_path
        );

        bool ok = lambda(
          static_cast<const decltype(override_path)&>(override_path),
          std::move(result)
        );
        if (!ok) {
          spdlog::error(
            "Some error take place processing the file '{}'",
            it->path().string()
          );
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

enum class AffectedDocumentStatus {
  TO_REMOVE,
  TO_ADD,
  DEPENDENCY
};

void increment_version_of_the_affected_documents(
  config_namespace_t& config_namespace,
  absl::flat_hash_map<std::pair<std::string, std::string>, absl::flat_hash_map<std::string, AffectedDocumentStatus>>& updated_documents_by_flavor_and_override,
  absl::flat_hash_set<std::shared_ptr<api::stream::WatchInputMessage>>& watchers_to_trigger,
  bool only_nonexistent
);

void fill_affected_documents(
  const config_namespace_t& config_namespace,
  absl::flat_hash_map<std::string, AffectedDocumentStatus>& affected_documents
);

Element override_with(
  const Element& a,
  const Element& b,
  const absl::flat_hash_map<std::string, Element> &elements_by_document
);

NodeType get_virtual_node_type(
  const Element& element
);

bool apply_tags(
  ::string_pool::Pool* pool,
  const Element& element,
  const Element& root,
  const absl::flat_hash_map<std::string, Element> &elements_by_document,
  Element& result
);

Element apply_tag_format(
  ::string_pool::Pool* pool,
  const Element& element
);

std::string format_str(
  const std::string& templ,
  const std::vector<std::pair<std::string, std::string>>& template_arguments
);

Element apply_tag_ref(
  const Element& element,
  const absl::flat_hash_map<std::string, Element> &elements_by_document
);

Element apply_tag_sref(
  const Element& child,
  Element root
);

/*
 * All the structure checks must be done here
 */
Element make_and_check_element(
  ::string_pool::Pool* pool,
  YAML::Node &node,
  absl::flat_hash_set<std::string> &reference_to
);

bool are_valid_arguments(
  const std::vector<std::string>& overrides,
  const std::vector<std::string>& flavors,
  const std::string& document,
  const std::string& template_
);

bool is_a_valid_path(
  const Sequence* path,
  const std::string& tag
);

Element make_element(
  ::string_pool::Pool* pool,
  YAML::Node &node,
  absl::flat_hash_set<std::string> &reference_to
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

template <typename F>
inline void with_raw_config(
  config_namespace_t& config_namespace,
  const std::string& override_path,
  uint32_t version,
  F lambda
) {
  spdlog::trace(
    "Obtaining the raw config of the override path '{}' with version '{}'",
    override_path,
    version
  );

  auto override_search = config_namespace.override_metadata_by_override_path
    .find(override_path);

  if (override_search == config_namespace.override_metadata_by_override_path.end()) {
    spdlog::trace("Don't exists the override path '{}'", override_path);
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

  lambda(
    static_cast<const decltype(override_path)&>(override_path),
    //static_cast<decltype(override_search->second)&>(override_search->second),
    static_cast<decltype(raw_config_search->second)&>(raw_config_search->second)
  );
}

template <typename F>
inline void for_each_document_override(
  config_namespace_t& config_namespace,
  const std::vector<std::string>& flavors,
  const std::vector<std::string>& overrides,
  const std::string& document,
  uint32_t version,
  F lambda
) {
  for_each_document_override_path(
    flavors,
    overrides,
    document,
    [&config_namespace, version, lambda](const auto& override_path) {
      with_raw_config(
        config_namespace,
        override_path,
        version,
        lambda
      );
    }
  );
}

template <typename F>
inline void for_each_document_override_path(
  const std::vector<std::string>& flavors,
  const std::vector<std::string>& overrides,
  const std::string& document,
  F lambda
) {
  std::string override_path;
  size_t flavors_l = flavors.size();
  size_t overrides_l = overrides.size();

  for (size_t override_idx = 0; override_idx < overrides_l; ++override_idx) {
    make_override_path(
      overrides[override_idx],
      document,
      "",
      override_path
    );
    lambda(static_cast<const decltype(override_path)&>(override_path));
  }

  for (size_t flavor_idx = 0; flavor_idx < flavors_l; ++flavor_idx) {
    for (size_t override_idx = 0; override_idx < overrides_l; ++override_idx) {
      make_override_path(
        overrides[override_idx],
        document,
        flavors[flavor_idx],
        override_path
      );
      lambda(static_cast<const decltype(override_path)&>(override_path));
    }
  }
}

bool is_a_valid_document_name(const std::string& document);

bool has_last_version(
  const override_metadata_t& override_metadata
);

} /* builder */
} /* mhconfig */

#endif
