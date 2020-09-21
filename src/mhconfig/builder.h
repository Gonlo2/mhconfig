#ifndef MHCONFIG__BUILDER_H
#define MHCONFIG__BUILDER_H

#include <filesystem>
#include <fstream>

#include <absl/container/flat_hash_set.h>

#include "mhconfig/string_pool.h"
#include "mhconfig/validator.h"
#include "mhconfig/command.h"
#include "mhconfig/config_namespace.h"
#include "yaml-cpp/exceptions.h"
#include "jmutils/common.h"
#include "jmutils/cow.h"
#include "jmutils/time.h"
#include "jmutils/base64.h"

#include "spdlog/spdlog.h"

namespace mhconfig
{

const static std::string TAG_NON_PLAIN_SCALAR{"!"};
const static std::string TAG_PLAIN_SCALAR{"?"};
const static std::string TAG_NONE{"tag:yaml.org,2002:null"};
const static std::string TAG_STR{"tag:yaml.org,2002:str"};
const static std::string TAG_BIN{"tag:yaml.org,2002:binary"};
const static std::string TAG_INT{"tag:yaml.org,2002:int"};
const static std::string TAG_DOUBLE{"tag:yaml.org,2002:float"};
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

enum class VirtualNode {
  UNDEFINED,
  MAP,
  SEQUENCE,
  LITERAL,
  REF
};

bool init_config_namespace(
  config_namespace_t* cn,
  std::shared_ptr<jmutils::string::Pool>&& pool
);

inline void make_override_path(
  const std::string& override_,
  const std::string& flavor,
  std::string& output
) {
  output.clear();
  output.reserve(override_.size() + 1 + flavor.size());
  output += override_;
  output.push_back('/');
  output += flavor;
}

struct split_override_path_result_t {
  std::string_view override_;
  std::string_view flavor;
};

inline split_override_path_result_t split_override_path(
  const std::string_view override_path
) {
  split_override_path_result_t result;
  auto override_end_pos = override_path.rfind('/');
  result.override_ = std::string_view(
    override_path.data(),
    override_end_pos
  );
  if (override_path.size() != override_end_pos+1) {
    result.flavor = std::string_view(
      &override_path[override_end_pos+1],
      override_path.size()-override_end_pos-1
    );
  }
  return result;
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

    result.raw_config = std::make_shared<raw_config_t>();
    result.raw_config->has_content = true;

    lambda(data, result);

    result.raw_config->value.freeze();

    auto full_checksum = result.raw_config->value.make_checksum();
    result.raw_config->checksum = 0;
    for (size_t i = 0; i < 8; ++i) {
      result.raw_config->checksum <<= 8;
      result.raw_config->checksum |= full_checksum[i];
    }
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
  jmutils::string::Pool* pool,
  const std::filesystem::path& root_path,
  const std::filesystem::path& path
);

template <typename T>
bool index_files(
  jmutils::string::Pool* pool,
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
  DEPENDENCY,
  TO_REMOVE_BUT_DEPENDENCY
};

absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, AffectedDocumentStatus>> get_dep_by_doc(
  config_namespace_t* cn,
  absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, AffectedDocumentStatus>>& updated_documents_by_path
);

void fill_affected_documents(
  config_namespace_t* cn,
  absl::flat_hash_map<std::string, AffectedDocumentStatus>& affected_documents
);

bool touch_affected_documents(
  config_namespace_t* cn,
  VersionId version,
  const absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, AffectedDocumentStatus>>& dep_by_doc,
  bool only_nonexistent
);

Element override_with(
  const Element& a,
  const Element& b,
  const absl::flat_hash_map<std::string, Element> &element_by_document_name
);

VirtualNode get_virtual_node_type(
  const Element& element
);

std::pair<bool, Element> apply_tags(
  jmutils::string::Pool* pool,
  Element element,
  const Element& root,
  const absl::flat_hash_map<std::string, Element> &element_by_document_name
);

Element apply_tag_format(
  jmutils::string::Pool* pool,
  const Element& element,
  const Element& root,
  const absl::flat_hash_map<std::string, Element> &element_by_document_name
);

Element apply_tag_ref(
  const Element& element,
  const absl::flat_hash_map<std::string, Element> &element_by_document_name
);

Element apply_tag_sref(
  const Element& child,
  Element root
);

/*
 * All the structure checks must be done here
 */
Element make_and_check_element(
  jmutils::string::Pool* pool,
  YAML::Node &node,
  const std::string& document,
  absl::flat_hash_set<std::string> &reference_to
);

bool is_a_valid_path(
  const Sequence* path,
  const std::string& tag
);

Element make_element(
  jmutils::string::Pool* pool,
  YAML::Node &node,
  const std::string& document,
  absl::flat_hash_set<std::string> &reference_to
);

Element make_element_from_scalar(
  jmutils::string::Pool* pool,
  YAML::Node &node,
  const std::string& document,
  absl::flat_hash_set<std::string> &reference_to
);

Element make_element_from_plain_scalar(
  jmutils::string::Pool* pool,
  YAML::Node &node
);

Element make_element_from_format(
  jmutils::string::Pool* pool,
  YAML::Node &node,
  const std::string& document,
  absl::flat_hash_set<std::string> &reference_to
);

std::optional<std::string> parse_format_slice(
  const std::string& tmpl,
  size_t& idx
);

Element make_element_from_int64(
  YAML::Node &node
);

Element make_element_from_double(
  YAML::Node &node
);

Element make_element_from_bool(
  YAML::Node &node
);

Element make_element_from_map(
  jmutils::string::Pool* pool,
  YAML::Node &node,
  const std::string& document,
  absl::flat_hash_set<std::string> &reference_to
);

Element make_element_from_sequence(
  jmutils::string::Pool* pool,
  YAML::Node &node,
  const std::string& document,
  absl::flat_hash_set<std::string> &reference_to
);

// Get logic

std::shared_ptr<merged_config_t> get_or_build_merged_config(
  document_t* document,
  const std::string& overrides_key
);

std::shared_ptr<merged_config_t> get_merged_config(
  document_t* document,
  const std::string& overrides_key
);

template <typename F>
inline void with_raw_config_locked(
  document_t* document,
  const std::string& override_path,
  VersionId version,
  F lambda
) {
  spdlog::trace(
    "Obtaining the raw config of the override path '{}' with version '{}'",
    override_path,
    version
  );

  auto o_search = document->override_by_path.find(override_path);
  if (o_search == document->override_by_path.end()) {
    spdlog::trace("Don't exists the override path '{}'", override_path);
    return;
  }

  auto rc_search = o_search->second.raw_config_by_version.upper_bound(version);
  if (rc_search == o_search->second.raw_config_by_version.begin()) {
    spdlog::trace("Don't exists a version lower or equal to {}", version);
  } else {
    --rc_search;
    if (rc_search->second == nullptr) {
      spdlog::trace(
        "The raw_config value is deleted for the version {}",
        rc_search->first
      );
    } else {
      spdlog::trace(
        "Obtained a raw config with the version {}",
        rc_search->first
      );

      lambda(
        static_cast<const decltype(override_path)&>(override_path),
        static_cast<decltype(rc_search->second)&>(rc_search->second)
      );
    }
  }
}

template <typename F>
inline bool for_each_document_override(
  document_t* document,
  const std::vector<std::string>& flavors,
  const std::vector<std::string>& overrides,
  VersionId version,
  F lambda
) {
  document->mutex.ReaderLock();
  bool is_a_valid_version = document->oldest_version <= version;
  if (is_a_valid_version) {
    for_each_document_override_path(
      flavors,
      overrides,
      [document, version, lambda](const auto& override_path) -> bool {
        with_raw_config_locked(
          document,
          override_path,
          version,
          lambda
        );
        return true;
      }
    );
  }
  document->mutex.ReaderUnlock();
  return is_a_valid_version;
}

template <typename F>
inline bool for_each_document_override_path(
  const std::vector<std::string>& flavors,
  const std::vector<std::string>& overrides,
  F lambda
) {
  std::string override_path;
  size_t flavors_l = flavors.size();
  size_t overrides_l = overrides.size();

  for (size_t override_idx = 0; override_idx < overrides_l; ++override_idx) {
    make_override_path(
      overrides[override_idx],
      "",
      override_path
    );
    if (!lambda(static_cast<const decltype(override_path)&>(override_path))) {
      return false;
    }
  }

  for (size_t flavor_idx = 0; flavor_idx < flavors_l; ++flavor_idx) {
    for (size_t override_idx = 0; override_idx < overrides_l; ++override_idx) {
      make_override_path(
        overrides[override_idx],
        flavors[flavor_idx],
        override_path
      );
      if (!lambda(static_cast<const decltype(override_path)&>(override_path))) {
        return false;
      }
    }
  }

  return true;
}

std::shared_ptr<document_t> get_document_locked(
  const config_namespace_t* cn,
  const std::string& name,
  VersionId version
);

inline std::shared_ptr<document_t> get_document(
  config_namespace_t* cn,
  const std::string& name,
  VersionId version
) {
  cn->mutex.ReaderLock();
  auto result = get_document_locked(cn, name, version);
  cn->mutex.ReaderUnlock();
  return result;
}

std::optional<DocumentId> next_document_id_locked(
  config_namespace_t* cn
);

bool try_insert_document_locked(
  config_namespace_t* cn,
  const std::string& name,
  VersionId version,
  std::shared_ptr<document_t>& document
);

inline document_versions_t* get_or_guild_document_versions_locked(
  config_namespace_t* cn,
  const std::string& name
) {
  auto inserted = cn->document_versions_by_name.try_emplace(name, nullptr);
  if (inserted.second) {
    inserted.first->second = std::make_unique<document_versions_t>();
  }
  return inserted.first->second.get();
}

inline std::shared_ptr<document_t> try_get_or_build_document_locked(
  config_namespace_t* cn,
  const std::string& name,
  VersionId version
) {
  auto document = get_document_locked(cn, name, version);

  if (document == nullptr) {
    document = std::make_shared<document_t>();
    if (!try_insert_document_locked(cn, name, version, document)) {
      document = nullptr;
    }
  }

  return document;
}

std::shared_ptr<document_t> try_get_or_build_document(
  config_namespace_t* cn,
  const std::string& name,
  VersionId version
);

inline raw_config_t* get_last_raw_config_locked(
  const override_t& override_
) {
  return override_.raw_config_by_version.empty()
    ? nullptr
    : override_.raw_config_by_version.crbegin()->second.get();
}

std::shared_ptr<document_t> try_migrate_document_locked(
  config_namespace_t* cn,
  document_t* document,
  VersionId version
);

inline bool is_document_full_locked(
  const document_t* document
) {
  return document->next_raw_config_id == 0xffff;
}

std::shared_ptr<document_t> try_obtain_non_full_document(
  config_namespace_t* cn,
  const std::string& name,
  VersionId version,
  size_t required_size = 1
);

struct split_filename_result_t {
  bool ok;
  std::string_view kind;
  std::string_view name;
  std::string_view flavor;
};

split_filename_result_t split_filename(
  std::string_view stem
);

std::array<uint8_t, 32> make_checksum(const Element& element);

namespace {
  struct trace_variables_counter_t {
    bool document : 1;
    uint16_t flavors : 15;
    uint16_t overrides;

    trace_variables_counter_t()
      : document(false),
      flavors(0),
      overrides(0)
    {
    }
  };
}

template <typename T>
std::shared_ptr<api::stream::TraceOutputMessage> make_trace_output_message(
  api::stream::TraceInputMessage* input_message,
  api::stream::TraceOutputMessage::Status status,
  uint64_t namespace_id,
  uint32_t version,
  T* message
) {
  auto output_message = input_message->make_output_message();

  output_message->set_status(status);
  output_message->set_namespace_id(namespace_id);
  output_message->set_version(version);
  output_message->set_overrides(message->overrides());
  output_message->set_flavors(message->flavors());
  output_message->set_document(message->document());
  output_message->set_peer(message->peer());

  return output_message;
}

template <typename T, typename F>
void for_each_trace_to_trigger(
  config_namespace_t* cn,
  const T* message,
  F lambda
) {
  absl::flat_hash_map<
    std::shared_ptr<api::stream::TraceInputMessage>,
    trace_variables_counter_t
  > match_by_trace;

  if (!cn->traces_by_override.empty()) {
    for (size_t i = 0, l = message->overrides().size(); i < l; ++i) {
      cn->traces_by_override.for_each(
        message->overrides()[i],
        [&match_by_trace](auto&& trace) { match_by_trace[trace].overrides += 1; }
      );
    }
  }

  if (!cn->traces_by_flavor.empty()) {
    for (size_t i = 0, l = message->flavors().size(); i < l; ++i) {
      cn->traces_by_flavor.for_each(
        message->flavors()[i],
        [&match_by_trace](auto&& trace) { match_by_trace[trace].flavors += 1; }
      );
    }
  }

  if (!message->document().empty()){
    cn->traces_by_document.for_each(
      message->document(),
      [&match_by_trace](auto&& trace) { match_by_trace[trace].document = true; }
    );
  }

  cn->to_trace_always.for_each(
    [lambda](auto&& trace) {
      lambda(trace.get());
    }
  );

  for (auto& it : match_by_trace) {
    bool trigger_trace = true;

    if (!it.first->overrides().empty()) {
      trigger_trace = it.first->overrides().size() == it.second.overrides;
    }
    if (trigger_trace && !it.first->flavors().empty()) {
      trigger_trace = it.first->flavors().size() == it.second.flavors;
    }
    if (trigger_trace && !it.first->document().empty()) {
      trigger_trace = it.second.document;
    }

    if (trigger_trace) {
      lambda(it.first.get());
    }
  }
}

inline std::shared_ptr<config_namespace_t> get_cn_locked(
  context_t* ctx,
  const std::string& root_path
) {
  if (
    auto search = ctx->cn_by_root_path.find(root_path);
    search != ctx->cn_by_root_path.end()
  ) {
    return search->second;
  }

  return nullptr;
}

std::shared_ptr<config_namespace_t> get_cn(
  context_t* ctx,
  const std::string& root_path
);

std::shared_ptr<config_namespace_t> get_or_build_cn(
  context_t* ctx,
  const std::string& root_path
);

inline VersionId get_version(
  const config_namespace_t* cn,
  VersionId version
) {
  if (version == 0) return cn->current_version;
  return ((version < cn->oldest_version) || (cn->current_version < version))
    ? 0
    : version;
}

enum CheckMergedConfigResult {
  IN_PROGRESS,
  NEED_EXCLUSIVE_LOCK,
  BUILD_CONFIG,
  OPTIMIZE_CONFIG,
  COMMIT_MESSAGE,
  ERROR
};

template <bool has_exclusive_lock>
CheckMergedConfigResult check_merged_config(
  merged_config_t* merged_config,
  std::shared_ptr<api::request::GetRequest>& request
) {
  switch (merged_config->status) {
    case MergedConfigStatus::UNDEFINED:
      if (!has_exclusive_lock) {
        return CheckMergedConfigResult::NEED_EXCLUSIVE_LOCK;
      }
      merged_config->waiting.push_back(request);
      merged_config->status = MergedConfigStatus::BUILDING;
      return CheckMergedConfigResult::BUILD_CONFIG;

    case MergedConfigStatus::BUILDING: // Fallback
    case MergedConfigStatus::OK_CONFIG_OPTIMIZING:
      if (!has_exclusive_lock) {
        return CheckMergedConfigResult::NEED_EXCLUSIVE_LOCK;
      }
      merged_config->waiting.push_back(std::move(request));
      return CheckMergedConfigResult::IN_PROGRESS;

    case MergedConfigStatus::OK_CONFIG_NO_OPTIMIZED:
      if (!has_exclusive_lock) {
        return CheckMergedConfigResult::NEED_EXCLUSIVE_LOCK;
      }
      merged_config->status = MergedConfigStatus::OK_CONFIG_OPTIMIZING;
      merged_config->waiting.push_back(std::move(request));
      return CheckMergedConfigResult::OPTIMIZE_CONFIG;

    case MergedConfigStatus::OK_CONFIG_OPTIMIZED:
      spdlog::debug("The built document '{}' has been found", request->document());

      request->set_preprocessed_payload(
        merged_config->preprocesed_value.data(),
        merged_config->preprocesed_value.size()
      );

      return CheckMergedConfigResult::COMMIT_MESSAGE;

    case MergedConfigStatus::REF_GRAPH_IS_NOT_DAG:
      spdlog::debug("The built document '{}' has been found but it isn't a DAG", request->document());

      request->set_status(api::request::GetRequest::Status::REF_GRAPH_IS_NOT_DAG);

      return CheckMergedConfigResult::COMMIT_MESSAGE;
  }

  return CheckMergedConfigResult::ERROR;
}

void delete_cn_locked(
  config_namespace_t* cn
);

inline void delete_cn(
  config_namespace_t* cn
) {
  cn->mutex.Lock();
  delete_cn_locked(cn);
  cn->mutex.Unlock();
}

void remove_cn_locked(
  context_t* ctx,
  const std::string& root_path,
  uint64_t id
);

inline void remove_cn(
  context_t* ctx,
  const std::string& root_path,
  uint64_t id
) {
  ctx->mutex.Lock();
  remove_cn_locked(ctx, root_path, id);
  ctx->mutex.Lock();
}

} /* mhconfig */

#endif
