#ifndef MHCONFIG__BUILDER_H
#define MHCONFIG__BUILDER_H

#include <absl/container/flat_hash_set.h>
#include <spdlog/fmt/fmt.h>
#include <stddef.h>
#include <algorithm>
#include <array>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>
#include <openssl/sha.h>

#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "jmutils/base64.h"
#include "jmutils/common.h"
#include "jmutils/container/label_set.h"
#include "jmutils/container/weak_multimap.h"
#include "jmutils/cow.h"
#include "jmutils/time.h"
#include "mhconfig/api/common.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/stream/trace_stream.h"
#include "mhconfig/auth/policy.h"
#include "mhconfig/auth/tokens.h"
#include "mhconfig/config_namespace.h"
#include "mhconfig/context.h"
#include "mhconfig/element.h"
#include "mhconfig/string_pool.h"
#include "mhconfig/validator.h"
#include "spdlog/spdlog.h"
#include "yaml-cpp/exceptions.h"
#include "mhconfig/logger/spdlog_logger.h"
#include "mhconfig/logger/bi_logger.h"
#include "mhconfig/element_builder.h"

namespace mhconfig
{

using jmutils::container::Labels;
using jmutils::container::label_t;

enum class LoadRawConfigStatus {
    OK,
    INVALID_FILENAME,
    FILE_DONT_EXISTS,
    ERROR
};

struct load_raw_config_result_t {
    LoadRawConfigStatus status;
    std::string document;
    std::shared_ptr<raw_config_t> raw_config{nullptr};
};

bool init_config_namespace(
    config_namespace_t* cn
);

std::optional<std::string> read_file(
    const std::filesystem::path& path
);

template <typename T>
void load_raw_config(
    const std::filesystem::path& root_path,
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

        auto data = read_file(path);
        if (!data) return;

        result.raw_config = std::make_shared<raw_config_t>();
        result.raw_config->has_content = true;
        result.raw_config->logger = std::make_shared<PersistentLogger>();
        result.raw_config->path = std::filesystem::relative(path, root_path).string();

        auto& logger = *result.raw_config->logger;

        lambda(logger, *data, result);

        {
            std::array<uint8_t, 32> checksum;

            SHA256_CTX sha256;
            SHA256_Init(&sha256);
            SHA256_Update(&sha256, data->data(), data->size());
            SHA256_Final(checksum.data(), &sha256);

            result.raw_config->file_checksum = 0;
            for (size_t i = 0; i < 4; ++i) {
                result.raw_config->file_checksum <<= 8;
                result.raw_config->file_checksum |= checksum[i];
            }
        }

        {
            auto checksum = result.raw_config->value.make_checksum();

            result.raw_config->checksum = 0;
            for (size_t i = 0; i < 8; ++i) {
                result.raw_config->checksum <<= 8;
                result.raw_config->checksum |= checksum[i];
            }
        }

        result.status = LoadRawConfigStatus::OK;
    } catch(const std::exception &e) {
        spdlog::error(
            "Error making the element (path: '{}'): {}",
            path.string(),
            e.what()
        );
    } catch(...) {
        spdlog::error(
            "Unknown error making the element (path: '{}')",
            path.string()
        );
    }

    if (result.raw_config->logger != nullptr) {
        result.raw_config->logger->freeze();
    }
}

load_raw_config_result_t index_file(
    jmutils::string::Pool* pool,
    const std::filesystem::path& root_path,
    const std::filesystem::path& path
);

std::optional<Labels> get_path_labels(
    const std::filesystem::path& path
);

template <typename T>
bool index_files(
    jmutils::string::Pool* pool,
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
        if (it->path().filename().native()[0] == '.') {
            it.disable_recursion_pending();
        } else if (it->is_regular_file()) {
            auto result = index_file(pool, root_path, it->path());
            if (result.status != LoadRawConfigStatus::INVALID_FILENAME) {
                auto relative_path = std::filesystem::relative(it->path(), root_path)
                    .parent_path();

                auto labels = get_path_labels(relative_path);

                bool ok = labels.has_value();
                if (ok) {
                    ok = lambda(std::move(labels.value()), std::move(result));
                }

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

absl::flat_hash_map<std::string, absl::flat_hash_map<Labels, AffectedDocumentStatus>> get_dep_by_doc(
    config_namespace_t* cn,
    absl::flat_hash_map<Labels, absl::flat_hash_map<std::string, AffectedDocumentStatus>>& updated_documents_by_path
);

void fill_affected_documents(
    config_namespace_t* cn,
    absl::flat_hash_map<std::string, AffectedDocumentStatus>& affected_documents
);

bool touch_affected_documents(
    config_namespace_t* cn,
    VersionId version,
    const absl::flat_hash_map<std::string, absl::flat_hash_map<Labels, AffectedDocumentStatus>>& dep_by_doc,
    bool only_nonexistent
);

// Get logic

bool dummy_payload_alloc(Element& element, void*& payload);
void dummy_payload_dealloc(void* payload);

bool mhc_tokens_payload_alloc(Element& element, void*& payload);
void mhc_tokens_payload_dealloc(void* payload);

bool mhc_policy_payload_alloc(Element& element, void*& payload);
void mhc_policy_payload_dealloc(void* payload);

inline std::shared_ptr<config_namespace_t> get_cn_locked(
    context_t* ctx,
    const std::string& root_path
) {
    if (
        auto search = ctx->cn_by_root_path.find(root_path);
        search != ctx->cn_by_root_path.end()
    ) {
        search->second->last_access_timestamp = jmutils::monotonic_now_sec();
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

std::shared_ptr<merged_config_t> get_or_build_merged_config(
    document_t* document,
    const std::string& overrides_key
);

std::shared_ptr<merged_config_t> get_merged_config(
    document_t* document,
    const std::string& overrides_key
);

inline std::shared_ptr<raw_config_t> get_raw_config_locked(
    override_t* override_,
    VersionId version
) {
    auto search = override_->raw_config_by_version.upper_bound(version);
    return search == override_->raw_config_by_version.begin()
        ? nullptr
        : (--search)->second;
}

inline std::shared_ptr<raw_config_t> get_raw_config_locked(
    document_t* document,
    const Labels& labels,
    VersionId version
) {
    auto override_ = document->lbl_set.get(labels);
    return override_ == nullptr
        ? nullptr
        : get_raw_config_locked(override_, version);
}

inline Element get_element(
    document_t* document,
    const Labels& labels,
    VersionId version
) {
    document->mutex.ReaderLock();
    auto rc = get_raw_config_locked(document, labels, version);
    auto r = rc == nullptr ? UNDEFINED_ELEMENT : rc->value;
    document->mutex.ReaderUnlock();
    return r;
}

template <typename F>
bool for_each_document_override(
    const Element& config,
    document_t* document,
    const Labels& labels,
    VersionId version,
    F lambda
) {
    document->mutex.ReaderLock();
    bool is_a_valid_version = document->oldest_version <= version;
    if (is_a_valid_version) {
        //TODO Review solution and performance
        std::vector<std::pair<std::vector<uint32_t>, override_t*>> overrides;

        spdlog::debug(
            "Obtaining overrides subset of the labels {} for the document '{}'",
            labels,
            document->name
        );
        auto labels_metadata = config.get("labels_metadata");
        is_a_valid_version = document->lbl_set.for_each_subset(
            labels,
            [&labels_metadata, &overrides](const auto& labels, auto* override_) -> bool {
                spdlog::trace(
                    "Obtained unordered override {} with labels {}",
                    (void*)override_,
                    labels
                );
                std::vector<uint32_t> weights;
                for (const auto& label : labels) {
                    auto meta = labels_metadata.get(label.first);
                    auto weight = meta.get("weight");
                    if (auto r = weight.template try_as<int64_t>(); r) {
                        weights.push_back(*r);
                    } else {
                        spdlog::error("Can't obtain the weight of the label '{}'", label.first);
                        return false;
                    }
                }
                std::sort(weights.begin(), weights.end());
                overrides.emplace_back(std::move(weights), override_);
                return true;
            }
        );

        if (is_a_valid_version) {
            std::sort(
                overrides.begin(),
                overrides.end(),
                [](const auto& lhs, const auto& rhs) {
                    return lhs.first.size() == rhs.first.size()
                        ? lhs.first < rhs.first
                        : lhs.first.size() < rhs.first.size();
                }
            );

            for (const auto& it : overrides) {
                spdlog::trace("Using override {}", (void*)it.second);
                auto rc = get_raw_config_locked(it.second, version);
                if (rc != nullptr) {
                    lambda(std::move(rc));
                }
            }
        }
    }
    document->mutex.ReaderUnlock();
    return is_a_valid_version;
}

bool alloc_payload_locked(
    merged_config_t* merged_config
);

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

inline document_versions_t* get_or_build_document_versions_locked(
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

        if (
            auto search = cn->mc_payload_fun_by_document.find(name);
            search != cn->mc_payload_fun_by_document.end()
        ) {
            document->mc_payload_fun = search->second;
        } else {
            //TODO Add some custom default allocator
            document->mc_payload_fun.alloc = dummy_payload_alloc;
            document->mc_payload_fun.dealloc = dummy_payload_dealloc;
        }

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
    const override_t* override_
) {
    if (override_->raw_config_by_version.empty()) {
        return nullptr;
    }
    auto last = override_->raw_config_by_version.crbegin()->second.get();
    if (last == nullptr) {
        return nullptr;
    }
    return last->has_content ? last : nullptr;
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
};

split_filename_result_t split_filename(
    std::string_view stem
);

namespace {
    struct trace_variables_counter_t {
        bool document : 1;
        uint16_t labels : 15;

        trace_variables_counter_t()
            : document(false),
            labels(0)
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
    output_message->set_labels(message->labels());
    output_message->set_document(message->document());

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

    if (!cn->traces_by_label.empty()) {
        for (const auto& label: message->labels()) {
            cn->traces_by_label.for_each(
                label,
                [&match_by_trace](auto&& trace) { match_by_trace[trace].labels += 1; }
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

        if (!it.first->labels().empty()) {
            trigger_trace = it.first->labels().size() == it.second.labels;
        }
        if (trigger_trace && !it.first->document().empty()) {
            trigger_trace = it.second.document;
        }

        if (trigger_trace) {
            lambda(it.first.get());
        }
    }
}

inline VersionId get_version(
    const config_namespace_t* cn,
    VersionId version
) {
    if (version == 0) return cn->current_version;
    return ((version < cn->oldest_version) || (cn->current_version < version))
        ? 0
        : version;
}

std::vector<api::source_t> make_sources(
    const api::SourceIds& source_ids,
    config_namespace_t* cn
);

enum CheckMergedConfigResult {
    IN_PROGRESS,
    NEED_EXCLUSIVE_LOCK,
    BUILD_CONFIG,
    OPTIMIZE_CONFIG,
    OK,
    ERROR
};

CheckMergedConfigResult check_merged_config(
    merged_config_t* merged_config,
    std::shared_ptr<GetConfigTask>& task,
    bool has_exclusive_lock
);

void delete_cn_locked(
    std::shared_ptr<config_namespace_t>& cn
);

inline void delete_cn(
    std::shared_ptr<config_namespace_t>& cn
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
    ctx->mutex.Unlock();
}

} /* mhconfig */

#endif
