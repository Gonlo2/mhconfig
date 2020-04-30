#ifndef MHCONFIG__SCHEDULER__COMMAND__API_GET_COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__API_GET_COMMAND_H

#include <memory>

#include "mhconfig/builder.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/worker/command/build_command.h"
#include "mhconfig/worker/command/api_reply_command.h"
#include "mhconfig/worker/command/api_get_reply_command.h"
#include "mhconfig/scheduler/command/command.h"
#include "jmutils/time.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

using namespace ::mhconfig::builder;

class ApiGetCommand : public Command
{
public:
  ApiGetCommand(
    ::mhconfig::api::request::GetRequest* get_request
  ) : Command(),
      get_request_(get_request)
  {
  }

  virtual ~ApiGetCommand() {
  }

  std::string name() const override {
    return "API_GET";
  }

  CommandType command_type() const override {
    return CommandType::GET_NAMESPACE_BY_PATH;
  }

  const std::string& namespace_path() const override {
    return get_request_->root_path();
  }

  NamespaceExecutionResult execute_on_namespace(
    std::shared_ptr<config_namespace_t> config_namespace,
    Queue<worker::command::CommandRef>& worker_queue
  ) override {
    // First we set the basic request fields
    get_request_->set_version(
      get_specific_version(config_namespace, get_request_->version())
    );
    get_request_->set_namespace_id(config_namespace->id);

    // We check if exists the asked document
    auto search = config_namespace->document_metadata_by_document
      .find(get_request_->key()[0]);

    if (search == config_namespace->document_metadata_by_document.end()) {
      spdlog::warn(
        "Can't found a config file with the name '{}'",
        get_request_->key()[0]
      );

      get_request_->set_element(UNDEFINED_ELEMENT);
      send_api_response(worker_queue);
      return NamespaceExecutionResult::OK;
    }

    // If the document exists and the user asked for a version
    // we check if the version is available
    auto document_metadata = search->second;
    if (
        (get_request_->version() != 0)
        && !config_namespace->stored_versions_by_deprecation_timestamp.empty()
        && (get_request_->version() < config_namespace->stored_versions_by_deprecation_timestamp.front().second)
    ) {
      spdlog::trace("The asked version {} don't exists", get_request_->version());

      get_request_->set_element(UNDEFINED_ELEMENT);
      send_api_response(worker_queue);
      return NamespaceExecutionResult::OK;
    }

    // If we are here it's possible obtain the asked document so first of all we check
    // if exists a cached value of the document with the requested overrides.
    // To search it we create the overrides key
    std::string overrides_key;
    make_overrides_key(
      document_metadata,
      get_request_->overrides(),
      get_request_->version(),
      overrides_key
    );
    auto merged_config = ::mhconfig::builder::get_merged_config(
      config_namespace,
      get_request_->key()[0],
      overrides_key
    );

    if ((merged_config != nullptr) && (merged_config->status == MergedConfigStatus::OK)) {
      spdlog::debug("The built document '{}' has been found", get_request_->key()[0]);

      merged_config->last_access_timestamp = jmutils::time::monotonic_now_sec();

      send_api_get_response(worker_queue, merged_config->api_merged_config);
      return NamespaceExecutionResult::OK;
    }

    ///////////////////////////// Check after this

    spdlog::debug("Preparing build for document '{}'", get_request_->key()[0]);
    return prepare_build_request(
      config_namespace,
      worker_queue
    );
  }

  void send_api_response(
    Queue<worker::command::CommandRef>& worker_queue
  ) {
    auto api_reply_command = std::make_shared<::mhconfig::worker::command::ApiReplyCommand>(
      static_cast<::mhconfig::api::request::Request*>(get_request_)
    );
    worker_queue.push(api_reply_command);
  }

  void send_api_get_response(
    Queue<worker::command::CommandRef>& worker_queue,
    std::shared_ptr<mhconfig::api::config::MergedConfig> api_merged_config
  ) {
    auto api_get_reply_command = std::make_shared<::mhconfig::worker::command::ApiGetReplyCommand>(
      get_request_,
      api_merged_config
    );
    worker_queue.push(api_get_reply_command);
  }

  NamespaceExecutionResult prepare_build_request(
    std::shared_ptr<config_namespace_t> config_namespace,
    Queue<worker::command::CommandRef>& worker_queue
  ) {
    // The references are allowed only if the graph is a DAG,
    // so we check it here
    auto is_a_dag_result = check_if_ref_graph_is_a_dag(
      config_namespace,
      get_request_->key()[0],
      get_request_->overrides(),
      get_request_->version()
    );
    if (!is_a_dag_result.first) {
      return NamespaceExecutionResult::ERROR;
    }

    // If the graph is a DAG we could do a topological sort
    // of the required documents
    auto documents_in_order = do_topological_sort_over_ref_graph(
      is_a_dag_result.second
    );

    auto wait_built = std::make_shared<build::wait_built_t>();

    wait_built->is_main = documents_in_order.size() == 1;
    wait_built->request = get_request_;
    wait_built->elements_to_build.resize(documents_in_order.size());
    wait_built->specific_version = get_specific_version(
      config_namespace,
      get_request_->version()
    );

    for (size_t i = 0; i < documents_in_order.size(); ++i) {
      auto& build_element = wait_built->elements_to_build[i];
      build_element.name = documents_in_order[i];
      spdlog::debug("Checking the document '{}'", build_element.name);

      auto document_metadata = config_namespace
        ->document_metadata_by_document[build_element.name];

      make_overrides_key(
        document_metadata,
        get_request_->overrides(),
        get_request_->version(),
        build_element.overrides_key
      );
      auto merged_config = ::mhconfig::builder::get_or_build_merged_config(
        config_namespace,
        build_element.name,
        build_element.overrides_key
      );
      switch (merged_config->status) {
        case MergedConfigStatus::UNDEFINED: {
          spdlog::debug(
            "The document '{}' is undefined, preparing the building",
            build_element.name
          );
          for (const auto& k : get_request_->overrides()) {
            auto raw_config = get_raw_config(
              document_metadata,
              k,
              get_request_->version()
            );
            if (raw_config != nullptr) {
              build_element.raw_config_by_override[k] = raw_config;
            }
          }
          merged_config->status = MergedConfigStatus::BUILDING;
          break;
        }

        case MergedConfigStatus::BUILDING: {
          spdlog::debug(
            "The document '{}' is building, waiting for it",
            build_element.name
          );
          wait_built->pending_element_position_by_name[build_element.name] = i;

          std::stringstream wait_built_key_ss;
          wait_built_key_ss << build_element.name << ':'
                            << build_element.overrides_key; //TODO change the delimiter

          config_namespace->wait_builts_by_key[wait_built_key_ss.str()].push_back(
            wait_built
          );
          break;
        }

        case MergedConfigStatus::OK: {
          spdlog::debug("The document '{}' is ok", build_element.name);
          build_element.config = merged_config->value;
          break;
        }
      }
    }

    if (wait_built->pending_element_position_by_name.empty()) {
      spdlog::debug(
        "Sending the get request with id {} to built",
        (uint64_t) wait_built->request
      );
      auto build_command = std::make_shared<::mhconfig::worker::command::BuildCommand>(
        config_namespace->id,
        config_namespace->pool,
        wait_built
      );
      worker_queue.push(build_command);
    } else {
      spdlog::debug(
        "The document '{}' need wait to {} building documents",
        get_request_->key()[0],
        wait_built->pending_element_position_by_name.size()
      );
    }

    return NamespaceExecutionResult::OK;
  }

  std::pair<bool, std::unordered_map<std::string, std::unordered_set<std::string>>> check_if_ref_graph_is_a_dag(
    const std::shared_ptr<config_namespace_t> config_namespace,
    const std::string& document,
    const std::vector<std::string> overrides,
    uint32_t version
  ) {
    std::pair<bool, std::unordered_map<std::string, std::unordered_set<std::string>>> result;

    std::vector<std::string> dfs_path;
    std::unordered_set<std::string> dfs_path_set;
    result.first = check_if_ref_graph_is_a_dag_rec(
      config_namespace,
      document,
      overrides,
      version,
      dfs_path,
      dfs_path_set,
      result.second
    );

    result.second[document] = std::unordered_set<std::string>();

    return result;
  }

  bool check_if_ref_graph_is_a_dag_rec(
    const std::shared_ptr<config_namespace_t> config_namespace,
    const std::string& document,
    const std::vector<std::string> overrides,
    uint32_t version,
    std::vector<std::string>& dfs_path,
    std::unordered_set<std::string>& dfs_path_set,
    std::unordered_map<std::string, std::unordered_set<std::string>>& referenced_documents
  ) {
    if (dfs_path_set.count(document)) {
      std::stringstream path_ss;
      path_ss << "'" << dfs_path[0] << "'";
      for (size_t i = 1; i < dfs_path.size(); ++i) {
        path_ss << ", '" << dfs_path[i] << "'";
      }

      spdlog::error(
        "The config dependencies defined with the '!ref' tag has a cycle with the document path [{}]",
        path_ss.str()
      );
      return false;
    }

    if (referenced_documents.count(document)) return true;

    auto document_metadata_search = config_namespace->document_metadata_by_document.find(
      document
    );
    if (document_metadata_search == config_namespace->document_metadata_by_document.end()) {
      spdlog::warn("Can't found a config file with the name '{}'", document);
      return false;
    }

    auto document_metadata = document_metadata_search->second;
    std::string overrides_key;
    make_overrides_key(
      document_metadata,
      overrides,
      version,
      overrides_key
    );
    auto config = ::mhconfig::builder::get_merged_config(
      config_namespace,
      document,
      overrides_key
    );
    if (config != nullptr) {
      return true;
    }

    dfs_path.push_back(document);
    dfs_path_set.insert(document);

    for (uint32_t i = 0; i < overrides.size(); ++i) {
      auto raw_config = get_raw_config(document_metadata, overrides[i], version);
      if (raw_config != nullptr) {
        for (const auto& ref_document : raw_config->reference_to) {
          referenced_documents[ref_document].insert(document);
          bool is_a_dag = check_if_ref_graph_is_a_dag_rec(
            config_namespace,
            ref_document,
            overrides,
            version,
            dfs_path,
            dfs_path_set,
            referenced_documents
          );
          if (!is_a_dag) return false;
        }
      }
    }

    dfs_path.pop_back();
    dfs_path_set.erase(document);

    return true;
  }

  std::vector<std::string> do_topological_sort_over_ref_graph(
    const std::unordered_map<std::string, std::unordered_set<std::string>>& referenced_documents
  ) {
    std::vector<std::string> reversed_topological_sort;
    std::unordered_set<std::string> visited_documents;

    for (const auto& it : referenced_documents) {
      do_topological_sort_over_ref_graph_rec(
        it.first,
        referenced_documents,
        visited_documents,
        reversed_topological_sort
      );
    }

    std::reverse(reversed_topological_sort.begin(), reversed_topological_sort.end());

    return reversed_topological_sort;
  }

  void do_topological_sort_over_ref_graph_rec(
    const std::string& document,
    const std::unordered_map<std::string, std::unordered_set<std::string>>& referenced_documents,
    std::unordered_set<std::string>& visited_documents,
    std::vector<std::string>& inverted_topological_sort
  ) {
    if (visited_documents.count(document)) return;
    visited_documents.insert(document);

    for (const auto& ref_document : referenced_documents.at(document)) {
      do_topological_sort_over_ref_graph_rec(
        ref_document,
        referenced_documents,
        visited_documents,
        inverted_topological_sort
      );
    }

    inverted_topological_sort.push_back(document);
  }

  // Help functions
  inline void make_overrides_key(
    const std::shared_ptr<document_metadata_t> document_metadata,
    const std::vector<std::string>& overrides,
    uint32_t version,
    std::string& overrides_key
  ) {
    union converter {
      char c[4];
      uint32_t n;
    };
    converter k;

    overrides_key.clear();
    overrides_key.reserve(overrides.size()*4);
    for (auto& override_: overrides) {
      auto raw_config = get_raw_config(document_metadata, override_, version);
      if (raw_config != nullptr) {
        k.n = raw_config->id;
        overrides_key.push_back(k.c[0]);
        overrides_key.push_back(k.c[1]);
        overrides_key.push_back(k.c[2]);
        overrides_key.push_back(k.c[3]);
      }
    }
  }

  inline uint32_t get_specific_version(
    const std::shared_ptr<config_namespace_t> config_namespace,
    uint32_t version
  ) {
    return (version == 0) ? config_namespace->current_version : version;
  }

  bool on_get_namespace_error(
    Queue<worker::command::CommandRef>& worker_queue
  ) override {
    //TODO
    return false;
  }

private:
  ::mhconfig::api::request::GetRequest* get_request_;
};

} /* command */
} /* scheduler */
} /* mhconfig */

#endif
