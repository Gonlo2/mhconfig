#ifndef MHCONFIG__SCHEDULER__COMMAND__API_GET_COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__API_GET_COMMAND_H

#include <memory>

#include "mhconfig/worker/command/build_command.h"
#include "mhconfig/scheduler/command/command.h"
#include "jmutils/time.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

template <typename T>
class ApiGetCommand : public Command
{
public:
  ApiGetCommand(
    T get_request
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

  bool execute_on_namespace(
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
      return send_api_response();
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
      return send_api_response();
    }

    // If we are here it's possible obtain the asked document so first of all we check
    // if exists a cached value of the document with the requested overrides.
    // To search it we create the overrides key
    std::string overrides_key = make_overrides_key(
      document_metadata,
      get_request_->overrides(),
      get_request_->version()
    );
    auto merged_config = get_merged_config(
      config_namespace,
      get_request_->key()[0],
      overrides_key
    );

    if ((merged_config != nullptr) && (merged_config->status == MergedConfigStatus::OK)) {
      spdlog::debug("The built document '{}' has been found", get_request_->key()[0]);

      merged_config->last_access_timestamp = jmutils::time::monotonic_now_sec();

      //return send_api_response(merged_config->api_merged_config);
      return false;
    }

    ///////////////////////////// Check after this

    spdlog::debug("Preparing build for document '{}'", get_request_->key()[0]);
    return prepare_build_request(
      config_namespace,
      worker_queue
    );
  }

  std::shared_ptr<merged_config_t> get_merged_config(
    std::shared_ptr<config_namespace_t> config_namespace,
    const std::string& document,
    const std::string& overrides_key
  ) {
    std::shared_ptr<merged_config_metadata_t> merged_config_metadata = nullptr;
    // First we search if exists cached some mergd config using the overrides_key
    {
      auto search = config_namespace
        ->merged_config_metadata_by_overrides_key
        .find(overrides_key);

      if (search == config_namespace->merged_config_metadata_by_overrides_key.end()) {
        return nullptr;
      }

      merged_config_metadata = search->second;
      assert(merged_config_metadata != nullptr);
    }

    // If the override is cached we search the document
    auto search = merged_config_metadata
      ->merged_config_by_document
      .find(document);

    if (search == merged_config_metadata->merged_config_by_document.end()) {
      return nullptr;
    }

    // We use a weak pointer to free the merged config so it's
    // possible that the obtained pointer is empty
    if (auto merged_config = search->second.lock()) {
      return merged_config;
    }

    // If the pointer is invalidated we drop the item to avoid
    // do this check in a future, I'm to lazy ;)
    merged_config_metadata->merged_config_by_document.erase(search);

    return nullptr;
  }

  std::shared_ptr<merged_config_t> get_or_build_merged_config(
    std::shared_ptr<config_namespace_t> config_namespace,
    const std::string& document,
    const std::string& overrides_key
  ) {
    auto merged_config = get_merged_config(
      config_namespace,
      document,
      overrides_key
    );
    if (merged_config == nullptr) {
      std::shared_ptr<merged_config_metadata_t> merged_config_metadata = nullptr;
      {
        auto search = config_namespace->merged_config_metadata_by_overrides_key.find(
          overrides_key
        );
        if (search == config_namespace->merged_config_metadata_by_overrides_key.end()) {
          merged_config_metadata = std::make_shared<merged_config_metadata_t>();
          config_namespace->merged_config_metadata_by_overrides_key[overrides_key] = merged_config_metadata;
        } else {
          merged_config_metadata = search->second;
        }
      }

      merged_config = std::make_shared<merged_config_t>();
      merged_config_metadata->merged_config_by_document[document] = merged_config;
      config_namespace->merged_config_by_gc_generation[0].push_back(merged_config);
    }

    return merged_config;
  }

  bool send_api_response(
    //std::shared_ptr<mhconfig::api::config::MergedConfig> api_merged_config
  ) {
//
//    command::command_t command;
//    command.type = command::CommandType::API_GET;
//    command.api_request = get_request;
//    command.api_merged_config = api_merged_config;
//
//    worker_queue_.push(command);
//
    return true;
  }

  //bool process_command_type_update_response(
    //const std::shared_ptr<command::update::response_t> update_response,
    //std::shared_ptr<config_namespace_t> config_namespace
  //) {
//    auto update_api_request = (update_request::UpdateRequest*) update_response->api_request;
//    update_api_request->set_namespace_id(config_namespace->id);
//
//    if (update_response->status != command::update::ResponseStatus::OK) {
//      switch (update_response->status) {
//        case command::update::ResponseStatus::OK:
//        case command::update::ResponseStatus::ERROR:
//          update_api_request->set_status(update_request::Status::ERROR);
//      }
//      return send_api_response(update_api_request);
//    }
//
//    spdlog::debug(
//      "Increasing the current version {} of the namespace with ID {}",
//      config_namespace->current_version,
//      config_namespace->id
//    );
//    ++(config_namespace->current_version);
//
//    uint64_t timestamp = std::chrono::duration_cast<std::chrono::seconds>(
//      std::chrono::system_clock::now().time_since_epoch()
//    ).count();
//
//    config_namespace->stored_versions_by_deprecation_timestamp.back().first = timestamp;
//    config_namespace->stored_versions_by_deprecation_timestamp.emplace_back(
//      0,
//      config_namespace->current_version
//    );
//
//
//    spdlog::debug("Decreasing the referenced_by counter of the previous raw config");
//    for (auto& item : update_response->items) {
//      auto document_metadata_search = config_namespace->document_metadata_by_document
//        .find(item.document);
//
//      if (document_metadata_search != config_namespace->document_metadata_by_document.end()) {
//        auto raw_config = get_raw_config(
//          document_metadata_search->second,
//          item.override_,
//          0
//        );
//
//        if (raw_config != nullptr) {
//          for (const auto& reference_to : raw_config->reference_to) {
//            auto& referenced_by = config_namespace
//              ->document_metadata_by_document[reference_to]
//              ->referenced_by;
//
//            spdlog::debug(
//              "Decreasing referenced_by counter (document: '{}', reference_to: '{}', counter: {}, override: '{}')",
//              item.document,
//              reference_to,
//              referenced_by[item.document].v,
//              item.override_
//            );
//
//            if (referenced_by[item.document].v-- <= 1) {
//              referenced_by.erase(item.document);
//            }
//          }
//        }
//      }
//    }
//
//    spdlog::debug("Updating the id of the affected documents");
//    std::unordered_map<std::string, std::unordered_set<std::string>> updated_documents_by_override;
//    for (auto& item : update_response->items) {
//      updated_documents_by_override[item.override_].insert(item.document);
//    }
//
//    for (auto& updated_documents_it : updated_documents_by_override) {
//      std::unordered_set<std::string> affected_documents;
//      for (const auto& document : updated_documents_it.second) {
//        get_affected_documents(config_namespace, document, affected_documents);
//      }
//      for (const auto& document : updated_documents_it.second) {
//        affected_documents.erase(document);
//      }
//
//      for (const auto document : affected_documents) {
//        auto document_metadata = config_namespace
//          ->document_metadata_by_document[document];
//
//        auto raw_config = get_raw_config(
//          document_metadata,
//          updated_documents_it.first,
//          0
//        );
//
//        if (raw_config != nullptr) {
//          spdlog::debug(
//            "Updating affected raw config id (document: '{}', override: '{}', old_id: {}, new_id: {})",
//            document,
//            updated_documents_it.first,
//            raw_config->id,
//            config_namespace->next_raw_config_id
//          );
//
//          auto new_raw_config = std::make_shared<raw_config_t>();
//          new_raw_config->id = config_namespace->next_raw_config_id++;
//          new_raw_config->value = raw_config->value;
//          new_raw_config->reference_to = raw_config->reference_to;
//
//          document_metadata->raw_config_by_version_by_override[updated_documents_it.first][config_namespace->current_version] = new_raw_config;
//        }
//      }
//    }
//
//    spdlog::debug("Updating the elements of the updated documents");
//    for (auto& item : update_response->items) {
//      auto document_metadata_search = config_namespace->document_metadata_by_document
//        .find(item.document);
//
//      if (item.raw_config == nullptr) {
//        spdlog::debug(
//          "Removing a raw config (document: '{}', override: '{}')",
//          item.document,
//          item.override_
//        );
//        if (document_metadata_search != config_namespace->document_metadata_by_document.end()) {
//          document_metadata_search->second->raw_config_by_version_by_override[item.override_][config_namespace->current_version] = std::make_shared<raw_config_t>();
//        }
//      } else {
//        for (const auto& reference_to : item.raw_config->reference_to) {
//          std::shared_ptr<document_metadata_t> referenced_document_metadata;
//          {
//            auto search = config_namespace->document_metadata_by_document
//              .find(reference_to);
//            if (search == config_namespace->document_metadata_by_document.end()) {
//              referenced_document_metadata = std::make_shared<document_metadata_t>();
//              config_namespace
//                ->document_metadata_by_document[reference_to] = referenced_document_metadata;
//            } else {
//              referenced_document_metadata = search->second;
//            }
//          }
//
//          spdlog::debug(
//            "Increasing referenced_by counter (document: '{}', reference_to: '{}', counter: {}, override: '{}')",
//            item.document,
//            reference_to,
//            referenced_document_metadata->referenced_by[item.document].v,
//            item.override_
//          );
//
//          referenced_document_metadata->referenced_by[item.document].v += 1;
//        }
//
//        std::shared_ptr<document_metadata_t> document_metadata;
//        if (document_metadata_search == config_namespace->document_metadata_by_document.end()) {
//          document_metadata = std::make_shared<document_metadata_t>();
//          config_namespace->document_metadata_by_document[item.document] = document_metadata;
//        } else {
//          document_metadata = document_metadata_search->second;
//        }
//
//        spdlog::debug(
//          "Updating a raw config (document: '{}', override: '{}', new_id: {})",
//          item.document,
//          item.override_,
//          config_namespace->next_raw_config_id
//        );
//
//        item.raw_config->id = config_namespace->next_raw_config_id++;
//        document_metadata->raw_config_by_version_by_override[item.override_][config_namespace->current_version] = item.raw_config;
//      }
//    }
//
//    //Remove the namespace if the versions numbers are on the limit
//    if (
//      (config_namespace->next_raw_config_id >= 0xff000000)
//      || (config_namespace->current_version >= 0xfffffff0)
//    ) {
//      auto search = config_namespace_by_root_path_.find(config_namespace->root_path);
//      if (
//        (search != config_namespace_by_root_path_.end())
//        && (search->second->id == config_namespace->id)
//      ) {
//        spdlog::info(
//          "Removing the namespace '{}' because the internal ids are in the limit",
//          config_namespace->root_path
//        );
//        config_namespace_by_root_path_.erase(search);
//      }
//    }
//
//    update_api_request->set_status(update_request::Status::OK);
//    update_api_request->set_version(config_namespace->current_version);
//    return send_api_response(update_api_request);
  //}

  void get_affected_documents(
    const std::shared_ptr<config_namespace_t> config_namespace,
    const std::string& document,
    std::unordered_set<std::string>& affected_documents
  ) {
    if (affected_documents.count(document)) return;
    affected_documents.insert(document);

    auto search = config_namespace->document_metadata_by_document.find(document);
    if (search == config_namespace->document_metadata_by_document.end()) return;

    for (const auto& referenced_document_it : search->second->referenced_by) {
      get_affected_documents(
        config_namespace,
        referenced_document_it.first,
        affected_documents
      );
    }
  }

  bool prepare_build_request(
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
      return false;
    }

    // If the graph is a DAG we could do a topological sort
    // of the required documents
    auto documents_in_order = do_topological_sort_over_ref_graph(
      is_a_dag_result.second
    );

    auto wait_built = std::make_shared<build::wait_built_t>();

    wait_built->is_main = documents_in_order.size() == 1;
    wait_built->request = (void*) get_request_;
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

      build_element.overrides_key = make_overrides_key(
        document_metadata,
        get_request_->overrides(),
        get_request_->version()
      );
      auto merged_config = get_or_build_merged_config(
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

    spdlog::debug(
      "The document '{}' need wait to {} building documents",
      get_request_->key()[0],
      wait_built->pending_element_position_by_name.size()
    );
    if (wait_built->pending_element_position_by_name.empty()) {
      auto build_command = std::make_shared<::mhconfig::worker::command::BuildCommand>(
        config_namespace->id,
        config_namespace->pool,
        wait_built
      );
      worker_queue.push(build_command);
    }

    return true;
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
      spdlog::warn(
        "Can't found a config file with the name '{}', exiting!",
        document
      );
      return false;
    }

    auto document_metadata = document_metadata_search->second;
    std::string overrides_key = make_overrides_key(
      document_metadata,
      overrides,
      version
    );
    if (get_merged_config(config_namespace, document, overrides_key) != nullptr) {
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

  std::shared_ptr<raw_config_t> get_raw_config(
    const std::shared_ptr<document_metadata_t> document_metadata,
    const std::string& override_,
    uint32_t version
  ) {
    spdlog::trace(
      "Obtaining the raw config of the override '{}' with version '{}'",
      override_,
      version
    );

    auto raw_config_by_version_search = document_metadata
      ->raw_config_by_version_by_override
      .find(override_);

    if (raw_config_by_version_search == document_metadata->raw_config_by_version_by_override.end()) {
      spdlog::trace("Don't exists the override '{}'", override_);
      return nullptr;
    }

    auto& raw_config_by_version = raw_config_by_version_search->second;

    auto raw_config_search = (version == 0)
      ? raw_config_by_version.end()
      : raw_config_by_version.upper_bound(version);

    if (raw_config_search == raw_config_by_version.begin()) {
      spdlog::trace("Don't exists a version lower or equal to {}", version);
      return nullptr;
    }

    --raw_config_search;
    if (raw_config_search->second->value == nullptr) {
      spdlog::trace(
        "The raw_config value is deleted for the version {}",
        raw_config_search->first
      );
      return nullptr;
    }

    spdlog::trace(
      "Obtained a raw config with the version {}",
      raw_config_search->first
    );

    return raw_config_search->second;
  }

  // Help functions
  std::string make_overrides_key(
    const std::shared_ptr<document_metadata_t> document_metadata,
    const std::vector<std::string>& overrides,
    uint32_t version
  ) {
    std::stringstream ss;
    for (auto& override_: overrides) {
      auto raw_config = get_raw_config(document_metadata, override_, version);
      if (raw_config != nullptr) {
        ss << (char)(((raw_config->id)>>24)&255)
           << (char)(((raw_config->id)>>16)&255)
           << (char)(((raw_config->id)>>8)&255)
           << (char)(((raw_config->id)>>0)&255);
      }
    }

    return ss.str();
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
  }




private:
  T get_request_;
};

} /* command */
} /* scheduler */
} /* mhconfig */

#endif
