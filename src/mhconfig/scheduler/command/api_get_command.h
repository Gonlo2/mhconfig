#ifndef MHCONFIG__SCHEDULER__COMMAND__API_GET_COMMAND_H
#define MHCONFIG__SCHEDULER__COMMAND__API_GET_COMMAND_H

#include <memory>

#include "mhconfig/scheduler/command/command.h"

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
    return get_request_->key()[0];
  }

  bool execute_on_namespace(
    std::shared_ptr<config_namespace_t> config_namespace,
    Queue<worker::command::CommandRef>& worker_queue
  ) override {
    // First we set the basic request fields
    get_request_->set_version(
      get_specific_version(config_namespace, get_request_->version());
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
      return send_api_response(get_request_);
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
      return send_api_response(get_request_);
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

      return send_api_response(merged_config->api_merged_config);
    }

    ///////////////////////////// Check after this

    spdlog::debug("Preparing build for document '{}'", get_request_->key()[0]);
    return prepare_build_request(get_request_, config_namespace);
  }

  std::shared_ptr<merged_config_t> Scheduler::get_merged_config(
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

  std::shared_ptr<merged_config_t> Scheduler::get_or_build_merged_config(
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

  bool Scheduler::send_api_response(
    std::shared_ptr<mhconfig::api::config::MergedConfig> api_merged_config
  ) {
    command::command_t command;
    command.type = command::CommandType::API_GET;
    command.api_request = get_request;
    command.api_merged_config = api_merged_config;

    worker_queue_.push(command);

    return true;
  }

  bool Scheduler::process_command_type_run_gc_request(
    const std::shared_ptr<command::run_gc::request_t> run_gc_request
  ) {
    uint64_t limit_timestamp = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now().time_since_epoch()
    ).count() - run_gc_request->max_live_in_seconds;

    switch (run_gc_request->type) {
      case command::run_gc::Type::CACHE_GENERATION_0:
        return remove_merge_configs(limit_timestamp, 0);

      case command::run_gc::Type::CACHE_GENERATION_1:
        return remove_merge_configs(limit_timestamp, 1);

      case command::run_gc::Type::CACHE_GENERATION_2:
        return remove_merge_configs(limit_timestamp, 2);

      case command::run_gc::Type::DEAD_POINTERS:
        return remove_dead_pointers();

      case command::run_gc::Type::NAMESPACES:
        return remove_namespaces(limit_timestamp);

      case command::run_gc::Type::VERSIONS:
        return remove_versions(limit_timestamp);
    }

    return false;
  }

  bool Scheduler::remove_merge_configs(
    uint32_t limit_timestamp,
    uint32_t generation
  ) {
    spdlog::debug(
      "To remove merge configs in the {} generation without accessing them since timestamp {}",
      generation,
      limit_timestamp
    );

    size_t number_of_removed_merged_configs = 0;
    size_t number_of_processed_merged_configs = 0;
    for (auto& it : config_namespace_by_root_path_) {
      assert(it.second != nullptr);

      auto &from = it.second->merged_config_by_gc_generation[generation];
      number_of_processed_merged_configs += from.size();

      if (generation+1 < NUMBER_OF_GC_GENERATIONS) {
        auto& to = it.second->merged_config_by_gc_generation[generation+1];
        to.reserve(to.size() + from.size());

        for (auto merged_config : from) {
          if (merged_config->last_access_timestamp <= limit_timestamp) {
            ++number_of_removed_merged_configs;
          } else {
            to.push_back(merged_config);
          }
        }

        from.clear();
      } else {
        for (size_t i = 0; i < from.size(); ) {
          if (from[i]->last_access_timestamp < limit_timestamp) {
            from[i] = from.back();
            from.pop_back();
            ++number_of_removed_merged_configs;
          } else {
            ++i;
          }
        }
      }
    }

    spdlog::debug(
      "Removed merged configs (removed: {}, processed: {})",
      number_of_removed_merged_configs,
      number_of_processed_merged_configs
    );

    return true;
  }

  bool Scheduler::remove_dead_pointers() {
    spdlog::debug("To remove dead pointers");

    size_t number_of_removed_dead_pointers = 0;
    size_t number_of_processed_pointers = 0;
    for (auto& it : config_namespace_by_root_path_) {
      for (auto& it_2 : it.second->merged_config_metadata_by_overrides_key) {
        number_of_processed_pointers += it_2.second->merged_config_by_document.size();

        for (
          auto it_3 = it_2.second->merged_config_by_document.begin();
          it_3 != it_2.second->merged_config_by_document.end();
        ) {
          bool remove = true;
          if (auto merged_config = it_3->second.lock()) {
            remove = merged_config->status == MergedConfigStatus::REMOVED;
          }

          if (remove) {
            it_3 = it_2.second->merged_config_by_document.erase(it_3);
            ++number_of_removed_dead_pointers;
          } else {
            ++it_3;
          }
        }
      }
    }

    spdlog::debug(
      "Removed dead pointers (removed: {}, processed: {})",
      number_of_removed_dead_pointers,
      number_of_processed_pointers
    );

    return true;
  }

  bool Scheduler::remove_namespaces(
    uint32_t limit_timestamp
  ) {
    spdlog::debug(
      "To remove namespaces without accessing them since timestamp {}",
      limit_timestamp
    );

    size_t number_of_removed_namespaces = 0;
    size_t number_of_processed_namespaces = config_namespace_by_id_.size();
    for (
      auto it = config_namespace_by_id_.begin();
      it != config_namespace_by_id_.end();
    ) {
      if (it->second->last_access_timestamp <= limit_timestamp) {
        spdlog::debug(
          "Removing the namespace '{}' with id {}",
          it->second->root_path,
          it->first
        );

        auto search = config_namespace_by_root_path_.find(it->second->root_path);
        if (
          (search != config_namespace_by_root_path_.end())
          && (search->second->id == it->first)
        ) {
          config_namespace_by_root_path_.erase(search);
        }

        it = config_namespace_by_id_.erase(it);
        ++number_of_removed_namespaces;
      } else {
        ++it;
      }
    }

    spdlog::debug(
      "Removed namespaces (removed: {}, processed: {})",
      number_of_removed_namespaces,
      number_of_processed_namespaces
    );

    return true;
  }


  bool Scheduler::remove_versions(
    uint32_t limit_timestamp
  ) {
    spdlog::debug(
      "To remove versions deprecated before timestamp {}",
      limit_timestamp
    );

    for (auto& it : config_namespace_by_root_path_) {
      auto config_namespace = it.second;

      if (
          (config_namespace->stored_versions_by_deprecation_timestamp.size() > 1)
          && (config_namespace->stored_versions_by_deprecation_timestamp.front().first <= limit_timestamp)
      ) {
        while (
            (config_namespace->stored_versions_by_deprecation_timestamp.size() > 1)
            && (config_namespace->stored_versions_by_deprecation_timestamp.front().first <= limit_timestamp)
        ) {
          config_namespace->stored_versions_by_deprecation_timestamp.pop_front();
        }

        uint32_t remove_till_version = config_namespace
          ->stored_versions_by_deprecation_timestamp
          .front()
          .second;

        for (
          auto it_2 = config_namespace->document_metadata_by_document.begin();
          it_2 != config_namespace->document_metadata_by_document.end();
        ) {
          auto& raw_config_by_version_by_override = it_2
            ->second
            ->raw_config_by_version_by_override;

          for (
            auto it_3 = raw_config_by_version_by_override.begin();
            it_3 != raw_config_by_version_by_override.end();
          ) {
            auto& raw_config_by_version = it_3->second;

            auto version_search = raw_config_by_version.lower_bound(remove_till_version);
            bool delete_override = version_search == raw_config_by_version.end();
            if (delete_override) {
              delete_override = raw_config_by_version.rbegin()->second->value == nullptr;
              if (!delete_override) --version_search;
            }

            if (delete_override) {
              spdlog::debug(
                "Removed override '{}' in the namespace '{}'",
                it_3->first,
                it.first
              );

              it_3 = raw_config_by_version_by_override.erase(it_3);
            } else {
              spdlog::debug(
                "Removed the {} previous versions to {} of the override '{}' in the namespace '{}'",
                std::distance(raw_config_by_version.begin(), version_search),
                remove_till_version,
                it_3->first,
                it.first
              );

              raw_config_by_version.erase(
                raw_config_by_version.begin(),
                version_search
              );
              ++it_3;
            }
          }

          if (raw_config_by_version_by_override.empty()) {
            it_2 = config_namespace->document_metadata_by_document.erase(it_2);
          } else {
            ++it_2;
          }
        }
      }
    }

    return true;
  }

  bool Scheduler::process_command_type_update_response(
    const std::shared_ptr<command::update::response_t> update_response,
    std::shared_ptr<config_namespace_t> config_namespace
  ) {
    auto update_api_request = (update_request::UpdateRequest*) update_response->api_request;
    update_api_request->set_namespace_id(config_namespace->id);

    if (update_response->status != command::update::ResponseStatus::OK) {
      switch (update_response->status) {
        case command::update::ResponseStatus::OK:
        case command::update::ResponseStatus::ERROR:
          update_api_request->set_status(update_request::Status::ERROR);
      }
      return send_api_response(update_api_request);
    }

    spdlog::debug(
      "Increasing the current version {} of the namespace with ID {}",
      config_namespace->current_version,
      config_namespace->id
    );
    ++(config_namespace->current_version);

    uint64_t timestamp = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now().time_since_epoch()
    ).count();

    config_namespace->stored_versions_by_deprecation_timestamp.back().first = timestamp;
    config_namespace->stored_versions_by_deprecation_timestamp.emplace_back(
      0,
      config_namespace->current_version
    );


    spdlog::debug("Decreasing the referenced_by counter of the previous raw config");
    for (auto& item : update_response->items) {
      auto document_metadata_search = config_namespace->document_metadata_by_document
        .find(item.document);

      if (document_metadata_search != config_namespace->document_metadata_by_document.end()) {
        auto raw_config = get_raw_config(
          document_metadata_search->second,
          item.override_,
          0
        );

        if (raw_config != nullptr) {
          for (const auto& reference_to : raw_config->reference_to) {
            auto& referenced_by = config_namespace
              ->document_metadata_by_document[reference_to]
              ->referenced_by;

            spdlog::debug(
              "Decreasing referenced_by counter (document: '{}', reference_to: '{}', counter: {}, override: '{}')",
              item.document,
              reference_to,
              referenced_by[item.document].v,
              item.override_
            );

            if (referenced_by[item.document].v-- <= 1) {
              referenced_by.erase(item.document);
            }
          }
        }
      }
    }

    spdlog::debug("Updating the id of the affected documents");
    std::unordered_map<std::string, std::unordered_set<std::string>> updated_documents_by_override;
    for (auto& item : update_response->items) {
      updated_documents_by_override[item.override_].insert(item.document);
    }

    for (auto& updated_documents_it : updated_documents_by_override) {
      std::unordered_set<std::string> affected_documents;
      for (const auto& document : updated_documents_it.second) {
        get_affected_documents(config_namespace, document, affected_documents);
      }
      for (const auto& document : updated_documents_it.second) {
        affected_documents.erase(document);
      }

      for (const auto document : affected_documents) {
        auto document_metadata = config_namespace
          ->document_metadata_by_document[document];

        auto raw_config = get_raw_config(
          document_metadata,
          updated_documents_it.first,
          0
        );

        if (raw_config != nullptr) {
          spdlog::debug(
            "Updating affected raw config id (document: '{}', override: '{}', old_id: {}, new_id: {})",
            document,
            updated_documents_it.first,
            raw_config->id,
            config_namespace->next_raw_config_id
          );

          auto new_raw_config = std::make_shared<raw_config_t>();
          new_raw_config->id = config_namespace->next_raw_config_id++;
          new_raw_config->value = raw_config->value;
          new_raw_config->reference_to = raw_config->reference_to;

          document_metadata->raw_config_by_version_by_override[updated_documents_it.first][config_namespace->current_version] = new_raw_config;
        }
      }
    }

    spdlog::debug("Updating the elements of the updated documents");
    for (auto& item : update_response->items) {
      auto document_metadata_search = config_namespace->document_metadata_by_document
        .find(item.document);

      if (item.raw_config == nullptr) {
        spdlog::debug(
          "Removing a raw config (document: '{}', override: '{}')",
          item.document,
          item.override_
        );
        if (document_metadata_search != config_namespace->document_metadata_by_document.end()) {
          document_metadata_search->second->raw_config_by_version_by_override[item.override_][config_namespace->current_version] = std::make_shared<raw_config_t>();
        }
      } else {
        for (const auto& reference_to : item.raw_config->reference_to) {
          std::shared_ptr<document_metadata_t> referenced_document_metadata;
          {
            auto search = config_namespace->document_metadata_by_document
              .find(reference_to);
            if (search == config_namespace->document_metadata_by_document.end()) {
              referenced_document_metadata = std::make_shared<document_metadata_t>();
              config_namespace
                ->document_metadata_by_document[reference_to] = referenced_document_metadata;
            } else {
              referenced_document_metadata = search->second;
            }
          }

          spdlog::debug(
            "Increasing referenced_by counter (document: '{}', reference_to: '{}', counter: {}, override: '{}')",
            item.document,
            reference_to,
            referenced_document_metadata->referenced_by[item.document].v,
            item.override_
          );

          referenced_document_metadata->referenced_by[item.document].v += 1;
        }

        std::shared_ptr<document_metadata_t> document_metadata;
        if (document_metadata_search == config_namespace->document_metadata_by_document.end()) {
          document_metadata = std::make_shared<document_metadata_t>();
          config_namespace->document_metadata_by_document[item.document] = document_metadata;
        } else {
          document_metadata = document_metadata_search->second;
        }

        spdlog::debug(
          "Updating a raw config (document: '{}', override: '{}', new_id: {})",
          item.document,
          item.override_,
          config_namespace->next_raw_config_id
        );

        item.raw_config->id = config_namespace->next_raw_config_id++;
        document_metadata->raw_config_by_version_by_override[item.override_][config_namespace->current_version] = item.raw_config;
      }
    }

    //Remove the namespace if the versions numbers are on the limit
    if (
      (config_namespace->next_raw_config_id >= 0xff000000)
      || (config_namespace->current_version >= 0xfffffff0)
    ) {
      auto search = config_namespace_by_root_path_.find(config_namespace->root_path);
      if (
        (search != config_namespace_by_root_path_.end())
        && (search->second->id == config_namespace->id)
      ) {
        spdlog::info(
          "Removing the namespace '{}' because the internal ids are in the limit",
          config_namespace->root_path
        );
        config_namespace_by_root_path_.erase(search);
      }
    }

    update_api_request->set_status(update_request::Status::OK);
    update_api_request->set_version(config_namespace->current_version);
    return send_api_response(update_api_request);
  }

  void Scheduler::get_affected_documents(
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

  bool Scheduler::prepare_build_request(
    get_request::GetRequest* get_request,
    std::shared_ptr<config_namespace_t> config_namespace
  ) {
    auto is_a_dag_result = check_if_ref_graph_is_a_dag(
      config_namespace,
      get_request->key()[0],
      get_request->overrides(),
      get_request->version()
    );
    if (!is_a_dag_result.first) return false;

    auto documents_in_order = do_topological_sort_over_ref_graph(is_a_dag_result.second);

    auto wait_built = std::make_shared<wait_built_t>();
    wait_built->is_main = documents_in_order.size() == 1;
    wait_built->command.type = command::CommandType::BUILD_REQUEST;

    wait_built->command.build_request = std::make_shared<command::build::request_t>();
    wait_built->command.build_request->request = get_request;
    wait_built->command.build_request->namespace_id = config_namespace->id;
    wait_built->command.build_request->specific_version = get_specific_version(
      config_namespace,
      get_request->version()
    );
    wait_built->command.build_request->pool = config_namespace->pool;
    wait_built->command.build_request->elements_to_build.resize(documents_in_order.size());

    for (size_t i = 0; i < documents_in_order.size(); ++i) {
      auto& build_element = wait_built->command.build_request->elements_to_build[i];
      build_element.name = documents_in_order[i];
      spdlog::debug("Checking the document '{}'", build_element.name);

      auto document_metadata = config_namespace->document_metadata_by_document[
        build_element.name
      ];
      build_element.overrides_key = make_overrides_key(
        document_metadata,
        get_request->overrides(),
        get_request->version()
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
          for (const auto& k : get_request->overrides()) {
            auto raw_config = get_raw_config(document_metadata, k, get_request->version());
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
          wait_built_key_ss << build_element.name << ':' << build_element.overrides_key;

          config_namespace->wait_builts_by_key[wait_built_key_ss.str()].push_back(wait_built);
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
      get_request->key()[0],
      wait_built->pending_element_position_by_name.size()
    );
    if (wait_built->pending_element_position_by_name.empty()) {
      worker_queue_.push(wait_built->command);
    }

    return true;
  }

  bool Scheduler::process_command_type_setup_response(
    const std::shared_ptr<command::setup::response_t> setup_response
  ) {
    assert(config_namespace_by_id_.count(setup_response->config_namespace->id) == 0);

    config_namespace_by_root_path_[
      setup_response->root_path
    ] = setup_response->config_namespace;

    config_namespace_by_id_[
      setup_response->config_namespace->id
    ] = setup_response->config_namespace;

    auto search = commands_waiting_for_config_namespace_by_root_path_
      .find(setup_response->root_path);

    scheduler_queue_.push(search->second);
    commands_waiting_for_config_namespace_by_root_path_.erase(search);

    return true;
  }

  bool Scheduler::process_command_type_build_response(
    const std::shared_ptr<command::build::response_t> build_response
  ) {
    auto get_request = (get_request::GetRequest*) build_response->request;

    spdlog::debug(
      "Processing the build response with id {}",
      (uint64_t) get_request
    );

    auto config_namespace_search = config_namespace_by_id_
      .find(build_response->namespace_id);
    assert(config_namespace_search != config_namespace_by_id_.end());
    auto config_namespace = config_namespace_search->second;

    for (const auto it : build_response->built_elements_by_document) {
      spdlog::debug("Setting the document '{}'", it.first);

      auto document_metadata = config_namespace->document_metadata_by_document[it.first];
      auto merged_config = get_or_build_merged_config(
        config_namespace,
        it.first,
        it.second.overrides_key
      );
      merged_config->status = MergedConfigStatus::OK;
      merged_config->last_access_timestamp = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()
      ).count();
      merged_config->value = it.second.config;
      //merged_config->api_merged_config = std::make_shared<mhconfig::api::config::BasicMergedConfig>(it.second.config);
      auto test_me = std::make_shared<mhconfig::api::config::OptimizedMergedConfig>();
      test_me->init(it.second.config);
      merged_config->api_merged_config = test_me;

      std::stringstream wait_built_key_ss;
      wait_built_key_ss << it.first << ':' << it.second.overrides_key;
      auto& wait_builts = config_namespace->wait_builts_by_key[wait_built_key_ss.str()];

      for (size_t i = 0; i < wait_builts.size(); ) {
        std::shared_ptr<wait_built_t> wait_built = wait_builts[i];

        spdlog::debug("Unchecking element built");
        auto search = wait_built->pending_element_position_by_name.find(it.first);
        wait_built->command
          .build_request
          ->elements_to_build[search->second]
          .config = it.second.config;
        wait_built->pending_element_position_by_name.erase(search);

        if (wait_built->pending_element_position_by_name.empty()) {
          if (wait_built->is_main) {
            spdlog::debug(
              "Responding the get with id: {}",
              (uint64_t) wait_built->command.build_request->request
            );

            auto other_get_request = (get_request::GetRequest*) wait_built->command.build_request->request;

            other_get_request->set_namespace_id(config_namespace->id);
            other_get_request->set_version(wait_built->command.build_request->specific_version);
            send_api_get_response(other_get_request, merged_config->api_merged_config);
          } else {
            worker_queue_.push(wait_built->command);
          }

          wait_builts[i] = wait_builts.back();
          wait_builts.pop_back();
        } else {
          ++i;
        }
      }
    }

    get_request->set_namespace_id(config_namespace->id);
    get_request->set_version(build_response->specific_version);

    auto merged_config = get_merged_config(
      config_namespace,
      get_request->key()[0],
      build_response->built_elements_by_document[get_request->key()[0]].overrides_key
    );

    return send_api_get_response(get_request, merged_config->api_merged_config);
  }

  std::pair<bool, std::unordered_map<std::string, std::unordered_set<std::string>>> Scheduler::check_if_ref_graph_is_a_dag(
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

  bool Scheduler::check_if_ref_graph_is_a_dag_rec(
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

  std::vector<std::string> Scheduler::do_topological_sort_over_ref_graph(
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

  void Scheduler::do_topological_sort_over_ref_graph_rec(
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

  std::shared_ptr<raw_config_t> Scheduler::get_raw_config(
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
  std::string ApiGetCommand::make_overrides_key(
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

  inline uint32_t Scheduler::get_specific_version(
    const std::shared_ptr<config_namespace_t> config_namespace,
    uint32_t version
  ) {
    return (version == 0) ? config_namespace->current_version : version;
  }

  } /* worker */
  } /* mhconfig */


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
