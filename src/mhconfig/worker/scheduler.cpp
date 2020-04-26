#include "mhconfig/worker/scheduler.h"

namespace mhconfig
{
namespace worker
{

Scheduler::Scheduler(
  Queue<command::command_t>& scheduler_queue,
  Queue<command::command_t>& worker_queue,
  Metrics& metrics
) :
  scheduler_queue_(scheduler_queue),
  worker_queue_(worker_queue),
  metrics_(metrics)
{
}

Scheduler::~Scheduler() {
}

bool Scheduler::start() {
  if (thread_ != nullptr) return false;
  thread_ = std::make_unique<std::thread>(&Scheduler::run, this);
  return true;
}

void Scheduler::join() {
  if (thread_ != nullptr) {
    thread_->join();
  }
}

void Scheduler::run() {
  while (true) {
    auto command = scheduler_queue_.pop();

    auto start_time = std::chrono::high_resolution_clock::now();
    try {
      process_command(command);
    } catch(...) {
      logger_->error("Some unknown error take place processing a {} command", to_string(command.type));
    }
    auto end_time = std::chrono::high_resolution_clock::now();

    double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    metrics_.scheduler_duration(to_string(command.type), duration_ns);
  }
}

bool Scheduler::process_command(command::command_t& command) {
  //jmutils::metrics::ScopeDuration scope_duration(
    //registry_,
    //"scheduler_process_duration_nanoseconds",
    //"How many nanoseconds takes process a task by the scheduler worker"
  //);

  logger_->debug("Received a {} command", to_string(command.type));

  switch (command.type) {
    case command::CommandType::API: {
      logger_->debug("Obtained a API request {}", command.api_request->name());

      switch (command.api_request->id()) {
        case 0: {  //TODO GET
          auto get_request = (get_request::GetRequest*) command.api_request;

          auto config_namespace_result = get_or_build_config_namespace(
            get_request->root_path(),
            command
          );
          switch (config_namespace_result.first) {
            case ConfigNamespaceState::OK:
              return process_command_type_get_request(
                get_request,
                config_namespace_result.second
              );

            case ConfigNamespaceState::BUILDING:
              return true;

            case ConfigNamespaceState::ERROR: {  //TODO Return a proper error
              get_request->set_element(UNDEFINED_ELEMENT);
              return send_api_response(get_request);
            }
          }

          return false;
        }
        case 1: {  //TODO UPDATE
          auto update_request = (update_request::UpdateRequest*) command.api_request;

          auto config_namespace_result = get_or_build_config_namespace(
            update_request->root_path(),
            command
          );
          switch (config_namespace_result.first) {
            case ConfigNamespaceState::OK:
              return process_command_type_update_request(
                update_request,
                config_namespace_result.second
              );

            case ConfigNamespaceState::BUILDING:
              return true;

            case ConfigNamespaceState::ERROR:
              update_request->set_status(update_request::Status::ERROR);
              return send_api_response(update_request);
          }

          return false;
        }
      }

      logger_->debug("Unknown API request {}", command.api_request->name());
      return false;
    }

    case command::CommandType::RUN_GC_REQUEST:
      return process_command_type_run_gc_request(command.run_gc_request);

    case command::CommandType::SETUP_RESPONSE:
      return process_command_type_setup_response(command.setup_response);

    case command::CommandType::BUILD_RESPONSE:
      return process_command_type_build_response(command.build_response);

    case command::CommandType::UPDATE_RESPONSE: {
      auto search = config_namespace_by_id_
        .find(command.update_response->namespace_id);

      if (search == config_namespace_by_id_.end()) {
        auto update_request = (update_request::UpdateRequest*) command.update_response->api_request;
        update_request->set_status(update_request::Status::ERROR);
        return send_api_response(update_request);
      } else {
        return process_command_type_update_response(
          command.update_response,
          search->second
        );
      }
    }
  }

  logger_->debug(
    "A scheduler worker can't process a {} command",
    to_string(command.type)
  );
  return false;
}

bool Scheduler::process_command_type_get_request(
  get_request::GetRequest* get_request,
  std::shared_ptr<config_namespace_t> config_namespace
) {
  auto document_metadata_search = config_namespace->document_metadata_by_document.find(
    get_request->key()[0]
  );
  if (document_metadata_search == config_namespace->document_metadata_by_document.end()) {
    logger_->warn("Can't found a config file with the name '{}'", get_request->key()[0]);

    get_request->set_namespace_id(config_namespace->id);
    get_request->set_version(get_specific_version(config_namespace, get_request->version()));
    get_request->set_element(UNDEFINED_ELEMENT);
    return send_api_response(get_request);
  }

  if (
      (get_request->version() != 0)
      && !config_namespace->stored_versions_by_deprecation_timestamp.empty()
      && (get_request->version() < config_namespace->stored_versions_by_deprecation_timestamp.front().second)
  ) {
    logger_->trace("The asked version {} don't exists", get_request->version());

    get_request->set_namespace_id(config_namespace->id);
    get_request->set_version(get_request->version());
    get_request->set_element(UNDEFINED_ELEMENT);
    return send_api_response(get_request);
  }

  auto document_metadata = document_metadata_search->second;
  std::string overrides_key = make_overrides_key(
    document_metadata,
    get_request->overrides(),
    get_request->version()
  );
  auto merged_config = get_merged_config(
    config_namespace,
    get_request->key()[0],
    overrides_key
  );

  if ((merged_config != nullptr) && (merged_config->status == MergedConfigStatus::OK)) {
    logger_->debug("The built document '{}' has been found", get_request->key()[0]);

    merged_config->last_access_timestamp = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now().time_since_epoch()
    ).count();

    get_request->set_namespace_id(config_namespace->id);
    get_request->set_version(get_specific_version(config_namespace, get_request->version()));
    return send_api_get_response(get_request, merged_config->api_merged_config);
  }

  logger_->debug("Preparing build for document '{}'", get_request->key()[0]);
  return prepare_build_request(
    get_request,
    config_namespace
  );
}

bool Scheduler::process_command_type_update_request(
  update_request::UpdateRequest* update_request,
  std::shared_ptr<config_namespace_t> config_namespace
) {
    command::command_t update_command;
    update_command.type = command::CommandType::UPDATE_REQUEST;
    update_command.update_request = std::make_shared<command::update::request_t>();
    update_command.update_request->namespace_id = config_namespace->id;
    update_command.update_request->pool = config_namespace->pool;
    update_command.update_request->api_request = update_request;

    worker_queue_.push(update_command);
    return true;
}

std::shared_ptr<merged_config_t> Scheduler::get_merged_config(
  std::shared_ptr<config_namespace_t> config_namespace,
  const std::string& document,
  const std::string& overrides_key
) {
  std::shared_ptr<merged_config_metadata_t> merged_config_metadata = nullptr;
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

  auto search = merged_config_metadata
    ->merged_config_by_document
    .find(document);

  if (search == merged_config_metadata->merged_config_by_document.end()) {
    return nullptr;
  }

  if (auto merged_config = search->second.lock()) {
    if (merged_config->status != MergedConfigStatus::REMOVED) {
      return merged_config;
    }
  }

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

std::pair<Scheduler::ConfigNamespaceState, std::shared_ptr<config_namespace_t>> Scheduler::get_or_build_config_namespace(
  const std::string& root_path,
  const command::command_t& command
) {
  auto search_config_namespace = config_namespace_by_root_path_.find(root_path);
  if (search_config_namespace == config_namespace_by_root_path_.end()) {
    auto search_commands_waiting = commands_waiting_for_config_namespace_by_root_path_
      .find(root_path);

    if (search_commands_waiting == commands_waiting_for_config_namespace_by_root_path_.end()) {
      command::command_t setup_command;
      setup_command.type = command::CommandType::SETUP_REQUEST;
      setup_command.setup_request = std::make_shared<command::setup::request_t>();
      setup_command.setup_request->root_path = root_path;

      worker_queue_.push(setup_command);

      commands_waiting_for_config_namespace_by_root_path_[root_path].push_back(command);
    } else {
      search_commands_waiting->second.push_back(command);
    }

    return std::make_pair(ConfigNamespaceState::BUILDING, nullptr);
  }

  if (search_config_namespace->second == nullptr || !search_config_namespace->second->ok) {
    return std::make_pair(ConfigNamespaceState::ERROR, nullptr);
  }

  search_config_namespace
    ->second
    ->last_access_timestamp = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now().time_since_epoch()
    ).count();

  return std::make_pair(ConfigNamespaceState::OK, search_config_namespace->second);
}

bool Scheduler::send_api_get_response(
  get_request::GetRequest* get_request,
  std::shared_ptr<mhconfig::api::config::MergedConfig> api_merged_config
) {
  command::command_t command;
  command.type = command::CommandType::API_GET;
  command.api_request = get_request;
  command.api_merged_config = api_merged_config;

  worker_queue_.push(command);

  return true;
}

bool Scheduler::send_api_response(
  Request* api_request
) {
  command::command_t command;
  command.type = command::CommandType::API;
  command.api_request = api_request;

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
  logger_->debug(
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

  logger_->debug(
    "Removed merged configs (removed: {}, processed: {})",
    number_of_removed_merged_configs,
    number_of_processed_merged_configs
  );

  return true;
}

bool Scheduler::remove_dead_pointers() {
  logger_->debug("To remove dead pointers");

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

  logger_->debug(
    "Removed dead pointers (removed: {}, processed: {})",
    number_of_removed_dead_pointers,
    number_of_processed_pointers
  );

  return true;
}

bool Scheduler::remove_namespaces(
  uint32_t limit_timestamp
) {
  logger_->debug(
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
      logger_->debug(
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

  logger_->debug(
    "Removed namespaces (removed: {}, processed: {})",
    number_of_removed_namespaces,
    number_of_processed_namespaces
  );

  return true;
}


bool Scheduler::remove_versions(
  uint32_t limit_timestamp
) {
  logger_->debug(
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
            logger_->debug(
              "Removed override '{}' in the namespace '{}'",
              it_3->first,
              it.first
            );

            it_3 = raw_config_by_version_by_override.erase(it_3);
          } else {
            logger_->debug(
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

  logger_->debug(
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


  logger_->debug("Decreasing the referenced_by counter of the previous raw config");
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

          logger_->debug(
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

  logger_->debug("Updating the id of the affected documents");
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
        logger_->debug(
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

  logger_->debug("Updating the elements of the updated documents");
  for (auto& item : update_response->items) {
    auto document_metadata_search = config_namespace->document_metadata_by_document
      .find(item.document);

    if (item.raw_config == nullptr) {
      logger_->debug(
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

        logger_->debug(
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

      logger_->debug(
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
      logger_->info(
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
    logger_->debug("Checking the document '{}'", build_element.name);

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
        logger_->debug(
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
        logger_->debug(
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
        logger_->debug("The document '{}' is ok", build_element.name);
        build_element.config = merged_config->value;
        break;
      }
    }
  }

  logger_->debug(
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

  logger_->debug(
    "Processing the build response with id {}",
    (uint64_t) get_request
  );

  auto config_namespace_search = config_namespace_by_id_
    .find(build_response->namespace_id);
  assert(config_namespace_search != config_namespace_by_id_.end());
  auto config_namespace = config_namespace_search->second;

  for (const auto it : build_response->built_elements_by_document) {
    logger_->debug("Setting the document '{}'", it.first);

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

      logger_->debug("Unchecking element built");
      auto search = wait_built->pending_element_position_by_name.find(it.first);
      wait_built->command
        .build_request
        ->elements_to_build[search->second]
        .config = it.second.config;
      wait_built->pending_element_position_by_name.erase(search);

      if (wait_built->pending_element_position_by_name.empty()) {
        if (wait_built->is_main) {
          logger_->debug(
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

    logger_->error(
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
    logger_->warn(
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
  logger_->trace(
    "Obtaining the raw config of the override '{}' with version '{}'",
    override_,
    version
  );

  auto raw_config_by_version_search = document_metadata
    ->raw_config_by_version_by_override
    .find(override_);

  if (raw_config_by_version_search == document_metadata->raw_config_by_version_by_override.end()) {
    logger_->trace("Don't exists the override '{}'", override_);
    return nullptr;
  }

  auto& raw_config_by_version = raw_config_by_version_search->second;

  auto raw_config_search = (version == 0)
    ? raw_config_by_version.end()
    : raw_config_by_version.upper_bound(version);

  if (raw_config_search == raw_config_by_version.begin()) {
    logger_->trace("Don't exists a version lower or equal to {}", version);
    return nullptr;
  }

  --raw_config_search;
  if (raw_config_search->second->value == nullptr) {
    logger_->trace(
      "The raw_config value is deleted for the version {}",
      raw_config_search->first
    );
    return nullptr;
  }

  logger_->trace(
    "Obtained a raw config with the version {}",
    raw_config_search->first
  );

  return raw_config_search->second;
}

std::string Scheduler::make_overrides_key(
  const std::shared_ptr<document_metadata_t> document_metadata,
  const std::vector<std::string>& overrides,
  uint32_t version
) {
  std::stringstream ss;
  for (auto& override_: overrides) {
    auto raw_config = get_raw_config(document_metadata, override_, version);
    uint32_t id = (raw_config == nullptr) ? 0 : raw_config->id;

    ss << (char)((id>>24)&255)
       << (char)((id>>16)&255)
       << (char)((id>>8)&255)
       << (char)((id>>0)&255);
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
