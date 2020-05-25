#include "mhconfig/scheduler/command/api_get_command.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

ApiGetCommand::ApiGetCommand(
  std::shared_ptr<::mhconfig::api::request::GetRequest> get_request
) : Command(),
    get_request_(get_request)
{
}

ApiGetCommand::~ApiGetCommand() {
}

std::string ApiGetCommand::name() const {
  return "API_GET";
}

CommandType ApiGetCommand::command_type() const {
  return CommandType::GET_NAMESPACE_BY_PATH;
}

const std::string& ApiGetCommand::namespace_path() const {
  return get_request_->root_path();
}

NamespaceExecutionResult ApiGetCommand::execute_on_namespace(
  config_namespace_t& config_namespace,
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue
) {
  // First we set the basic request fields
  get_request_->set_version(
    get_specific_version(config_namespace, get_request_->version())
  );
  get_request_->set_namespace_id(config_namespace.id);

  // We check if exists the asked document
  auto search = config_namespace.document_metadata_by_document
    .find(get_request_->document());

  if (search == config_namespace.document_metadata_by_document.end()) {
    spdlog::warn(
      "Can't found a config file with the name '{}'",
      get_request_->document()
    );

    get_request_->set_element(UNDEFINED_ELEMENT.get());
    send_api_response(worker_queue);
    return NamespaceExecutionResult::OK;
  }

  // If the document exists and the user asked for a version
  // we check if the version is available
  auto document_metadata = search->second;
  if (
      (get_request_->version() != 0)
      && !config_namespace.stored_versions_by_deprecation_timestamp.empty()
      && (get_request_->version() < config_namespace.stored_versions_by_deprecation_timestamp.front().second)
  ) {
    spdlog::trace("The asked version {} don't exists", get_request_->version());

    get_request_->set_status(
      ::mhconfig::api::request::get_request::Status::INVALID_VERSION
    );
    send_api_response(worker_queue);
    return NamespaceExecutionResult::OK;
  }

  // If we are here it's possible obtain the asked document so first of all we check
  // if exists a cached value of the document with the requested overrides.
  // To search it we create the overrides key
  thread_local static std::string overrides_key;
  overrides_key.clear();
  make_overrides_key(
    *document_metadata,
    get_request_->overrides(),
    get_request_->version(),
    overrides_key
  );
  auto merged_config = ::mhconfig::builder::get_merged_config(
    config_namespace,
    get_request_->document(),
    overrides_key
  );

  if ((merged_config != nullptr) && (merged_config->status == MergedConfigStatus::OK)) {
    spdlog::debug("The built document '{}' has been found", get_request_->document());

    merged_config->last_access_timestamp = jmutils::time::monotonic_now_sec();

    send_api_get_response(worker_queue, merged_config->api_merged_config);
    return NamespaceExecutionResult::OK;
  }

  spdlog::debug("Preparing build for document '{}'", get_request_->document());
  return prepare_build_request(
    config_namespace,
    worker_queue
  );
}

void ApiGetCommand::send_api_response(
  WorkerQueue& worker_queue
) {
  worker_queue.push(
    std::make_unique<::mhconfig::worker::command::ApiReplyCommand>(
      std::move(get_request_)
    )
  );
}

void ApiGetCommand::send_api_get_response(
  WorkerQueue& worker_queue,
  std::shared_ptr<mhconfig::api::config::MergedConfig> api_merged_config
) {
  worker_queue.push(
    std::make_unique<::mhconfig::worker::command::ApiGetReplyCommand>(
      std::move(get_request_),
      api_merged_config
    )
  );
}

NamespaceExecutionResult ApiGetCommand::prepare_build_request(
  config_namespace_t& config_namespace,
  WorkerQueue& worker_queue
) {
  // The references are allowed only if the graph is a DAG,
  // so we check it here
  auto is_a_dag_result = check_if_ref_graph_is_a_dag(
    config_namespace,
    get_request_->document(),
    get_request_->overrides(),
    get_request_->version()
  );
  if (!is_a_dag_result.first) {
    get_request_->set_status(
      ::mhconfig::api::request::get_request::Status::REF_GRAPH_IS_NOT_DAG
    );
    send_api_response(worker_queue);
    return NamespaceExecutionResult::OK;
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
      .document_metadata_by_document[build_element.name];

    make_overrides_key(
      *document_metadata,
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
          with_raw_config(
            *document_metadata,
            k,
            get_request_->version(),
            [&build_element, &k](auto& raw_config) {
              build_element.raw_config_by_override[k] = raw_config;
            }
          );
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

        config_namespace.wait_builts_by_key[build_element.overrides_key]
          .push_back(wait_built);
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
      (void*)wait_built->request.get()
    );
    worker_queue.push(
      std::make_unique<::mhconfig::worker::command::BuildCommand>(
        config_namespace.id,
        config_namespace.pool,
        wait_built
      )
    );
  } else {
    spdlog::debug(
      "The document '{}' need wait to {} building documents",
      get_request_->document(),
      wait_built->pending_element_position_by_name.size()
    );
  }

  return NamespaceExecutionResult::OK;
}

std::pair<bool, std::unordered_map<std::string, std::unordered_set<std::string>>> ApiGetCommand::check_if_ref_graph_is_a_dag(
  config_namespace_t& config_namespace,
  const std::string& document,
  const std::vector<std::string>& overrides,
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

bool ApiGetCommand::check_if_ref_graph_is_a_dag_rec(
  config_namespace_t& config_namespace,
  const std::string& document,
  const std::vector<std::string>& overrides,
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

  auto document_metadata_search = config_namespace.document_metadata_by_document.find(
    document
  );
  if (document_metadata_search == config_namespace.document_metadata_by_document.end()) {
    spdlog::warn("Can't found a config file with the name '{}'", document);
    return false;
  }

  auto document_metadata = document_metadata_search->second;
  std::string overrides_key;
  make_overrides_key(
    *document_metadata,
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
    bool is_a_dag = true;
    with_raw_config(
      *document_metadata,
      overrides[i],
      version,
      [this, &config_namespace, &document, &overrides, version, &dfs_path,
        &dfs_path_set, &referenced_documents, &is_a_dag
      ](auto& raw_config) {
        for (const auto& ref_document : raw_config->reference_to) {
          referenced_documents[ref_document].insert(document);
          is_a_dag = check_if_ref_graph_is_a_dag_rec(
            config_namespace,
            ref_document,
            overrides,
            version,
            dfs_path,
            dfs_path_set,
            referenced_documents
          );
          if (!is_a_dag) return;
        }
      }
    );
    if (!is_a_dag) return false;
  }

  dfs_path.pop_back();
  dfs_path_set.erase(document);

  return true;
}

std::vector<std::string> ApiGetCommand::do_topological_sort_over_ref_graph(
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

void ApiGetCommand::do_topological_sort_over_ref_graph_rec(
  const std::string& document,
  const std::unordered_map<std::string, std::unordered_set<std::string>>& referenced_documents,
  std::unordered_set<std::string>& visited_documents,
  std::vector<std::string>& inverted_topological_sort
) {
  auto is_inserted = visited_documents.insert(document);
  if (is_inserted.second) {
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
}

bool ApiGetCommand::on_get_namespace_error(
  WorkerQueue& worker_queue
) {
  get_request_->set_status(
    ::mhconfig::api::request::get_request::Status::ERROR
  );
  send_api_response(worker_queue);
  return true;
}

} /* command */
} /* scheduler */
} /* mhconfig */
