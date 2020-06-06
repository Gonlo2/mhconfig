#include "mhconfig/scheduler/command/api_get_command.h"

namespace mhconfig
{
namespace scheduler
{
namespace command
{

ApiGetCommand::ApiGetCommand(
  std::shared_ptr<::mhconfig::api::request::GetRequest> get_request
) : get_request_(get_request)
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
  get_request_->set_version(
    get_specific_version(config_namespace, get_request_->version())
  );
  get_request_->set_namespace_id(config_namespace.id);

  if (get_request_->document().empty() || (get_request_->document()[0] == '_')) {
    spdlog::error(
      "The asked document '{}' don't have a valid name",
      get_request_->document()
    );

    get_request_->set_status(
      ::mhconfig::api::request::GetRequest::Status::ERROR
    );
    send_api_response(worker_queue);
    return NamespaceExecutionResult::OK;
  }

  if (!get_request_->template_().empty() && (get_request_->template_()[0] != '_')) {
    spdlog::error(
      "The asked template '{}' don't have a valid name",
      get_request_->template_()
    );

    get_request_->set_status(
      ::mhconfig::api::request::GetRequest::Status::ERROR
    );
    send_api_response(worker_queue);
    return NamespaceExecutionResult::OK;
  }

  // If the document exists and the user asked for a version
  // we check if the version is available
  if (
      (get_request_->version() != 0)
      && !config_namespace.stored_versions_by_deprecation_timestamp.empty()
      && (get_request_->version() < config_namespace.stored_versions_by_deprecation_timestamp.front().second)
  ) {
    spdlog::trace("The asked version {} don't exists", get_request_->version());

    get_request_->set_status(
      ::mhconfig::api::request::GetRequest::Status::INVALID_VERSION
    );
    send_api_response(worker_queue);
    return NamespaceExecutionResult::OK;
  }

  // If we are here it's possible obtain the asked document so first of all we check
  // if exists a cached value of the document with the requested overrides.
  // To search it we create the overrides key
  thread_local static std::string overrides_key;
  overrides_key.clear();
  overrides_key.reserve((get_request_->overrides().size()+1)*4);
  std::shared_ptr<inja::Template> template_;
  if (!add_overrides_key(config_namespace, overrides_key, template_)) {
    send_api_response(worker_queue);
    return NamespaceExecutionResult::OK;
  }

  auto merged_config = ::mhconfig::builder::get_merged_config(
    config_namespace,
    overrides_key
  );

  if (merged_config != nullptr) {
    auto status = merged_config->status;
    switch (status) {
      case MergedConfigStatus::UNDEFINED:  // Fallback
      case MergedConfigStatus::BUILDING:
        break;
      case MergedConfigStatus::OK_CONFIG_NO_OPTIMIZED:  // Fallback
        merged_config->status = MergedConfigStatus::OK_CONFIG_OPTIMIZING;
        // fallthrough
      case MergedConfigStatus::OK_CONFIG_OPTIMIZING:  // Fallback
      case MergedConfigStatus::OK_CONFIG_OPTIMIZED:  // Fallback
      case MergedConfigStatus::OK_TEMPLATE: {
        spdlog::debug(
          "The built document '{}' and template '{}' has been found",
          get_request_->document(),
          get_request_->template_()
        );

        merged_config->last_access_timestamp = jmutils::time::monotonic_now_sec();

        worker_queue.push(
          std::make_unique<::mhconfig::worker::command::ApiGetReplyCommand>(
            std::move(get_request_),
            std::move(merged_config),
            status
          )
        );
        return NamespaceExecutionResult::OK;
      }
    }
  }

  spdlog::debug("Preparing build for document '{}'", get_request_->document());
  return prepare_build_request(
    config_namespace,
    worker_queue,
    overrides_key,
    std::move(template_)
  );
}

NamespaceExecutionResult ApiGetCommand::prepare_build_request(
  config_namespace_t& config_namespace,
  WorkerQueue& worker_queue,
  const std::string& overrides_key,
  std::shared_ptr<inja::Template>&& template_
) {
  absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>> referenced_documents;
  // The references are allowed only if the graph is a DAG,
  // so we check it here
  bool is_a_dag = check_if_ref_graph_is_a_dag(
    config_namespace,
    get_request_->document(),
    get_request_->overrides(),
    get_request_->version(),
    referenced_documents
  );
  if (!is_a_dag) {
    get_request_->set_status(
      ::mhconfig::api::request::GetRequest::Status::REF_GRAPH_IS_NOT_DAG
    );
    send_api_response(worker_queue);
    return NamespaceExecutionResult::OK;
  }

  // If the graph is a DAG we could do a topological sort
  // of the required documents
  auto documents_in_order = do_topological_sort_over_ref_graph(referenced_documents);

  auto wait_built = std::make_shared<build::wait_built_t>();
  wait_built->specific_version = get_specific_version(
    config_namespace,
    get_request_->version()
  );
  wait_built->request = get_request_;
  wait_built->template_ = std::move(template_);
  wait_built->overrides_key = overrides_key;
  wait_built->num_pending_elements = 0;
  wait_built->elements_to_build.resize(documents_in_order.size());

  for (size_t i = 0, l = documents_in_order.size(); i < l; ++i) {
    auto& build_element = wait_built->elements_to_build[i];
    build_element.name = documents_in_order[i];
    spdlog::debug("Checking the document '{}'", build_element.name);

    auto& document_metadata = config_namespace
      .document_metadata_by_document[build_element.name];

    build_element.overrides_key.reserve(get_request_->overrides().size()*4);
    add_config_overrides_key(
      document_metadata,
      get_request_->overrides(),
      get_request_->version(),
      build_element.overrides_key
    );
    auto merged_config = ::mhconfig::builder::get_or_build_merged_config(
      config_namespace,
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
            document_metadata,
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
        wait_built->num_pending_elements += 1;
        config_namespace.wait_builts_by_key[build_element.overrides_key]
          .push_back(wait_built);
        break;
      }

      case MergedConfigStatus::OK_CONFIG_NO_OPTIMIZED:  // Fallback
      case MergedConfigStatus::OK_CONFIG_OPTIMIZING:  // Fallback
      case MergedConfigStatus::OK_CONFIG_OPTIMIZED: {
        spdlog::debug("The document '{}' is ok", build_element.name);
        build_element.config = merged_config->value;
        build_element.to_build = false;
        break;
      }
      case MergedConfigStatus::OK_TEMPLATE:
        assert(false);
    }
  }

  if (wait_built->num_pending_elements == 0) {
    spdlog::debug(
      "Sending the get request with id {} to built",
      (void*)wait_built->request.get()
    );
    worker_queue.push(
      std::make_unique<::mhconfig::worker::command::BuildCommand>(
        config_namespace.id,
        config_namespace.pool,
        std::move(wait_built)
      )
    );
  } else {
    spdlog::debug(
      "The document '{}' need wait to {} building documents",
      get_request_->document(),
      wait_built->num_pending_elements
    );
  }

  return NamespaceExecutionResult::OK;
}

bool ApiGetCommand::check_if_ref_graph_is_a_dag(
  config_namespace_t& config_namespace,
  const std::string& document,
  const std::vector<std::string>& overrides,
  uint32_t version,
  absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>& referenced_documents
) {
  std::vector<std::string> dfs_path;
  absl::flat_hash_set<std::string> dfs_path_set;

  bool is_a_dag = check_if_ref_graph_is_a_dag_rec(
    config_namespace,
    document,
    overrides,
    version,
    dfs_path,
    dfs_path_set,
    referenced_documents
  );

  referenced_documents[document] = absl::flat_hash_set<std::string>();

  return is_a_dag;
}

bool ApiGetCommand::check_if_ref_graph_is_a_dag_rec(
  config_namespace_t& config_namespace,
  const std::string& document,
  const std::vector<std::string>& overrides,
  uint32_t version,
  std::vector<std::string>& dfs_path,
  absl::flat_hash_set<std::string>& dfs_path_set,
  absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>& referenced_documents
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

  auto document_metadata_search = config_namespace.document_metadata_by_document
    .find(document);
  if (document_metadata_search == config_namespace.document_metadata_by_document.end()) {
    spdlog::warn("Can't found a config file with the name '{}'", document);
    return false;
  }

  thread_local static std::string overrides_key;
  overrides_key.clear();
  overrides_key.reserve(overrides.size()*4);
  add_config_overrides_key(
    document_metadata_search->second,
    overrides,
    version,
    overrides_key
  );
  auto config = ::mhconfig::builder::get_merged_config(
    config_namespace,
    overrides_key
  );
  if (config != nullptr) {
    return true;
  }

  dfs_path.push_back(document);
  dfs_path_set.insert(document);

  for (size_t i = 0, l = overrides.size(); i < l; ++i) {
    bool is_a_dag = true;
    with_raw_config(
      document_metadata_search->second,
      overrides[i],
      version,
      [&](auto& raw_config) {
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
  const absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>& referenced_documents
) {
  std::vector<std::string> reversed_topological_sort;
  absl::flat_hash_set<std::string> visited_documents;

  for (const auto it : referenced_documents) {
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
  const absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>& referenced_documents,
  absl::flat_hash_set<std::string>& visited_documents,
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
    ::mhconfig::api::request::GetRequest::Status::ERROR
  );
  send_api_response(worker_queue);
  return true;
}

inline void ApiGetCommand::send_api_response(
  WorkerQueue& worker_queue
) {
  worker_queue.push(
    std::make_unique<::mhconfig::worker::command::ApiReplyCommand>(
      std::move(get_request_)
    )
  );
}

} /* command */
} /* scheduler */
} /* mhconfig */
