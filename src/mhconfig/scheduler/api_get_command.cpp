#include "mhconfig/scheduler/api_get_command.h"

namespace mhconfig
{
namespace scheduler
{

ApiGetCommand::ApiGetCommand(
  std::shared_ptr<api::request::GetRequest>&& get_request
) : get_request_(std::move(get_request))
{
}

ApiGetCommand::~ApiGetCommand() {
}

std::string ApiGetCommand::name() const {
  return "API_GET";
}

SchedulerCommand::CommandType ApiGetCommand::type() const {
  return CommandType::GET_NAMESPACE_BY_PATH;
}

const std::string& ApiGetCommand::namespace_path() const {
  return get_request_->root_path();
}

SchedulerCommand::CommandResult ApiGetCommand::execute_on_namespace(
  config_namespace_t& config_namespace,
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue
) {
  uint32_t specific_version = get_specific_version(config_namespace, get_request_->version());
  get_request_->set_version(specific_version);
  get_request_->set_namespace_id(config_namespace.id);

  // If the request isn't valid we exit
  if (!validate_request(config_namespace, worker_queue)) {
    return CommandResult::OK;
  }

  std::vector<std::shared_ptr<api::Commitable>> traces;
  for_each_trace_to_trigger(
    config_namespace,
    get_request_.get(),
    [&traces, version=specific_version](auto namespace_id, const auto* message, auto* trace) {
      traces.push_back(
        scheduler::make_trace_output_message(
          trace,
          api::stream::TraceOutputMessage::Status::RETURNED_ELEMENTS,
          namespace_id,
          version,
          message
        )
      );
    }
  );
  if (!traces.empty()) {
    worker_queue.push(
      std::make_unique<worker::ApiBatchReplyCommand>(
        std::move(traces)
      )
    );
  }

  // If we are here it's possible obtain the asked document so first of all we check
  // if exists a cached value of the document with the requested overrides.
  // To search it we create the overrides key
  thread_local static std::string overrides_key;
  overrides_key.clear();
  overrides_key.reserve(((get_request_->flavors().size()+1)*get_request_->overrides().size()+1)*4);
  std::shared_ptr<inja::Template> template_;
  if (!add_overrides_key(config_namespace, overrides_key, template_)) {
    send_api_response(worker_queue);
    return CommandResult::OK;
  }

  spdlog::debug(
    "Searching the merged config of a overrides_key with size {}",
    overrides_key.size()
  );

  auto merged_config = builder::get_merged_config(config_namespace, overrides_key);
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

        merged_config->last_access_timestamp = jmutils::monotonic_now_sec();

        worker_queue.push(
          std::make_unique<worker::ApiGetReplyCommand>(
            std::move(get_request_),
            std::move(merged_config),
            status
          )
        );
        return CommandResult::OK;
      }
    }
  }

  spdlog::debug(
    "Preparing built for document '{}' and template '{}'",
    get_request_->document(),
    get_request_->template_()
  );
  return prepare_build_request(
    config_namespace,
    worker_queue,
    overrides_key,
    std::move(template_)
  );
}

bool ApiGetCommand::validate_request(
  const config_namespace_t& config_namespace,
  WorkerQueue& worker_queue
) {
  // If the document exists and the user asked for a version
  // we check if the version is available
  if (
    (get_request_->version() != 0)
    && !config_namespace.stored_versions_by_deprecation_timestamp.empty()
    && (get_request_->version() < config_namespace.stored_versions_by_deprecation_timestamp.front().second)
  ) {
    spdlog::error("The asked version {} don't exists", get_request_->version());
    get_request_->set_status(api::request::GetRequest::Status::INVALID_VERSION);
    send_api_response(worker_queue);
    return false;
  }

  return true;
}

SchedulerCommand::CommandResult ApiGetCommand::prepare_build_request(
  config_namespace_t& config_namespace,
  WorkerQueue& worker_queue,
  const std::string& overrides_key,
  std::shared_ptr<inja::Template>&& template_
) {
  auto wait_built = std::make_shared<build::wait_built_t>();

  std::vector<std::string> topological_sort;
  absl::flat_hash_map<std::string, std::string> overrides_key_by_document;
  // The references are allowed only if the graph is a DAG, so we check it here
  // and build at the same time all the necessary values
  bool is_a_dag = check_if_ref_graph_is_a_dag(
    config_namespace,
    topological_sort,
    wait_built->raw_config_by_override_path,
    overrides_key_by_document
  );
  if (!is_a_dag) {
    get_request_->set_status(api::request::GetRequest::Status::REF_GRAPH_IS_NOT_DAG);
    send_api_response(worker_queue);
    return CommandResult::OK;
  }

  wait_built->specific_version = get_specific_version(
    config_namespace,
    get_request_->version()
  );
  wait_built->request = get_request_;
  wait_built->template_ = std::move(template_);
  wait_built->overrides_key = overrides_key;
  wait_built->num_pending_elements = 0;
  wait_built->elements_to_build.resize(topological_sort.size());

  for (size_t i = 0, l = topological_sort.size(); i < l; ++i) {
    auto& build_element = wait_built->elements_to_build[i];
    build_element.name = std::move(topological_sort[i]);
    spdlog::debug("Checking the document '{}'", build_element.name);

    build_element.overrides_key = std::move(overrides_key_by_document[build_element.name]);

    auto merged_config = builder::get_or_build_merged_config(
      config_namespace,
      build_element.overrides_key
    );
    switch (merged_config->status) {
      case MergedConfigStatus::UNDEFINED: {
        spdlog::debug(
          "The document '{}' is undefined, preparing the building",
          build_element.name
        );
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
      std::make_unique<worker::BuildCommand>(
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

  return CommandResult::OK;
}

bool ApiGetCommand::check_if_ref_graph_is_a_dag(
  config_namespace_t& config_namespace,
  std::vector<std::string>& topological_sort,
  absl::flat_hash_map<std::string, std::shared_ptr<raw_config_t>>& raw_config_by_override_path,
  absl::flat_hash_map<std::string, std::string>& overrides_key_by_document
) {
  absl::flat_hash_set<std::string> dfs_path_set;

  return check_if_ref_graph_is_a_dag_rec(
    config_namespace,
    get_request_->flavors(),
    get_request_->overrides(),
    get_request_->document(),
    get_request_->version(),
    dfs_path_set,
    topological_sort,
    raw_config_by_override_path,
    overrides_key_by_document
  );
}

bool ApiGetCommand::check_if_ref_graph_is_a_dag_rec(
  config_namespace_t& config_namespace,
  const std::vector<std::string>& flavors,
  const std::vector<std::string>& overrides,
  const std::string& document,
  uint32_t version,
  absl::flat_hash_set<std::string>& dfs_path_set,
  std::vector<std::string>& topological_sort,
  absl::flat_hash_map<std::string, std::shared_ptr<raw_config_t>>& raw_config_by_override_path,
  absl::flat_hash_map<std::string, std::string>& overrides_key_by_document
) {
  if (dfs_path_set.count(document)) {
    spdlog::error("The config dependencies defined with the '!ref' tag has a cycle");
    return false;
  }

  if (overrides_key_by_document.count(document)) return true;

  std::string& overrides_key = overrides_key_by_document[document];
  overrides_key.reserve((flavors.size()+1)*overrides.size()*4);
  for_each_document_override(
    config_namespace,
    flavors,
    overrides,
    document,
    version,
    [&overrides_key, &raw_config_by_override_path](const auto& override_path, auto& raw_config) {
      if (raw_config->has_content) {
        raw_config_by_override_path[override_path] = raw_config;
      }
      jmutils::push_uint32(overrides_key, raw_config->id);
    }
  );

  bool is_a_dag = true;

  if (builder::get_merged_config(config_namespace, overrides_key) == nullptr) {
    dfs_path_set.insert(document);

    for_each_document_override(
      config_namespace,
      flavors,
      overrides,
      document,
      version,
      [&](const auto&, auto& raw_config) {
        if (is_a_dag && raw_config->has_content) {
          for (size_t i = 0, l = raw_config->reference_to.size(); i < l; ++i) {
            is_a_dag = check_if_ref_graph_is_a_dag_rec(
              config_namespace,
              flavors,
              overrides,
              raw_config->reference_to[i],
              version,
              dfs_path_set,
              topological_sort,
              raw_config_by_override_path,
              overrides_key_by_document
            );
          }
        }
      }
    );

    dfs_path_set.erase(document);
  }

  topological_sort.push_back(document);

  return is_a_dag;
}

bool ApiGetCommand::on_get_namespace_error(
  WorkerQueue& worker_queue
) {
  spdlog::error("Some error take place obtaining the namespace '{}'", get_request_->root_path());
  get_request_->set_status(api::request::GetRequest::Status::ERROR);
  send_api_response(worker_queue);
  return true;
}

inline void ApiGetCommand::send_api_response(
  WorkerQueue& worker_queue
) {
  worker_queue.push(
    std::make_unique<worker::ApiReplyCommand>(
      std::move(get_request_)
    )
  );
}

} /* scheduler */
} /* mhconfig */
