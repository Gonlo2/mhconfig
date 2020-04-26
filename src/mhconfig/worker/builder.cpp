#include "mhconfig/worker/builder.h"

namespace mhconfig
{
namespace worker
{

std::mt19937_64& prng_engine() {
  thread_local static std::mt19937_64 engine{std::random_device{}()};
  return engine;
}

Builder::Builder(
  Queue<command::command_t>& scheduler_queue,
  Queue<command::command_t>& builder_queue,
  size_t num_threads,
  Metrics& metrics
) :
  jmutils::parallelism::Worker<command::command_t>(builder_queue, num_threads),
  metrics_(metrics),
  scheduler_queue_(scheduler_queue)
{
}

Builder::~Builder() {
}

Builder::Builder(
  Builder&& o
) :
  jmutils::parallelism::Worker<command::command_t>(std::forward<Builder>(o)),
  metrics_(o.metrics_),
  scheduler_queue_(o.scheduler_queue_)
{}

Builder::ProcessResult Builder::worker_process(
  const command::command_t& command
) {
  switch (command.type) {
    case command::CommandType::API:
      return process_command_type_api(command.api_request)
        ? ProcessResult::OK
        : ProcessResult::ERROR;

    case command::CommandType::API_GET: {
      auto get_request = (get_request::GetRequest*) command.api_request;
      return process_command_type_api_get(get_request, command.api_merged_config)
        ? ProcessResult::OK
        : ProcessResult::ERROR;
    }

    case command::CommandType::SETUP_REQUEST:
      return process_command_type_setup_request(command.setup_request)
        ? ProcessResult::OK
        : ProcessResult::ERROR;

    case command::CommandType::BUILD_REQUEST:
      return process_command_type_build_request(command.build_request)
        ? ProcessResult::OK
        : ProcessResult::ERROR;

    case command::CommandType::UPDATE_REQUEST:
      return process_command_type_update_request(command.update_request)
        ? ProcessResult::OK
        : ProcessResult::ERROR;
  }

  return ProcessResult::MISSING;
}

bool Builder::process_command_type_api(
  Request* api_request
) {
  api_request->reply();
  return true;
}

bool Builder::process_command_type_api_get(
  get_request::GetRequest* api_request,
  std::shared_ptr<mhconfig::api::config::MergedConfig> merged_config
) {
  auto start_time = std::chrono::high_resolution_clock::now();
  merged_config->add_elements(api_request->key(), api_request->response());
  auto end_time = std::chrono::high_resolution_clock::now();

  double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
    end_time - start_time
  ).count();
  metrics_.serialization_duration(duration_ns);

  api_request->reply();
  return true;
}

bool Builder::process_command_type_setup_request(
  const std::shared_ptr<command::setup::request_t> setup_request
) {
  command::command_t command;
  command.type = command::CommandType::SETUP_RESPONSE;
  command.setup_response = std::make_shared<command::setup::response_t>();
  command.setup_response->root_path = setup_request->root_path;
  command.setup_response->config_namespace = index_files(
    setup_request->root_path
  );

  scheduler_queue_.push(command);

  return true;
}

bool Builder::process_command_type_build_request(
  const std::shared_ptr<command::build::request_t> build_request
) {
  command::command_t command;
  command.type = command::CommandType::BUILD_RESPONSE;
  command.build_response = std::make_shared<command::build::response_t>();
  command.build_response->request = build_request->request;
  command.build_response->namespace_id = build_request->namespace_id;
  command.build_response->specific_version = build_request->specific_version;

  auto get_request = (get_request::GetRequest*) build_request->request;  //TODO check

  std::unordered_map<std::string, ElementRef> ref_elements_by_document;
  for (auto& build_element : build_request->elements_to_build) {
    size_t override_id = 0;
    ElementRef config = build_element.config;

    if (config == nullptr) {
      for (; (override_id < get_request->overrides().size()) && (config == nullptr); ++override_id) {
        auto search = build_element.raw_config_by_override.find(
          get_request->overrides()[override_id]
        );
        if (search != build_element.raw_config_by_override.end()) {
          config = search->second->value;
        }
      }

      for (; override_id < get_request->overrides().size(); ++override_id) {
        auto search = build_element.raw_config_by_override.find(
          get_request->overrides()[override_id]
        );
        if (search != build_element.raw_config_by_override.end()) {
          config = override_with(
            config,
            search->second->value,
            ref_elements_by_document
          );
        }
      }

      config = (config == nullptr)
        ? UNDEFINED_ELEMENT
        : apply_tags(build_request->pool, config, config, ref_elements_by_document);

      command::build::built_element_t built_element;
      built_element.overrides_key = build_element.overrides_key;
      built_element.config = config;
      command.build_response->built_elements_by_document[build_element.name] = built_element;
    }

    ref_elements_by_document[build_element.name] = config;
  }

  scheduler_queue_.push(command);

  return true;
}

bool Builder::process_command_type_update_request(
  const std::shared_ptr<command::update::request_t> update_request
) {
  auto api_update_request = (update_request::UpdateRequest*) update_request->api_request;

  command::command_t command;
  command.type = command::CommandType::UPDATE_RESPONSE;
  command.update_response = std::make_shared<command::update::response_t>();
  command.update_response->api_request = update_request->api_request;
  command.update_response->namespace_id = update_request->namespace_id;
  command.update_response->status = command::update::ResponseStatus::ERROR;
  command.update_response->items.reserve(api_update_request->relative_paths().size());

  for (const std::string& relative_path : api_update_request->relative_paths()) {
    std::string path = jmutils::filesystem::join_paths(
      api_update_request->root_path(),
      relative_path
    );

    auto result = load_raw_config(
      update_request->pool,
      api_update_request->root_path(),
      path
    );
    switch (result.status) {
      case LoadRawConfigStatus::OK:
      case LoadRawConfigStatus::FILE_DONT_EXISTS:
        command.update_response->items.emplace_back(
          result.document,
          result.override_,
          result.raw_config
        );
        break;

      case LoadRawConfigStatus::INVALID_FILE:
      case LoadRawConfigStatus::ERROR:
        scheduler_queue_.push(command);
        return true;
    }
  }

  command.update_response->status = command::update::ResponseStatus::OK;
  scheduler_queue_.push(command);

  return true;
}

// Setup logic

std::shared_ptr<config_namespace_t> Builder::index_files(
  const std::string& root_path
) {
  auto config_namespace = std::make_shared<config_namespace_t>();
  config_namespace->ok = false;
  config_namespace->root_path = root_path;
  config_namespace->id = config_namespace_id_dist_(prng_engine());
  config_namespace->last_access_timestamp = 0;
  config_namespace->pool = std::make_shared<string_pool::Pool>();

  auto paths = jmutils::filesystem::recursive_list_files(root_path);
  for (const std::string& path : paths) {
    auto result = load_raw_config(config_namespace->pool, root_path, path);
    if (result.status == LoadRawConfigStatus::INVALID_FILE) {
      continue;
    } else if (result.status != LoadRawConfigStatus::OK) {
      return config_namespace;
    }

    std::shared_ptr<document_metadata_t> document_metadata = nullptr;
    {
      auto search = config_namespace->document_metadata_by_document.find(
        result.document
      );
      if (search == config_namespace->document_metadata_by_document.end()) {
        document_metadata = std::make_shared<document_metadata_t>();
        config_namespace->document_metadata_by_document[
          result.document
        ] = document_metadata;
      } else {
        document_metadata = search->second;
      }
    }

    result.raw_config->id = config_namespace->next_raw_config_id++;
    document_metadata->raw_config_by_version_by_override[result.override_][1] = result.raw_config;
  }

  for (auto document_metadata_it : config_namespace->document_metadata_by_document) {
    for (auto raw_config_by_override_it : document_metadata_it.second->raw_config_by_version_by_override) {
      for (auto& reference_to : raw_config_by_override_it.second[1]->reference_to) {
        std::shared_ptr<document_metadata_t> referenced_document_metadata;
        {
          auto search = config_namespace->document_metadata_by_document
            .find(reference_to);
          if (search == config_namespace->document_metadata_by_document.end()) {
            logger_->error(
              "The document '{}' in the override '{}' has a '{}' tag to the inexistent document '{}'",
              document_metadata_it.first,
              raw_config_by_override_it.first,
              TAG_REF,
              reference_to
            );

            return config_namespace;
          } else {
            referenced_document_metadata = search->second;
          }
        }

        referenced_document_metadata->referenced_by[document_metadata_it.first].v += 1;
      }
    }
  }

  uint64_t timestamp = std::chrono::duration_cast<std::chrono::seconds>(
    std::chrono::system_clock::now().time_since_epoch()
  ).count();

  config_namespace->ok = true;
  config_namespace->last_access_timestamp = timestamp;
  config_namespace->stored_versions_by_deprecation_timestamp.emplace_back(
    0,
    config_namespace->current_version
  );

  return config_namespace;
}

Builder::load_raw_config_result_t Builder::load_raw_config(
  std::shared_ptr<string_pool::Pool> pool,
  const std::string& root_path,
  const std::string& path
) {
  load_raw_config_result_t result;
  result.status = LoadRawConfigStatus::ERROR;

  try {
    auto file_name_and_ext = jmutils::filesystem::file_name_and_extension(path);
    if (file_name_and_ext.second != ".yaml") {
      logger_->error(
        "The config filetype must be '.yaml' (path: '{}')",
        path
      );
      result.status = LoadRawConfigStatus::INVALID_FILE;
      return result;
    }

    result.document = file_name_and_ext.first;
    result.override_ = jmutils::filesystem::relative_parent_path(
      path,
      root_path
    );

    if (!jmutils::filesystem::is_regular_file(path)) {
      result.status = jmutils::filesystem::exists(path)
        ? LoadRawConfigStatus::INVALID_FILE
        : LoadRawConfigStatus::FILE_DONT_EXISTS;

      return result;
    }

    logger_->debug("Loading YAML (path: '{}')", path);
    YAML::Node node = YAML::LoadFile(path);

    result.raw_config = std::make_shared<raw_config_t>();
    result.raw_config->value = make_and_check_element(
      pool,
      node,
      result.raw_config->reference_to
    );
  } catch (const std::exception &e) {
    logger_->error(
      "Error making the element (path: '{}'): {}",
      path,
      e.what()
    );
    return result;
  } catch(...) {
    logger_->error(
      "Unknown error making the element (path: '{}')",
      path
    );
    return result;
  }

  result.status = LoadRawConfigStatus::OK;
  return result;
}

ElementRef Builder::override_with(
  ElementRef a,
  ElementRef b,
  const std::unordered_map<std::string, ElementRef> &ref_elements_by_document
) {
  if (b->tag() == TAG_OVERRIDE) {
    return b->clone_without_tag();
  }

  bool is_first_a_ref = a->tag() == TAG_REF;
  if (is_first_a_ref || (b->tag() == TAG_REF)) {
    auto referenced_element = apply_tag_ref(
      is_first_a_ref ? a : b,
      ref_elements_by_document
    );

    return override_with(
      is_first_a_ref ? referenced_element : a,
      is_first_a_ref ? b : referenced_element,
      ref_elements_by_document
    );
  }

  switch (get_virtual_node_type(b)) {
    case NULL_NODE:
    case SCALAR_NODE: {
      NodeType type = get_virtual_node_type(a);
      if ((type != NULL_NODE) && (type != SCALAR_NODE)) {
        logger_->warn(
          "Can't override {} with {} without the '{}' tag",
          a->repr(),
          b->repr(),
          TAG_OVERRIDE
        );
        return a;
      }

      return b;
    }

    case MAP_NODE: {
      if (!a->is_map()) {
        logger_->warn(
          "Can't override {} with {} without the '{}' tag",
          a->repr(),
          b->repr(),
          TAG_OVERRIDE
        );
        return a;
      }

      auto map = std::make_shared<Map>();
      map->reserve(a->as_map().size() + b->as_map().size());

      for (const auto& x : a->as_map()) {
        if (x.second->tag() != TAG_DELETE) {
          (*map)[x.first] = x.second;
        }
      }

      for (const auto& x : b->as_map()) {
        const auto& search = map->find(x.first);
        if (search == map->end()) {
          if (x.second->tag() != TAG_DELETE) {
            (*map)[x.first] = x.second;
          }
        } else if (x.second->tag() == TAG_DELETE) {
          map->erase(search);
        } else {
          (*map)[x.first] = override_with(
            search->second,
            x.second,
            ref_elements_by_document
          );
        }
      }

      return std::make_shared<Element>(map);
    }

    case SEQUENCE_NODE: {
      if (!a->is_sequence()) {
        logger_->warn(
          "Can't override {} with {} without the '{}' tag",
          a->repr(),
          b->repr(),
          TAG_OVERRIDE
        );
        return a;
      }

      auto sequence = std::make_shared<Sequence>();
      sequence->reserve(a->as_sequence().size() + b->as_sequence().size());

      for (const auto& x: a->as_sequence()) {
        if (x->tag() != TAG_DELETE) sequence->push_back(x);
      }

      for (const auto& x: b->as_sequence()) {
        if (x->tag() != TAG_DELETE) sequence->push_back(x);
      }

      return std::make_shared<Element>(sequence);
    }
  }

  logger_->warn("Can't override {} with {}", a->repr(), b->repr());
  return a;
}

NodeType Builder::get_virtual_node_type(
  ElementRef element
) {
  if (element->tag() == TAG_FORMAT) return SCALAR_NODE;
  if (element->tag() == TAG_SREF) return SCALAR_NODE;

  return element->type();
}

ElementRef Builder::apply_tags(
  std::shared_ptr<string_pool::Pool> pool,
  ElementRef element,
  ElementRef root,
  const std::unordered_map<std::string, ElementRef> &ref_elements_by_document
) {
  if (element->tag() == TAG_DELETE) {
    return UNDEFINED_ELEMENT;
  }

  switch (element->type()) {
    case MAP_NODE: {
      auto map = std::make_shared<Map>();
      map->reserve(element->as_map().size());

      bool changed = false;
      for (const auto& it : element->as_map()) {
        if (it.second->tag() == TAG_DELETE) {
          changed = true;
        } else {
          auto new_element = apply_tags(pool, it.second, root, ref_elements_by_document);

          changed |= new_element != it.second;
          (*map)[it.first] = new_element;
        }
      }

      if (changed) {
        element = std::make_shared<Element>(map, element->tag());
      }
      break;
    }

    case SEQUENCE_NODE: {
      auto sequence = std::make_shared<Sequence>();
      sequence->reserve(element->as_sequence().size());

      bool changed = false;
      for (const auto x : element->as_sequence()) {
        if (x->tag() == TAG_DELETE) {
          changed = true;
        } else {
          auto new_element = apply_tags(pool, x, root, ref_elements_by_document);

          changed |= new_element != x;
          sequence->push_back(new_element);
        }
      }

      if (changed) {
        element = std::make_shared<Element>(sequence, element->tag());
      }
      break;
    }
  }

  if (element->tag() == TAG_SREF) {
    element = apply_tag_sref(element, root);
  }

  if (element->tag() == TAG_REF) {
    element = apply_tag_ref(element, ref_elements_by_document);
  }

  if (element->tag() == TAG_FORMAT) {
    element = apply_tag_format(pool, element);
  }

  return element;
}

ElementRef Builder::apply_tag_format(
  std::shared_ptr<string_pool::Pool> pool,
  ElementRef element
) {
  if (!element->is_sequence() || (element->as_sequence().size() != 2)) {
    logger_->error(
      "The structure with the tag '{}' must be a sequence of size 2",
      TAG_FORMAT
    );
    return UNDEFINED_ELEMENT;
  }

  auto template_node = element->as_sequence()[0];
  auto arguments_node = element->as_sequence()[1];

  if (!template_node->is_scalar()) {
    logger_->error(
      "The '{}' tag first argument must be a template",
      TAG_FORMAT
    );
    return UNDEFINED_ELEMENT;
  }

  if (!arguments_node->is_map()) {
    logger_->error(
      "The '{}' tag second argument must be a map [string -> string]",
      TAG_FORMAT
    );
    return UNDEFINED_ELEMENT;
  }

  uint32_t num_arguments = arguments_node->as_map().size();
  if (num_arguments > 8) {
    logger_->error(
      "The '{}' tag can't handle more that 8 arguments",
      TAG_FORMAT
    );
    return UNDEFINED_ELEMENT;
  }

  std::vector<std::pair<std::string, std::string>> template_arguments;
  for (const auto& it : arguments_node->as_map()) {
    if (!it.second->is_scalar()) {
      logger_->error(
        "The '{}' tag second argument must be a map [string -> string]",
        TAG_FORMAT
      );
      return UNDEFINED_ELEMENT;
    }

    template_arguments.emplace_back(
      it.first.str(),
      it.second->as<std::string>()
    );
  }

  std::string value = format_str(
    template_node->as<std::string>(),
    num_arguments,
    template_arguments
  );

  return std::make_shared<Element>(pool->add(value));
}

std::string Builder::format_str(
  const std::string& templ,
  uint32_t num_arguments,
  const std::vector<std::pair<std::string, std::string>>& template_arguments
) {

  switch (num_arguments) {
    case 0:
      return templ;

    case 1:
      return fmt::format(
        templ,
        fmt::arg(template_arguments[0].first, template_arguments[0].second)
      );

    case 2:
      return fmt::format(
        templ,
        fmt::arg(template_arguments[0].first, template_arguments[0].second),
        fmt::arg(template_arguments[1].first, template_arguments[1].second)
      );

    case 3:
      return fmt::format(
        templ,
        fmt::arg(template_arguments[0].first, template_arguments[0].second),
        fmt::arg(template_arguments[1].first, template_arguments[1].second),
        fmt::arg(template_arguments[2].first, template_arguments[2].second)
      );

    case 4:
      return fmt::format(
        templ,
        fmt::arg(template_arguments[0].first, template_arguments[0].second),
        fmt::arg(template_arguments[1].first, template_arguments[1].second),
        fmt::arg(template_arguments[2].first, template_arguments[2].second),
        fmt::arg(template_arguments[3].first, template_arguments[3].second)
      );

    case 5:
      return fmt::format(
        templ,
        fmt::arg(template_arguments[0].first, template_arguments[0].second),
        fmt::arg(template_arguments[1].first, template_arguments[1].second),
        fmt::arg(template_arguments[2].first, template_arguments[2].second),
        fmt::arg(template_arguments[3].first, template_arguments[3].second),
        fmt::arg(template_arguments[4].first, template_arguments[4].second)
      );

    case 6:
      return fmt::format(
        templ,
        fmt::arg(template_arguments[0].first, template_arguments[0].second),
        fmt::arg(template_arguments[1].first, template_arguments[1].second),
        fmt::arg(template_arguments[2].first, template_arguments[2].second),
        fmt::arg(template_arguments[3].first, template_arguments[3].second),
        fmt::arg(template_arguments[4].first, template_arguments[4].second),
        fmt::arg(template_arguments[5].first, template_arguments[5].second)
      );

    case 7:
      return fmt::format(
        templ,
        fmt::arg(template_arguments[0].first, template_arguments[0].second),
        fmt::arg(template_arguments[1].first, template_arguments[1].second),
        fmt::arg(template_arguments[2].first, template_arguments[2].second),
        fmt::arg(template_arguments[3].first, template_arguments[3].second),
        fmt::arg(template_arguments[4].first, template_arguments[4].second),
        fmt::arg(template_arguments[5].first, template_arguments[5].second),
        fmt::arg(template_arguments[6].first, template_arguments[6].second)
      );

    case 8:
      return fmt::format(
        templ,
        fmt::arg(template_arguments[0].first, template_arguments[0].second),
        fmt::arg(template_arguments[1].first, template_arguments[1].second),
        fmt::arg(template_arguments[2].first, template_arguments[2].second),
        fmt::arg(template_arguments[3].first, template_arguments[3].second),
        fmt::arg(template_arguments[4].first, template_arguments[4].second),
        fmt::arg(template_arguments[5].first, template_arguments[5].second),
        fmt::arg(template_arguments[6].first, template_arguments[6].second),
        fmt::arg(template_arguments[7].first, template_arguments[7].second)
      );
  }

  return "";
}

ElementRef Builder::apply_tag_ref(
  ElementRef element,
  const std::unordered_map<std::string, ElementRef> &ref_elements_by_document
) {
  const auto& path = element->as_sequence();
  size_t path_len = path.size();

  std::string key = path[0]->as<std::string>();
  auto search = ref_elements_by_document.find(key);
  if (search == ref_elements_by_document.end()) {
    logger_->error("Can't ref to the document '{}'", key);
    return UNDEFINED_ELEMENT;
  }

  auto referenced_element = search->second;
  for (size_t i = 1; i < path_len; ++i) {
    referenced_element = referenced_element->get(path[i]->as<string_pool::String>());
  }

  return referenced_element;
}

ElementRef Builder::apply_tag_sref(
  ElementRef element,
  ElementRef root
) {
  if (!element->is_sequence() || element->as_sequence().empty()) {
    logger_->error(
      "The structure with the tag '{}' must be a non empty sequence",
      TAG_SREF
    );
    return UNDEFINED_ELEMENT;
  }

  for (const auto& element : element->as_sequence()) {
    root = root->get(element->as<string_pool::String>());
  }

  if (!root->is_scalar()) {
    logger_->error(
      "The element referenced by '{}' must be a scalar",
      TAG_SREF
    );
    return UNDEFINED_ELEMENT;
  }

  return root;
}

/*
 * All the structure checks must be done here
 */
ElementRef Builder::make_and_check_element(
    std::shared_ptr<string_pool::Pool> pool,
    YAML::Node &node,
    std::unordered_set<std::string> &reference_to
) {
  auto element = make_element(pool, node, reference_to);

  if (element->tag() == TAG_REF) {
    if (!element->is_sequence()) {
      logger_->error("The entry with the '{}' tag must be a sequence", TAG_REF);
      return UNDEFINED_ELEMENT;
    }

    const auto& ref_path = element->as_sequence();
    if (ref_path.empty()) {
      logger_->error("The key 'path' in a '{}' must be a sequence with at least one element", TAG_REF);
      return UNDEFINED_ELEMENT;
    }

    for (const auto x : ref_path) {
      if (!x->is_scalar()) {
        logger_->error("All the elements of the key 'path' in a '{}' must be scalars", TAG_REF);
        return UNDEFINED_ELEMENT;
      }
    }

    reference_to.insert(ref_path.front()->as<std::string>());
  }

  return element;
}

ElementRef Builder::make_element(
    std::shared_ptr<string_pool::Pool> pool,
    YAML::Node &node,
    std::unordered_set<std::string> &reference_to
) {
  switch (node.Type()) {
    case YAML::NodeType::Null:
      return std::make_shared<Element>(NULL_NODE);

    case YAML::NodeType::Scalar:
      return std::make_shared<Element>(
        pool->add(node.as<std::string>()),
        pool->add(node.Tag())
      );

    case YAML::NodeType::Sequence: {
      auto sequence = std::make_shared<Sequence>();
      sequence->reserve(node.size());
      for (auto it : node) {
        sequence->push_back(make_and_check_element(pool, it, reference_to));
      }
      return std::make_shared<Element>(
        sequence,
        pool->add(node.Tag())
      );
    }

    case YAML::NodeType::Map: {
      auto map = std::make_shared<Map>();
      map->reserve(node.size());
      for (auto it : node) {
        auto k = make_and_check_element(pool, it.first, reference_to);
        auto v = make_and_check_element(pool, it.second, reference_to);

        (*map)[k->as<string_pool::String>()] = v;
      }
      return std::make_shared<Element>(
        map,
        pool->add(node.Tag())
      );
    }
  }

  return UNDEFINED_ELEMENT;
}

} /* worker */
} /* mhconfig */
