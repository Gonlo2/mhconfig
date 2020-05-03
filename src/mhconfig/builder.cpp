#include "mhconfig/builder.h"

namespace mhconfig
{
namespace builder
{

// Setup logic

std::shared_ptr<config_namespace_t> index_files(
  const std::string& root_path
) {
  auto config_namespace = std::make_shared<config_namespace_t>();
  config_namespace->ok = false;
  config_namespace->root_path = root_path;
  config_namespace->id = std::uniform_int_distribution<uint64_t>{0, 0xffffffffffffffff}(jmutils::prng_engine());
  config_namespace->last_access_timestamp = 0;
  config_namespace->pool = std::make_shared<string_pool::Pool>();

  spdlog::debug("To index the files in the path '{}'", root_path);

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
    document_metadata->override_by_key[result.override_].raw_config_by_version[1] = result.raw_config;
  }

  for (auto document_metadata_it : config_namespace->document_metadata_by_document) {
    for (auto override_it : document_metadata_it.second->override_by_key) {
      for (auto& reference_to : override_it.second.raw_config_by_version[1]->reference_to) {
        std::shared_ptr<document_metadata_t> referenced_document_metadata;
        {
          auto search = config_namespace->document_metadata_by_document
            .find(reference_to);
          if (search == config_namespace->document_metadata_by_document.end()) {
            spdlog::error(
              "The document '{}' in the override '{}' has a '{}' tag to the inexistent document '{}'",
              document_metadata_it.first,
              override_it.first,
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

  config_namespace->ok = true;
  config_namespace->last_access_timestamp = jmutils::time::monotonic_now_sec();
  config_namespace->stored_versions_by_deprecation_timestamp.emplace_back(
    0,
    config_namespace->current_version
  );

  return config_namespace;
}

load_raw_config_result_t load_raw_config(
  std::shared_ptr<string_pool::Pool> pool,
  const std::string& root_path,
  const std::string& path
) {
  load_raw_config_result_t result;
  result.status = LoadRawConfigStatus::ERROR;

  try {
    auto file_name_and_ext = jmutils::filesystem::file_name_and_extension(path);
    if (file_name_and_ext.second != ".yaml") {
      spdlog::error(
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

    spdlog::debug("Loading YAML (path: '{}')", path);
    YAML::Node node = YAML::LoadFile(path);

    result.raw_config = std::make_shared<raw_config_t>();
    result.raw_config->value = make_and_check_element(
      pool,
      node,
      result.raw_config->reference_to
    );
  } catch (const std::exception &e) {
    spdlog::error(
      "Error making the element (path: '{}'): {}",
      path,
      e.what()
    );
    return result;
  } catch(...) {
    spdlog::error(
      "Unknown error making the element (path: '{}')",
      path
    );
    return result;
  }

  result.status = LoadRawConfigStatus::OK;
  return result;
}

ElementRef override_with(
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
        spdlog::warn(
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
        spdlog::warn(
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
        spdlog::warn(
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

  spdlog::warn("Can't override {} with {}", a->repr(), b->repr());
  return a;
}

NodeType get_virtual_node_type(
  ElementRef element
) {
  if (element->tag() == TAG_FORMAT) return SCALAR_NODE;
  if (element->tag() == TAG_SREF) return SCALAR_NODE;

  return element->type();
}

ElementRef apply_tags(
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

ElementRef apply_tag_format(
  std::shared_ptr<string_pool::Pool> pool,
  ElementRef element
) {
  if (!element->is_sequence() || (element->as_sequence().size() != 2)) {
    spdlog::error(
      "The structure with the tag '{}' must be a sequence of size 2",
      TAG_FORMAT
    );
    return UNDEFINED_ELEMENT;
  }

  auto template_node = element->as_sequence()[0];
  auto arguments_node = element->as_sequence()[1];

  if (!template_node->is_scalar()) {
    spdlog::error(
      "The '{}' tag first argument must be a template",
      TAG_FORMAT
    );
    return UNDEFINED_ELEMENT;
  }

  if (!arguments_node->is_map()) {
    spdlog::error(
      "The '{}' tag second argument must be a map [string -> string]",
      TAG_FORMAT
    );
    return UNDEFINED_ELEMENT;
  }

  uint32_t num_arguments = arguments_node->as_map().size();
  if (num_arguments > 8) {
    spdlog::error(
      "The '{}' tag can't handle more that 8 arguments",
      TAG_FORMAT
    );
    return UNDEFINED_ELEMENT;
  }

  std::vector<std::pair<std::string, std::string>> template_arguments;
  for (const auto& it : arguments_node->as_map()) {
    if (!it.second->is_scalar()) {
      spdlog::error(
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

std::string format_str(
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

ElementRef apply_tag_ref(
  ElementRef element,
  const std::unordered_map<std::string, ElementRef> &ref_elements_by_document
) {
  const auto& path = element->as_sequence();
  size_t path_len = path.size();

  std::string key = path[0]->as<std::string>();
  auto search = ref_elements_by_document.find(key);
  if (search == ref_elements_by_document.end()) {
    spdlog::error("Can't ref to the document '{}'", key);
    return UNDEFINED_ELEMENT;
  }

  auto referenced_element = search->second;
  for (size_t i = 1; i < path_len; ++i) {
    referenced_element = referenced_element->get(path[i]->as<string_pool::String>());
  }

  return referenced_element;
}

ElementRef apply_tag_sref(
  ElementRef element,
  ElementRef root
) {
  if (!element->is_sequence() || element->as_sequence().empty()) {
    spdlog::error(
      "The structure with the tag '{}' must be a non empty sequence",
      TAG_SREF
    );
    return UNDEFINED_ELEMENT;
  }

  for (const auto& element : element->as_sequence()) {
    root = root->get(element->as<string_pool::String>());
  }

  if (!root->is_scalar()) {
    spdlog::error(
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
ElementRef make_and_check_element(
    std::shared_ptr<string_pool::Pool> pool,
    YAML::Node &node,
    std::unordered_set<std::string> &reference_to
) {
  auto element = make_element(pool, node, reference_to);

  if (element->tag() == TAG_REF) {
    if (!element->is_sequence()) {
      spdlog::error("The entry with the '{}' tag must be a sequence", TAG_REF);
      return UNDEFINED_ELEMENT;
    }

    const auto& ref_path = element->as_sequence();
    if (ref_path.empty()) {
      spdlog::error("The key 'path' in a '{}' must be a sequence with at least one element", TAG_REF);
      return UNDEFINED_ELEMENT;
    }

    for (const auto x : ref_path) {
      if (!x->is_scalar()) {
        spdlog::error("All the elements of the key 'path' in a '{}' must be scalars", TAG_REF);
        return UNDEFINED_ELEMENT;
      }
    }

    reference_to.insert(ref_path.front()->as<std::string>());
  }

  return element;
}

ElementRef make_element(
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

// Get logic

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
    merged_config = std::make_shared<merged_config_t>();
    merged_config->creation_timestamp = jmutils::time::monotonic_now_sec();
    config_namespace->merged_config_by_overrides_key[overrides_key] = merged_config;
    config_namespace->merged_config_by_gc_generation[0].push_back(merged_config);
  }

  return merged_config;
}

std::shared_ptr<merged_config_t> get_merged_config(
  std::shared_ptr<config_namespace_t> config_namespace,
  const std::string& document,
  const std::string& overrides_key
) {
  // First we search if exists cached some mergd config using the overrides_key
  auto search = config_namespace->merged_config_by_overrides_key
    .find(overrides_key);

  if (search == config_namespace->merged_config_by_overrides_key.end()) {
    return nullptr;
  }

  // We use a weak pointer to free the old merged config so it's
  // possible that the obtained pointer is empty
  if (auto merged_config = search->second.lock()) {
    return merged_config;
  }

  // If the pointer is invalidated we drop the item to avoid
  // do this check in a future, I'm to lazy ;)
  config_namespace->merged_config_by_overrides_key
    .erase(search);

  return nullptr;
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

  auto override_search = document_metadata->override_by_key
    .find(override_);

  if (override_search == document_metadata->override_by_key.end()) {
    spdlog::trace("Don't exists the override '{}'", override_);
    return nullptr;
  }

  auto& raw_config_by_version = override_search->second
    .raw_config_by_version;

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


} /* builder */
} /* mhconfig */
