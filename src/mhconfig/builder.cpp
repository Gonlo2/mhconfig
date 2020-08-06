#include "mhconfig/builder.h"

namespace mhconfig
{
namespace builder
{

// Setup logic

std::shared_ptr<config_namespace_t> make_config_namespace(
  const std::filesystem::path& root_path,
  metrics::MetricsService& metrics
) {
  auto config_namespace = std::make_shared<config_namespace_t>();
  config_namespace->ok = false;
  config_namespace->root_path = root_path;
  config_namespace->id = std::uniform_int_distribution<uint64_t>{0, 0xffffffffffffffff}(jmutils::prng_engine());
  config_namespace->last_access_timestamp = 0;
  config_namespace->pool = std::make_shared<jmutils::string::Pool>(
    std::make_unique<::mhconfig::string_pool::MetricsStatsObserver>(metrics, root_path)
  );

  spdlog::debug("To index the files in the path '{}'", root_path.string());

  absl::flat_hash_map<
    std::pair<std::string, std::string>,
    absl::flat_hash_map<std::string, AffectedDocumentStatus>
  > updated_documents_by_flavor_and_override;

  bool ok = index_files(
    config_namespace->pool.get(),
    root_path,
    [&config_namespace, &updated_documents_by_flavor_and_override](const auto& override_path, auto&& result) {
      if (result.status != LoadRawConfigStatus::OK) {
        return false;
      }

      for (const auto& reference_to : result.raw_config->reference_to) {
        config_namespace->referenced_by_by_document[reference_to][result.document].v += 1;
      }

      result.raw_config->id = config_namespace->next_raw_config_id++;

      config_namespace->override_metadata_by_override_path[override_path]
        .raw_config_by_version[1] = std::move(result.raw_config);

      auto flavor_and_override = std::make_pair(
        std::move(result.flavor),
        std::move(result.override_)
      );
      updated_documents_by_flavor_and_override[flavor_and_override][result.document] = AffectedDocumentStatus::TO_ADD;

      return true;
    }
  );

  if (!ok) {
    return config_namespace;
  }

  spdlog::debug("Setting the ids of the nonexistent affected documents");

  absl::flat_hash_set<std::shared_ptr<api::stream::WatchInputMessage>> watchers_to_trigger;
  increment_version_of_the_affected_documents(
    *config_namespace,
    updated_documents_by_flavor_and_override,
    watchers_to_trigger,
    true
  );

  config_namespace->ok = true;
  config_namespace->last_access_timestamp = jmutils::monotonic_now_sec();
  config_namespace->stored_versions_by_deprecation_timestamp.emplace_back(
    0,
    config_namespace->current_version
  );

  return config_namespace;
}

load_raw_config_result_t index_file(
  jmutils::string::Pool* pool,
  const std::filesystem::path& root_path,
  const std::filesystem::path& path
) {
  load_raw_config_result_t result;
  result.status = LoadRawConfigStatus::ERROR;

  auto first_filename_char = path.filename().native()[0];
  if (first_filename_char == '.') {
    result.status = LoadRawConfigStatus::INVALID_FILENAME;
    return result;
  }

  result.override_ = std::filesystem::relative(path, root_path)
    .parent_path()
    .string();

  auto stem = path.stem().string();
  auto split_result = split_filename(stem);
  if (!split_result.ok) {
    return result;
  }

  result.flavor = split_result.flavor;

  if (first_filename_char == '_') {
    result.document = split_result.kind;
    result.document += '.';
    result.document += split_result.name;
    result.document += path.extension().string();

    if (split_result.kind == "_bin") {
      load_raw_config(
        path,
        [pool](const std::string& data, load_raw_config_result_t& result) {
          result.raw_config->value = Element(pool->add(data), NodeType::BIN);
        },
        result
      );
    } else if (split_result.kind == "_text") {
      load_raw_config(
        path,
        [pool](const std::string& data, load_raw_config_result_t& result) {
          result.raw_config->value = Element(pool->add(data));
        },
        result
      );
    } else {
      result.status = LoadRawConfigStatus::INVALID_FILENAME;
    }
  } else if (path.extension() == ".yaml") {
    result.document = split_result.name;

    load_raw_config(
      path,
      [pool](const std::string& data, load_raw_config_result_t& result) {
        YAML::Node node = YAML::Load(data);

        absl::flat_hash_set<std::string> reference_to;
        result.raw_config->value = make_and_check_element(pool, node, reference_to);
        result.raw_config->reference_to.reserve(reference_to.size());
        for (const auto& x : reference_to) {
          result.raw_config->reference_to.push_back(x);
        }
      },
      result
    );
  } else {
    result.status = LoadRawConfigStatus::INVALID_FILENAME;
  }

  return result;
}

void increment_version_of_the_affected_documents(
  config_namespace_t& config_namespace,
  absl::flat_hash_map<std::pair<std::string, std::string>, absl::flat_hash_map<std::string, AffectedDocumentStatus>>& updated_documents_by_flavor_and_override,
  absl::flat_hash_set<std::shared_ptr<api::stream::WatchInputMessage>>& watchers_to_trigger,
  bool only_nonexistent
) {
  std::string override_path;
  for (auto& updated_documents_it : updated_documents_by_flavor_and_override) {
    fill_affected_documents(config_namespace, updated_documents_it.second);

    for (const auto& it : updated_documents_it.second) {
      bool is_to_remove_but_dependency = it.second == AffectedDocumentStatus::TO_REMOVE_BUT_DEPENDENCY;
      if (is_to_remove_but_dependency || (it.second == AffectedDocumentStatus::DEPENDENCY)) {
        make_override_path(
          updated_documents_it.first.second,
          it.first,
          updated_documents_it.first.first,
          override_path
        );
        auto& override_metadata = config_namespace.override_metadata_by_override_path[override_path];

        if (!override_metadata.raw_config_by_version.empty() && only_nonexistent) {
          continue;
        }

        auto new_raw_config = !is_to_remove_but_dependency && has_last_version(override_metadata)
          ? override_metadata.raw_config_by_version.crbegin()->second->clone()
          : std::make_shared<raw_config_t>();

        spdlog::debug(
          "Updating affected raw config (override_path: '{}', old_id: {}, new_id: {})",
          override_path,
          new_raw_config->id,
          config_namespace.next_raw_config_id
        );

        new_raw_config->id = config_namespace.next_raw_config_id++;

        override_metadata.raw_config_by_version[config_namespace.current_version] = std::move(new_raw_config);

        for (size_t i = 0; i < override_metadata.watchers.size();) {
          if (auto watcher = override_metadata.watchers[i].lock()) {
            watchers_to_trigger.insert(watcher);
            ++i;
          } else {
            jmutils::swap_delete(override_metadata.watchers, i);
          }
        }
      }
    }
  }
}

bool has_last_version(
  const override_metadata_t& override_metadata
) {
  if (override_metadata.raw_config_by_version.empty()) {
    return false;
  }

  auto ptr = override_metadata.raw_config_by_version.crbegin()->second.get();
  return (ptr != nullptr) && ptr->has_content;
}


void fill_affected_documents(
  const config_namespace_t& config_namespace,
  absl::flat_hash_map<std::string, AffectedDocumentStatus>& affected_documents
) {
  std::vector<std::string> to_check;
  to_check.reserve(affected_documents.size());
  for (const auto& it : affected_documents) {
    to_check.push_back(it.first);
  }

  std::string doc;
  while (!to_check.empty()) {
    doc = std::move(to_check.back());
    to_check.pop_back();

    auto search = config_namespace.referenced_by_by_document.find(doc);
    if (search != config_namespace.referenced_by_by_document.end()) {
      for (const auto& it : search->second) {
        auto inserted = affected_documents.emplace(
          it.first,
          AffectedDocumentStatus::DEPENDENCY
        );
        if (inserted.second) {
          to_check.push_back(it.first);
        } else if (inserted.first->second == AffectedDocumentStatus::TO_REMOVE) {
          inserted.first->second = AffectedDocumentStatus::TO_REMOVE_BUT_DEPENDENCY;
        }
      }
    }
  }
}

Element override_with(
  const Element& a,
  const Element& b,
  const absl::flat_hash_map<std::string, Element> &elements_by_document
) {
  if (b.is_override()) {
    return b.clone_without_virtual();
  }

  bool is_first_a_ref = a.type() == NodeType::REF;
  if (is_first_a_ref || (b.type() == NodeType::REF)) {
    auto referenced_element = apply_tag_ref(
      is_first_a_ref ? a : b,
      elements_by_document
    );

    return override_with(
      is_first_a_ref ? referenced_element : a,
      is_first_a_ref ? b : referenced_element,
      elements_by_document
    );
  }

  switch (get_virtual_node_type(b)) {
    case VirtualNode::LITERAL: {
      VirtualNode type = get_virtual_node_type(a);
      if (type != VirtualNode::LITERAL) {
        spdlog::warn(
          "Can't override {} with {} without the '{}' tag",
          a.repr(),
          b.repr(),
          TAG_OVERRIDE
        );
        return a;
      }

      return b;
    }

    case VirtualNode::MAP: {
      if (!a.is_map()) {
        spdlog::warn(
          "Can't override {} with {} without the '{}' tag",
          a.repr(),
          b.repr(),
          TAG_OVERRIDE
        );
        return a;
      }

      Element result(a);
      auto map_a = result.as_map_mut();
      auto map_b = b.as_map();
      map_a->reserve(map_a->size() + map_b->size());

      for (const auto& x : *map_b) {
        const auto& search = map_a->find(x.first);
        if (search == map_a->end()) {
          if (x.second.type() != NodeType::DELETE) {
            (*map_a)[x.first] = x.second;
          }
        } else if (x.second.type() == NodeType::DELETE) {
          map_a->erase(search);
        } else if (search->second.type() == NodeType::DELETE) {
          (*map_a)[x.first] = x.second;
        } else {
          (*map_a)[x.first] = override_with(
            search->second,
            x.second,
            elements_by_document
          );
        }
      }

      return result;
    }

    case VirtualNode::SEQUENCE: {
      if (!a.is_sequence()) {
        spdlog::warn(
          "Can't override {} with {} without the '{}' tag",
          a.repr(),
          b.repr(),
          TAG_OVERRIDE
        );
        return a;
      }

      Element result(a);
      auto seq_a = result.as_sequence_mut();
      auto seq_b = b.as_sequence();
      seq_a->reserve(seq_a->size() + seq_b->size());

      for (size_t i = 0, l = seq_b->size(); i < l; ++i) {
        seq_a->push_back((*seq_b)[i]);
      }

      return result;
    }
  }

  spdlog::warn("Can't override {} with {}", a.repr(), b.repr());
  return a;
}

VirtualNode get_virtual_node_type(
  const Element& element
) {
  auto type = element.type();
  switch (type) {
    case NodeType::UNDEFINED:
      return VirtualNode::UNDEFINED;
    case NodeType::MAP:
      return VirtualNode::MAP;
    case NodeType::SEQUENCE:
      return VirtualNode::SEQUENCE;
    case NodeType::REF:
      return VirtualNode::REF;
    case NodeType::FORMAT: // Fallback
    case NodeType::SREF: // Fallback
    case NodeType::NONE: // Fallback
    case NodeType::STR: // Fallback
    case NodeType::BIN: // Fallback
    case NodeType::INT: // Fallback
    case NodeType::DOUBLE: // Fallback
    case NodeType::BOOL: // Fallback
      return VirtualNode::LITERAL;
  }
  assert(false);
}

std::pair<bool, Element> apply_tags(
  jmutils::string::Pool* pool,
  Element element,
  const Element& root,
  const absl::flat_hash_map<std::string, Element> &elements_by_document
) {
  bool any_changed = false;

  switch (element.type()) {
    case NodeType::MAP: // Fallback
    case NodeType::OVERRIDE_MAP: {
      std::vector<jmutils::string::String> to_remove;
      std::vector<std::pair<jmutils::string::String, Element>> to_modify;

      for (const auto& it : *element.as_map()) {
        if (it.second.type() == NodeType::DELETE) {
          to_remove.push_back(it.first);
        } else {
          auto r = apply_tags(
            pool,
            it.second,
            root,
            elements_by_document
          );
          if (r.first) {
            to_modify.emplace_back(it.first, r.second);
          }
        }
      }

      any_changed = !(to_remove.empty() && to_modify.empty());
      if (any_changed) {
        auto map = element.as_map_mut();

        for (size_t i = 0, l = to_remove.size(); i < l; ++i) {
          map->erase(to_remove[i]);
        }

        for (size_t i = 0, l = to_modify.size(); i < l; ++i) {
          (*map)[to_modify[i].first] = std::move(to_modify[i].second);
        }
      }
      break;
    }

    case NodeType::SEQUENCE: // Fallback
    case NodeType::FORMAT: // Fallback
    case NodeType::SREF: // Fallback
    case NodeType::REF: // Fallback
    case NodeType::OVERRIDE_SEQUENCE: {
      std::vector<Element> new_sequence;
      auto current_sequence = element.as_sequence();
      new_sequence.reserve(current_sequence->size());

      for (size_t i = 0, l = current_sequence->size(); i < l; ++i) {
        if ((*current_sequence)[i].type() == NodeType::DELETE) {
          any_changed = true;
        } else {
          auto r = apply_tags(
            pool,
            (*current_sequence)[i],
            root,
            elements_by_document
          );
          any_changed |= r.first;
          new_sequence.push_back(r.second);
        }
      }

      if (any_changed) {
        auto sequence = element.as_sequence_mut();
        std::swap(*sequence, new_sequence);
      }
      break;
    }
  }

  if (element.type() == NodeType::SREF) {
    element = apply_tag_sref(element, root);
    any_changed = true;
  }

  if (element.type() == NodeType::REF) {
    element = apply_tag_ref(element, elements_by_document);
    any_changed = true;
  }

  if (element.type() == NodeType::FORMAT) {
    element = apply_tag_format(pool, element);
    any_changed = true;
  }

  return std::make_pair(any_changed, element);
}

Element apply_tag_format(
  jmutils::string::Pool* pool,
  const Element& element
) {
  if (!element.is_sequence() || (element.as_sequence()->size() != 2)) {
    spdlog::error(
      "The structure with the tag '{}' must be a sequence of size 2",
      TAG_FORMAT
    );
    return Element();
  }

  auto& template_node = (*element.as_sequence())[0];
  auto template_result = template_node.try_as<std::string>();
  if (!template_node.is_string() || !template_result.first) {
    spdlog::error(
      "The '{}' tag first argument must be a template",
      TAG_FORMAT
    );
    return Element();
  }

  auto& arguments_node = (*element.as_sequence())[1];
  if (!arguments_node.is_map()) {
    spdlog::error(
      "The '{}' tag second argument must be a map [string -> scalar]",
      TAG_FORMAT
    );
    return Element();
  }

  std::vector<std::pair<std::string, std::string>> template_arguments;
  for (const auto& it : *arguments_node.as_map()) {
    auto r = it.second.try_as<std::string>();
    if (!it.second.is_scalar() || !r.first) {
      spdlog::error(
        "The '{}' tag second argument must be a map [string -> scalar]",
        TAG_FORMAT
      );
      return Element();
    }

    template_arguments.emplace_back(it.first.str(), r.second);
  }

  if (template_arguments.size() > 8) {
    spdlog::error(
      "The '{}' tag can't handle more that 8 arguments",
      TAG_FORMAT
    );
    return Element();
  }

  try {
    std::string value = format_str(template_result.second, template_arguments);
    return Element(pool->add(value));
  } catch (const std::exception &e) {
    spdlog::error(
      "Some error take place formating the template '{}': {}",
      template_result.second,
      e.what()
    );
  } catch (...) {
    spdlog::error(
      "Some unknown error take place formating the template '{}'",
      template_result.second
    );
  }

  return Element();
}

std::string format_str(
  const std::string& templ,
  const std::vector<std::pair<std::string, std::string>>& template_arguments
) {
  switch (template_arguments.size()) {
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

Element apply_tag_ref(
  const Element& element,
  const absl::flat_hash_map<std::string, Element> &elements_by_document
) {
  const auto path = element.as_sequence();

  std::string key = (*path)[0].as<std::string>();
  auto search = elements_by_document.find(key);
  if (search == elements_by_document.end()) {
    spdlog::error("Can't ref to the document '{}'", key);
    return Element();
  }

  auto referenced_element = search->second;
  for (size_t i = 1, l = path->size(); i < l; ++i) {
    referenced_element = referenced_element.get(
      (*path)[i].as<jmutils::string::String>()
    );
  }

  return referenced_element;
}

Element apply_tag_sref(
  const Element& element,
  Element root
) {
  const auto path = element.as_sequence();
  for (size_t i = 0, l = path->size(); i < l; ++i) {
    root = root.get((*path)[i].as<jmutils::string::String>());
  }

  if (!root.is_scalar()) {
    spdlog::error(
      "The element referenced by '{}' must be a scalar",
      TAG_SREF
    );
    return Element();
  }

  return root;
}

/*
 * All the structure checks must be done here
 */
Element make_and_check_element(
  jmutils::string::Pool* pool,
  YAML::Node &node,
  absl::flat_hash_set<std::string> &reference_to
) {
  auto element = make_element(pool, node, reference_to);

  if (element.type() == NodeType::REF) {
    const auto path = element.as_sequence();
    if (!is_a_valid_path(path, TAG_REF)) return Element();
    reference_to.insert(path->front().as<std::string>());
  } else if (element.type() == NodeType::SREF) {
    if (!is_a_valid_path(element.as_sequence(), TAG_SREF)) return Element();
  }

  return element;
}

bool is_a_valid_path(
  const Sequence* path,
  const std::string& tag
) {
  if (path->empty()) {
    spdlog::error(
      "The key 'path' in a '{}' must be a sequence with at least one element",
      tag
    );
    return false;
  }

  for (size_t i = 0, l = path->size(); i < l; ++i) {
    if (!(*path)[i].is_string()) {
      spdlog::error(
        "All the elements of the key 'path' in a '{}' must be strings",
        tag
      );
      return false;
    }
  }

  return true;
}

Element make_element(
  jmutils::string::Pool* pool,
  YAML::Node &node,
  absl::flat_hash_set<std::string> &reference_to
) {
  switch (node.Type()) {
    case YAML::NodeType::Null:
      if ((node.Tag() == "") || (node.Tag() == TAG_NONE)) {
        return Element(NodeType::NONE);
      } else if (node.Tag() == TAG_OVERRIDE) {
        return Element(NodeType::OVERRIDE_NONE);
      }
      spdlog::error("Unknown tag '{}' for a null value", node.Tag());
      return Element();

    case YAML::NodeType::Scalar:
      if ((node.Tag() == TAG_PLAIN_SCALAR) || (node.Tag() == TAG_NO_PLAIN_SCALAR)) {
        return Element(pool->add(node.as<std::string>()));
      } else if (node.Tag() == TAG_STR) {
        return Element(pool->add(node.as<std::string>()));
      } else if (node.Tag() == TAG_BIN) {
        std::string encoded_value = node.as<std::string>();

        if (!jmutils::base64_sanitize(encoded_value)) {
          spdlog::warn("The base64 '{}' don't have a valid structure", encoded_value);
          return Element();
        }

        std::string binary_value;
        jmutils::base64_decode(encoded_value, binary_value);
        return Element(pool->add(binary_value), NodeType::BIN);
      } else if (node.Tag() == TAG_INT) {
        auto str{node.as<std::string>()};
        errno = 0;
        int64_t value = std::strtoll(str.c_str(), nullptr, 10);
        if (errno == 0) return Element(value);
        spdlog::warn("Can't parse '{}' as a int", str);
        return Element();
      } else if (node.Tag() == TAG_DOUBLE) {
        auto str{node.as<std::string>()};
        errno = 0;
        double value = std::strtod(str.c_str(), nullptr);
        if (errno == 0) return Element(value);
        spdlog::warn("Can't parse '{}' as a float", str);
        return Element();
      } else if (node.Tag() == TAG_BOOL) {
        auto str{node.as<std::string>()};
        if (str == "true") {
          return Element(true);
        } else if (str == "false") {
          return Element(false);
        }
        spdlog::warn("Can't parse '{}' as a bool", str);
        return Element();
      } else if (node.Tag() == TAG_DELETE) {
        return Element(NodeType::DELETE);
      } else if (node.Tag() == TAG_OVERRIDE) {
        return Element(pool->add(node.as<std::string>()), NodeType::OVERRIDE_STR);
      }
      spdlog::error(
        "Unknown tag '{}' for the scalar value {}",
        node.Tag(),
        node.as<std::string>()
      );
      return Element();

    case YAML::NodeType::Sequence: {
      SequenceCow seq_cow;
      auto seq = seq_cow.get_mut();
      seq->reserve(node.size());
      for (auto it : node) {
        seq->push_back(make_and_check_element(pool, it, reference_to));
      }
      if ((node.Tag() == TAG_PLAIN_SCALAR) || (node.Tag() == TAG_NO_PLAIN_SCALAR)) {
        return Element(std::move(seq_cow));
      } else if (node.Tag() == TAG_FORMAT) {
        return Element(std::move(seq_cow), NodeType::FORMAT);
      } else if (node.Tag() == TAG_SREF) {
        return Element(std::move(seq_cow), NodeType::SREF);
      } else if (node.Tag() == TAG_REF) {
        return Element(std::move(seq_cow), NodeType::REF);
      } else if (node.Tag() == TAG_OVERRIDE) {
        return Element(std::move(seq_cow), NodeType::OVERRIDE_SEQUENCE);
      }
      spdlog::error("Unknown tag '{}' for a sequence value", node.Tag());
      return Element();
    }

    case YAML::NodeType::Map: {
      MapCow map_cow;
      auto map = map_cow.get_mut();
      map->reserve(node.size());
      for (auto it : node) {
        auto k = make_and_check_element(pool, it.first, reference_to);
        auto kk = k.try_as<jmutils::string::String>();
        if (!kk.first) {
          spdlog::error("The key of a map must be a string");
          return Element();
        }
        (*map)[kk.second] = make_and_check_element(pool, it.second, reference_to);
      }
      if ((node.Tag() == TAG_PLAIN_SCALAR) || (node.Tag() == TAG_NO_PLAIN_SCALAR)) {
        return Element(std::move(map_cow));
      } else if (node.Tag() == TAG_OVERRIDE) {
        return Element(std::move(map_cow), NodeType::OVERRIDE_MAP);
      }
      spdlog::error("Unknown tag '{}' for a map value", node.Tag());
      return Element();
    }
  }

  return Element();
}

// Get logic

std::shared_ptr<merged_config_t> get_or_build_merged_config(
  config_namespace_t& config_namespace,
  const std::string& overrides_key
) {
  auto merged_config = get_merged_config(
    config_namespace,
    overrides_key
  );
  if (merged_config == nullptr) {
    merged_config = std::make_shared<merged_config_t>();
    merged_config->creation_timestamp = jmutils::monotonic_now_sec();
    config_namespace.merged_config_by_overrides_key[overrides_key] = merged_config;
    config_namespace.merged_config_by_gc_generation[0].push_back(merged_config);
  }

  return merged_config;
}

std::shared_ptr<merged_config_t> get_merged_config(
  config_namespace_t& config_namespace,
  const std::string& overrides_key
) {
  // First we search if exists cached some mergd config using the overrides_key
  auto search = config_namespace.merged_config_by_overrides_key
    .find(overrides_key);

  if (search == config_namespace.merged_config_by_overrides_key.end()) {
    return nullptr;
  }

  // We use a weak pointer to free the old merged config so it's
  // possible that the obtained pointer is empty
  if (auto merged_config = search->second.lock()) {
    return merged_config;
  }

  // If the pointer is invalidated we drop the item to avoid
  // do this check in a future, I'm to lazy ;)
  config_namespace.merged_config_by_overrides_key
    .erase(search);

  return nullptr;
}

split_filename_result_t split_filename(
  std::string_view stem
) {
  split_filename_result_t result;
  result.ok = false;

  if (stem.empty()) {
    result.ok = true;
    return result;
  }

  if (stem[0] == '_') {
    auto pos = stem.find('.');
    if (pos == std::string::npos) {
      return result;
    }
    result.kind = std::string_view(stem.data(), pos);
    stem.remove_prefix(pos+1);
  }

  auto pos = stem.find('.');
  if (pos == std::string::npos) {
    result.name = stem;
  } else {
    result.name = std::string_view(stem.data(), pos);
    stem.remove_prefix(pos+1);
    result.flavor = stem;
  }

  result.ok = true;
  return result;
}


} /* builder */
} /* mhconfig */
