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
        .raw_config_by_version.emplace(1, std::move(result.raw_config));

      auto flavor_and_override = std::make_pair(
        std::move(result.flavor),
        std::move(result.override_)
      );
      updated_documents_by_flavor_and_override[flavor_and_override].emplace(
        std::move(result.document),
        AffectedDocumentStatus::TO_ADD
      );

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
  config_namespace->last_access_timestamp = jmutils::time::monotonic_now_sec();
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

  if (first_filename_char == '_') {
    result.document = path.filename().string();

    load_raw_config(
      path,
      [](const std::string& data, load_raw_config_result_t& result) {
        result.raw_config->template_ = std::make_shared<inja::Template>();
        result.raw_config->template_->content = data;

        {
          inja::ParserConfig parser_config;
          inja::LexerConfig lexer_config;
          inja::TemplateStorage included_templates;
          ForbiddenIncludeStrategy include_strategy;

          inja::Parser parser(
            parser_config,
            lexer_config,
            included_templates,
            include_strategy
          );

          parser.parse_into(*result.raw_config->template_, "");
        }
      },
      result
    );
  } else if (path.extension() == ".yaml") {
    auto stem = path.stem().string();
    auto pos = stem.find_first_of('.');
    if (pos == std::string::npos) {
      result.document = std::move(stem);
    } else {
      result.document.insert(0, stem, 0, pos);
      result.flavor.insert(0, stem, pos+1, stem.size()-pos-1);
    }

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
      if (it.second != AffectedDocumentStatus::TO_ADD) {
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

        auto new_raw_config = (it.second == AffectedDocumentStatus::TO_REMOVE || override_metadata.raw_config_by_version.empty())
          ? std::make_shared<raw_config_t>()
          : override_metadata.raw_config_by_version.rbegin()->second->clone();

        spdlog::debug(
          "Updating affected raw config (override_path: '{}', old_id: {}, new_id: {})",
          override_path,
          new_raw_config->id,
          config_namespace.next_raw_config_id
        );

        new_raw_config->id = config_namespace.next_raw_config_id++;

        override_metadata.raw_config_by_version.emplace(
          config_namespace.current_version,
          std::move(new_raw_config)
        );

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
        auto inserted = affected_documents.try_emplace(
          it.first,
          AffectedDocumentStatus::DEPENDENCY
        );
        if (inserted.second) {
          to_check.push_back(it.first);
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

  bool is_first_a_ref = a.type() == NodeType::REF_NODE;
  if (is_first_a_ref || (b.type() == NodeType::REF_NODE)) {
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
    case NodeType::STR_NODE: {
      NodeType type = get_virtual_node_type(a);
      if (type != NodeType::STR_NODE) {
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

    case NodeType::MAP_NODE: {
      if (!a.is_map()) {
        spdlog::warn(
          "Can't override {} with {} without the '{}' tag",
          a.repr(),
          b.repr(),
          TAG_OVERRIDE
        );
        return a;
      }

      auto map_box = new MapBox;
      auto map = map_box->get();
      auto map_a = a.as_map();
      auto map_b = b.as_map();
      map->reserve(map_a->size() + map_b->size());

      for (const auto& x : *map_a) {
        if (x.second.type() != NodeType::DELETE_NODE) {
          (*map)[x.first] = x.second;
        }
      }

      for (const auto& x : *map_b) {
        const auto& search = map->find(x.first);
        if (search == map->end()) {
          if (x.second.type() != NodeType::DELETE_NODE) {
            (*map)[x.first] = x.second;
          }
        } else if (x.second.type() == NodeType::DELETE_NODE) {
          map->erase(search);
        } else {
          (*map)[x.first] = override_with(
            search->second,
            x.second,
            elements_by_document
          );
        }
      }

      return Element(map_box);
    }

    case NodeType::SEQUENCE_NODE: {
      if (!a.is_sequence()) {
        spdlog::warn(
          "Can't override {} with {} without the '{}' tag",
          a.repr(),
          b.repr(),
          TAG_OVERRIDE
        );
        return a;
      }

      auto seq_box = new SequenceBox;
      auto seq = seq_box->get();
      auto seq_a = a.as_sequence();
      auto seq_b = b.as_sequence();
      seq->reserve(seq_a->size() + seq_b->size());

      for (size_t i = 0, l = seq_a->size(); i < l; ++i) {
        if ((*seq_a)[i].type() != NodeType::DELETE_NODE) {
          seq->push_back((*seq_a)[i]);
        }
      }

      for (size_t i = 0, l = seq_b->size(); i < l; ++i) {
        if ((*seq_b)[i].type() != NodeType::DELETE_NODE) {
          seq->push_back((*seq_b)[i]);
        }
      }

      return Element(seq_box);
    }
  }

  spdlog::warn("Can't override {} with {}", a.repr(), b.repr());
  return a;
}

NodeType get_virtual_node_type(
  const Element& element
) {
  auto type = element.type();
  switch (type) {
    case NodeType::FORMAT_NODE: // Fallback
    case NodeType::SREF_NODE: // Fallback
    case NodeType::NULL_NODE: // Fallback
    case NodeType::STR_NODE: // Fallback
    case NodeType::INT_NODE: // Fallback
    case NodeType::FLOAT_NODE: // Fallback
    case NodeType::BOOL_NODE: // Fallback
      return NodeType::STR_NODE;
    default:
      break;
  }

  return type;
}

bool apply_tags(
  jmutils::string::Pool* pool,
  const Element& element,
  const Element& root,
  const absl::flat_hash_map<std::string, Element> &elements_by_document,
  Element& result
) {
  bool any_changed = false;

  switch (element.type()) {
    case NodeType::MAP_NODE: // Fallback
    case NodeType::OVERRIDE_MAP_NODE: {
      auto map_box = new MapBox;
      auto map = map_box->get();
      map->reserve(element.as_map()->size());

      for (const auto& it : *element.as_map()) {
        if (it.second.type() == NodeType::DELETE_NODE) {
          any_changed = true;
        } else {
          auto changed = apply_tags(
            pool,
            it.second,
            root,
            elements_by_document,
            (*map)[it.first]
          );
          any_changed |= changed;
        }
      }

      if (any_changed) {
        result = Element(map_box, element.type());
      } else {
        delete map_box;
        result = element;
      }
      break;
    }

    case NodeType::SEQUENCE_NODE: // Fallback
    case NodeType::FORMAT_NODE: // Fallback
    case NodeType::SREF_NODE: // Fallback
    case NodeType::REF_NODE: // Fallback
    case NodeType::OVERRIDE_SEQUENCE_NODE: {
      auto seq_box = new SequenceBox;
      auto seq = seq_box->get();
      auto current_sequence = element.as_sequence();
      seq->reserve(current_sequence->size());

      for (size_t i = 0, l = current_sequence->size(); i < l; ++i) {
        if ((*current_sequence)[i].type() == NodeType::DELETE_NODE) {
          any_changed = true;
        } else {
          bool changed = apply_tags(
            pool,
            (*current_sequence)[i],
            root,
            elements_by_document,
            seq->emplace_back()
          );
          any_changed |= changed;
        }
      }

      if (any_changed) {
        result = Element(seq_box, element.type());
      } else {
        delete seq_box;
        result = element;
      }
      break;
    }
    default:
      result = element;
  }

  if (result.type() == NodeType::SREF_NODE) {
    result = apply_tag_sref(result, root);
    any_changed = true;
  }

  if (result.type() == NodeType::REF_NODE) {
    result = apply_tag_ref(result, elements_by_document);
    any_changed = true;
  }

  if (result.type() == NodeType::FORMAT_NODE) {
    result = apply_tag_format(pool, result);
    any_changed = true;
  }

  return any_changed;
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

  if (element.type() == NodeType::REF_NODE) {
    const auto path = element.as_sequence();
    if (!is_a_valid_path(path, TAG_REF)) return Element();
    reference_to.insert(path->front().as<std::string>());
  } else if (element.type() == NodeType::SREF_NODE) {
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
      if ((node.Tag() == "") || (node.Tag() == TAG_NULL)) {
        return Element(NodeType::NULL_NODE);
      } else if (node.Tag() == TAG_OVERRIDE) {
        return Element(NodeType::OVERRIDE_NULL_NODE);
      }
      spdlog::error("Unknown tag '{}' for a null value", node.Tag());
      return Element();

    case YAML::NodeType::Scalar:
      if ((node.Tag() == TAG_PLAIN_SCALAR) || (node.Tag() == TAG_NO_PLAIN_SCALAR)) {
        return Element(pool->add(node.as<std::string>()));
      } else if (node.Tag() == TAG_STR) {
        return Element(pool->add(node.as<std::string>()));
      } else if (node.Tag() == TAG_INT) {
        auto str{node.as<std::string>()};
        errno = 0;
        int64_t value = std::strtoll(str.c_str(), nullptr, 10);
        if (errno == 0) return Element(value);
        spdlog::warn("Can't parse '{}' as a int", str);
        return Element();
      } else if (node.Tag() == TAG_FLOAT) {
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
        return Element(NodeType::DELETE_NODE);
      } else if (node.Tag() == TAG_OVERRIDE) {
        return Element(pool->add(node.as<std::string>()), true);
      }
      spdlog::error(
        "Unknown tag '{}' for the scalar value {}",
        node.Tag(),
        node.as<std::string>()
      );
      return Element();

    case YAML::NodeType::Sequence: {
      auto seq_box = new SequenceBox;
      auto seq = seq_box->get();
      seq->reserve(node.size());
      for (auto it : node) {
        seq->push_back(make_and_check_element(pool, it, reference_to));
      }
      if ((node.Tag() == TAG_PLAIN_SCALAR) || (node.Tag() == TAG_NO_PLAIN_SCALAR)) {
        return Element(seq_box);
      } else if (node.Tag() == TAG_FORMAT) {
        return Element(seq_box, NodeType::FORMAT_NODE);
      } else if (node.Tag() == TAG_SREF) {
        return Element(seq_box, NodeType::SREF_NODE);
      } else if (node.Tag() == TAG_REF) {
        return Element(seq_box, NodeType::REF_NODE);
      } else if (node.Tag() == TAG_OVERRIDE) {
        return Element(seq_box, NodeType::OVERRIDE_SEQUENCE_NODE);
      }
      delete seq_box;
      spdlog::error("Unknown tag '{}' for a sequence value", node.Tag());
      return Element();
    }

    case YAML::NodeType::Map: {
      auto map_box = new MapBox;
      auto map = map_box->get();
      map->reserve(node.size());
      for (auto it : node) {
        auto k = make_and_check_element(pool, it.first, reference_to);
        auto kk = k.try_as<jmutils::string::String>();
        if (!kk.first) {
          delete map_box;
          spdlog::error("The key of a map must be a string");
          return Element();
        }

        (*map)[std::move(kk.second)] = make_and_check_element(
          pool,
          it.second,
          reference_to
        );
      }
      if ((node.Tag() == TAG_PLAIN_SCALAR) || (node.Tag() == TAG_NO_PLAIN_SCALAR)) {
        return Element(map_box);
      } else if (node.Tag() == TAG_OVERRIDE) {
        return Element(map_box, NodeType::OVERRIDE_MAP_NODE);
      }
      delete map_box;
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
    merged_config->creation_timestamp = jmutils::time::monotonic_now_sec();
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

bool is_a_valid_document_name(const std::string& document) {
  if (document.empty()) return true;
  if (document[0] == '_') return false;
  for (auto c: document) {
    if (c == '.') return false;
  }
  return true;
}

bool has_last_version(
  const override_metadata_t& override_metadata
) {
  if (override_metadata.raw_config_by_version.empty()) {
    return false;
  }

  return override_metadata.raw_config_by_version.crbegin()->second != nullptr;
}

} /* builder */
} /* mhconfig */
