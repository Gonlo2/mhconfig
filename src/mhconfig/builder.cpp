#include "mhconfig/builder.h"

namespace mhconfig
{

// Setup logic

bool init_config_namespace(
  config_namespace_t* cn,
  std::shared_ptr<jmutils::string::Pool>&& pool
) {
  cn->id = std::uniform_int_distribution<uint64_t>{0, 0xffffffffffffffff}(jmutils::prng_engine());
  cn->last_access_timestamp = 0;
  cn->pool = std::move(pool);

  absl::flat_hash_map<
    std::string,
    absl::flat_hash_map<std::string, AffectedDocumentStatus>
  > updated_documents_by_path;

  bool ok = index_files(
    cn->pool.get(),
    cn->root_path,
    [cn, &updated_documents_by_path](const auto& path, auto&& result) {
      if (result.status != LoadRawConfigStatus::OK) {
        return false;
      }

      cn->mutex.Lock();
      for (const auto& reference_to : result.raw_config->reference_to) {
        get_or_guild_document_versions_locked(cn, reference_to)->referenced_by[result.document].v += 1;
      }
      cn->mutex.Unlock();

      auto document = try_obtain_non_full_document(cn, result.document, 1);
      if (document == nullptr) return false;

      result.raw_config->id = document->next_raw_config_id++;
      document->override_by_path[path].raw_config_by_version[1] = std::move(result.raw_config);
      document->mutex.Unlock();

      updated_documents_by_path[path][result.document] = AffectedDocumentStatus::TO_ADD;

      return true;
    }
  );

  if (!ok) return false;

  spdlog::debug("Setting the ids of the nonexistent affected documents");

  auto dep_by_doc = get_dep_by_doc(cn, updated_documents_by_path);

  if (!touch_affected_documents(cn, 1, dep_by_doc, true)) {
    return false;
  }

  cn->last_access_timestamp = jmutils::monotonic_now_sec();
  cn->stored_versions.emplace_back(0, cn->current_version);

  return true;
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
        result.raw_config->value = make_and_check_element(
          pool,
          node,
          result.document,
          reference_to
        );
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

absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, AffectedDocumentStatus>> get_dep_by_doc(
  config_namespace_t* cn,
  absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, AffectedDocumentStatus>>& updated_documents_by_path
) {
  absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, AffectedDocumentStatus>> dep_by_doc;

  for (auto& it : updated_documents_by_path) {
    fill_affected_documents(cn, it.second);

    for (const auto& it2 : it.second) {
      dep_by_doc[it2.first][it.first] = it2.second;
    }
  }

  return dep_by_doc;
}

bool touch_affected_documents(
  config_namespace_t* cn,
  VersionId version,
  const absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, AffectedDocumentStatus>>& dep_by_doc,
  bool only_nonexistent
) {
  for (const auto& it : dep_by_doc) {
    auto document = try_obtain_non_full_document(cn, it.first, version, it.second.size());
    if (document == nullptr) {
      spdlog::debug("Some error happens obtaining a non full document for '{}'", it.first);
      return false;
    }

    for (const auto& it2 : it.second) {
      if (
        (it2.second == AffectedDocumentStatus::TO_REMOVE_BUT_DEPENDENCY)
        || (it2.second == AffectedDocumentStatus::DEPENDENCY)
      ) {
        auto& override_ = document->override_by_path[it2.first];
        if (override_.raw_config_by_version.empty() || !only_nonexistent) {
          auto last_version = get_last_raw_config_locked(override_);
          auto new_raw_config = (last_version != nullptr) && (it2.second != AffectedDocumentStatus::TO_REMOVE_BUT_DEPENDENCY)
            ? last_version->clone()
            : std::make_shared<raw_config_t>();

          spdlog::debug(
            "Updating affected raw config (document: '{}', path: '{}', version: {}, old_id: {}, new_id: {})",
            it.first,
            it2.first,
            version,
            last_version == nullptr ? 0 : last_version->id,
            document->next_raw_config_id
          );

          new_raw_config->id = document->next_raw_config_id++;

          override_.raw_config_by_version[version] = std::move(new_raw_config);
        }
      }
    }
    document->mutex.Unlock();
  }

  return true;
}

void fill_affected_documents(
  config_namespace_t* cn,
  absl::flat_hash_map<std::string, AffectedDocumentStatus>& affected_documents
) {
  std::vector<std::string> to_check;
  to_check.reserve(affected_documents.size());
  for (const auto& it : affected_documents) {
    to_check.push_back(it.first);
  }

  cn->mutex.ReaderLock();
  while (!to_check.empty()) {
    if (
      auto search = cn->document_versions_by_name.find(to_check.back());
      search != cn->document_versions_by_name.end()
    ) {
      search->second->mutex.ReaderLock();
      for (const auto& it : search->second->referenced_by) {
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
      search->second->mutex.ReaderUnlock();
    }
    to_check.pop_back();
  }
  cn->mutex.ReaderUnlock();
}

Element override_with(
  const Element& a,
  const Element& b,
  const absl::flat_hash_map<std::string, Element> &element_by_document_name
) {
  if (b.is_override()) {
    return b.clone_without_virtual();
  }

  bool is_first_a_ref = a.type() == NodeType::REF;
  if (is_first_a_ref || (b.type() == NodeType::REF)) {
    auto referenced_element = apply_tag_ref(
      is_first_a_ref ? a : b,
      element_by_document_name
    );

    return override_with(
      is_first_a_ref ? referenced_element : a,
      is_first_a_ref ? b : referenced_element,
      element_by_document_name
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
            element_by_document_name
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
    case NodeType::INT64: // Fallback
    case NodeType::DOUBLE: // Fallback
    case NodeType::BOOL:
      return VirtualNode::LITERAL;
  }
  assert(false);
}

std::pair<bool, Element> apply_tags(
  jmutils::string::Pool* pool,
  Element element,
  const Element& root,
  const absl::flat_hash_map<std::string, Element> &element_by_document_name
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
            element_by_document_name
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
            element_by_document_name
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
    element = apply_tag_ref(element, element_by_document_name);
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
  const absl::flat_hash_map<std::string, Element> &element_by_document_name
) {
  const auto path = element.as_sequence();

  std::string key = (*path)[0].as<std::string>();
  auto search = element_by_document_name.find(key);
  if (search == element_by_document_name.end()) {
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
  const std::string& document,
  absl::flat_hash_set<std::string> &reference_to
) {
  auto element = make_element(pool, node, document, reference_to);

  if (element.type() == NodeType::REF) {
    const auto path = element.as_sequence();
    if (!is_a_valid_path(path, TAG_REF)) return Element();
    auto ref_document = path->front().as<std::string>();
    if (document == ref_document) {
      spdlog::error("A reference can't use the same document");
      return Element();
    } else {
      reference_to.insert(ref_document);
    }
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
  const std::string& document,
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
      return make_element_from_scalar(pool, node);

    case YAML::NodeType::Sequence:
      return make_element_from_sequence(pool, node, document, reference_to);

    case YAML::NodeType::Map:
      return make_element_from_map(pool, node, document, reference_to);
  }

  return Element();
}

Element make_element_from_scalar(
  jmutils::string::Pool* pool,
  YAML::Node &node
) {
  if ((node.Tag() == TAG_NON_PLAIN_SCALAR) || (node.Tag() == TAG_STR)) {
    return Element(pool->add(node.as<std::string>()));
  } else if (node.Tag() == TAG_PLAIN_SCALAR) {
    return make_element_from_plain_scalar(pool, node);
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
    Element result = make_element_from_int64(node);
    if (result.is_undefined()) {
      spdlog::warn("Can't parse '{}' as a int64", node.as<std::string>());
    }
    return result;
  } else if (node.Tag() == TAG_DOUBLE) {
    Element result = make_element_from_double(node);
    if (result.is_undefined()) {
      spdlog::warn("Can't parse '{}' as a double", node.as<std::string>());
    }
    return result;
  } else if (node.Tag() == TAG_BOOL) {
    Element result = make_element_from_bool(node);
    if (result.is_undefined()) {
      spdlog::warn("Can't parse '{}' as a bool", node.as<std::string>());
    }
    return result;
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
}

Element make_element_from_plain_scalar(
  jmutils::string::Pool* pool,
  YAML::Node &node
) {
  Element e = make_element_from_bool(node);
  if (!e.is_undefined()) return e;

  e = make_element_from_int64(node);
  if (!e.is_undefined()) return e;

  e = make_element_from_double(node);
  if (!e.is_undefined()) return e;

  return Element(pool->add(node.as<std::string>()));
}

Element make_element_from_int64(
  YAML::Node &node
) {
  auto str(node.as<std::string>());
  char *end;
  errno = 0;
  int64_t value = std::strtoll(str.c_str(), &end, 10);
  return (errno || (str.c_str() == end) || (*end != 0))
    ? Element()
    : Element(value);
}

Element make_element_from_double(
  YAML::Node &node
) {
  auto str(node.as<std::string>());
  char *end;
  errno = 0;
  double value = std::strtod(str.c_str(), &end);
  return (errno || (str.c_str() == end) || (*end != 0))
    ? Element()
    : Element(value);
}

Element make_element_from_bool(
  YAML::Node &node
) {
  auto str(node.as<std::string>());
  if (str == "true") {
    return Element(true);
  } else if (str == "false") {
    return Element(false);
  }
  return Element();
}

Element make_element_from_map(
  jmutils::string::Pool* pool,
  YAML::Node &node,
  const std::string& document,
  absl::flat_hash_set<std::string> &reference_to
) {
  MapCow map_cow;
  auto map = map_cow.get_mut();
  map->reserve(node.size());
  for (auto it : node) {
    if (!it.first.IsScalar()) {
      spdlog::error("The key of a map must be a scalar");
      return Element();
    }
    jmutils::string::String key(pool->add(it.first.as<std::string>()));
    (*map)[key] = make_and_check_element(pool, it.second, document, reference_to);
  }
  if (node.Tag() == TAG_PLAIN_SCALAR) {
    return Element(std::move(map_cow));
  } else if (node.Tag() == TAG_OVERRIDE) {
    return Element(std::move(map_cow), NodeType::OVERRIDE_MAP);
  }
  spdlog::error("Unknown tag '{}' for a map value", node.Tag());
  return Element();
}

Element make_element_from_sequence(
  jmutils::string::Pool* pool,
  YAML::Node &node,
  const std::string& document,
  absl::flat_hash_set<std::string> &reference_to
) {
  SequenceCow seq_cow;
  auto seq = seq_cow.get_mut();
  seq->reserve(node.size());
  for (auto it : node) {
    seq->push_back(make_and_check_element(pool, it, document, reference_to));
  }
  if (node.Tag() == TAG_PLAIN_SCALAR) {
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

// Get logic

std::shared_ptr<document_t> get_document_locked(
  const config_namespace_t* cn,
  const std::string& name,
  VersionId version
) {
  if (
    auto versions_search = cn->document_versions_by_name.find(name);
    versions_search != cn->document_versions_by_name.end()
  ) {
    if (
      auto search = versions_search->second->document_by_version.upper_bound(version);
      search != versions_search->second->document_by_version.begin()
    ) {
      return std::prev(search)->second;
    }
  }

  return nullptr;
}

std::optional<DocumentId> next_document_id_locked(
  config_namespace_t* cn
) {
  DocumentId document_id = cn->next_document_id;
  do {
    if (!cn->document_ids_in_use.contains(document_id)) {
      cn->next_document_id = document_id+1;
      return std::optional<DocumentId>(document_id);
    }
  } while (++document_id != cn->next_document_id);
  return std::optional<DocumentId>();
}

bool try_insert_document_locked(
  config_namespace_t* cn,
  const std::string& name,
  VersionId version,
  std::shared_ptr<document_t>& document
) {
  if (auto document_id = next_document_id_locked(cn); document_id) {
    document->id = *document_id;
    document->oldest_version = cn->oldest_version;
    document->name = name;

    get_or_guild_document_versions_locked(cn, name)->document_by_version[version] = document;
    cn->document_ids_in_use.insert(*document_id);

    return true;
  }

  return false;
}

std::shared_ptr<document_t> try_get_or_build_document(
  config_namespace_t* cn,
  const std::string& name,
  VersionId version
) {
  cn->mutex.ReaderLock();
  auto document = get_document_locked(cn, name, version);
  if (document != nullptr) document->mutex.Lock();
  cn->mutex.ReaderUnlock();

  if (document == nullptr) {
    cn->mutex.Lock();
    document = try_get_or_build_document_locked(cn, name, version);
    if (document != nullptr) document->mutex.Lock();
    cn->mutex.Unlock();
  }

  return document;
}

std::shared_ptr<document_t> try_migrate_document_locked(
  config_namespace_t* cn,
  document_t* document,
  VersionId version
) {
  auto new_document = std::make_shared<document_t>();

  new_document->override_by_path.reserve(document->override_by_path.size());
  for (auto& it: document->override_by_path) {
    auto last_version = get_last_raw_config_locked(it.second);
    if (last_version != nullptr) {
      auto& new_override = new_document->override_by_path[it.first];
      if (is_document_full_locked(new_document.get())) {
        document->mutex.Unlock();
        return nullptr;
      }

      auto new_raw_config = last_version->clone();
      new_raw_config->id = new_document->next_raw_config_id++;
      new_override.raw_config_by_version[version] = std::move(new_raw_config);
    }
  }

  cn->mutex.Lock();
  if (try_insert_document_locked(cn, document->name, version, new_document)) {
    new_document->mutex.Lock();
  } else {
    new_document = nullptr;
  }
  document->mutex.Unlock();
  cn->mutex.Unlock();

  return new_document;
}

std::shared_ptr<document_t> try_obtain_non_full_document(
  config_namespace_t* cn,
  const std::string& name,
  VersionId version,
  size_t required_size
) {
  auto document = try_get_or_build_document(cn, name, version);
  if (document == nullptr) return nullptr;

  if (required_size + document->next_raw_config_id >= 0xffff) {
    auto new_document = try_migrate_document_locked(cn, document.get(), version);
    if (new_document == nullptr) return nullptr;
    std::swap(document, new_document);
    if (required_size + document->next_raw_config_id >= 0xffff) {
      document->mutex.Unlock();
      return nullptr;
    }
  }

  return document;
}

std::shared_ptr<merged_config_t> get_or_build_merged_config(
  document_t* document,
  const std::string& overrides_key
) {
  std::shared_ptr<merged_config_t> result;

  document->mutex.ReaderLock();
  if (
    auto search = document->merged_config_by_overrides_key.find(overrides_key);
    search != document->merged_config_by_overrides_key.end()
  ) {
    result = search->second.lock();
  }
  document->mutex.ReaderUnlock();

  if (result == nullptr) {
    document->mutex.Lock();

    if (
      auto search = document->merged_config_by_overrides_key.find(overrides_key);
      search != document->merged_config_by_overrides_key.end()
    ) {
      result = search->second.lock();
    }

    if (result == nullptr) {
      result = std::make_shared<merged_config_t>();
      result->next = result;

      document->merged_config_by_overrides_key[overrides_key] = result;
      std::swap(result->next, document->mc_generation[0].head);
    }

    document->mutex.Unlock();
  }

  return result;
}

std::shared_ptr<merged_config_t> get_merged_config(
  document_t* document,
  const std::string& overrides_key
) {
  std::shared_ptr<merged_config_t> result;
  bool found;

  document->mutex.ReaderLock();
  if (
    auto search = document->merged_config_by_overrides_key.find(overrides_key);
    found = (search != document->merged_config_by_overrides_key.end())
  ) {
    result = search->second.lock();
  }
  document->mutex.ReaderUnlock();

  if (found && (result == nullptr)) {
    document->mutex.Lock();
    if (
      auto search = document->merged_config_by_overrides_key.find(overrides_key);
      search != document->merged_config_by_overrides_key.end()
    ) {
      result = search->second.lock();
      if (result == nullptr) {
        document->merged_config_by_overrides_key.erase(search);
      }
    }
    document->mutex.Unlock();
  }

  return result;
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

std::shared_ptr<config_namespace_t> get_cn(
  context_t* ctx,
  const std::string& root_path
) {
  spdlog::debug("Obtaining the config namespace for the root path '{}'", root_path);

  ctx->mutex.ReaderLock();
  auto result = get_cn_locked(ctx, root_path);
  ctx->mutex.ReaderUnlock();

  return result;
}

std::shared_ptr<config_namespace_t> get_or_build_cn(
  context_t* ctx,
  const std::string& root_path
) {
  spdlog::debug("Obtaining the config namespace for the root path '{}'", root_path);

  ctx->mutex.ReaderLock();
  auto result = get_cn_locked(ctx, root_path);
  ctx->mutex.ReaderUnlock();

  if (result == nullptr) {
    ctx->mutex.Lock();

    auto inserted = ctx->cn_by_root_path.try_emplace(
      root_path,
      std::make_shared<config_namespace_t>()
    );
    result = inserted.first->second;

    ctx->mutex.Unlock();
  }

  return result;
}

void delete_cn_locked(
  config_namespace_t* cn
) {
  spdlog::debug("Deleting the config namespace for the root path '{}'", cn->root_path);

  cn->status = ConfigNamespaceStatus::DELETED;

  for (auto& it : cn->document_versions_by_name) {
    it.second->watchers.consume(
      [cn](auto&& watcher) {
        watcher->unregister(cn);

        auto output_message = watcher->make_output_message();
        output_message->set_status(api::stream::WatchStatus::REMOVED);
        output_message->send();
      }
    );
  }

  for (size_t i = 0, l = cn->get_requests_waiting.size(); i < l; ++i) {
    cn->get_requests_waiting[i]->set_namespace_id(cn->id);
    cn->get_requests_waiting[i]->set_status(api::request::GetRequest::Status::ERROR);
    cn->get_requests_waiting[i]->commit();
  }
  cn->get_requests_waiting.clear();

  for (auto& request: cn->update_requests_waiting) {
    request->set_namespace_id(cn->id);
    request->set_status(api::request::UpdateRequest::Status::ERROR);
    request->commit();
  }
  cn->update_requests_waiting.clear();

  for (size_t i = 0, l = cn->trace_requests_waiting.size(); i < l; ++i) {
    auto om = cn->trace_requests_waiting[i]->make_output_message();
    om->set_status(api::stream::TraceOutputMessage::Status::ERROR);
    om->send(true);
  }
  cn->trace_requests_waiting.clear();
}

void remove_cn_locked(
  context_t* ctx,
  const std::string& root_path,
  uint64_t id
) {
  spdlog::debug(
    "Removing the config namespace with the root path '{}' and id {}",
    root_path,
    id
  );

  auto search = ctx->cn_by_root_path.find(root_path);
  if (search != ctx->cn_by_root_path.end()) {
    if (search->second->id == id) {
      ctx->cn_by_root_path.erase(search);
    }
  }
}

} /* mhconfig */
