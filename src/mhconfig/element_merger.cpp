#include "mhconfig/element_merger.h"

namespace mhconfig
{

ElementMerger::ElementMerger(
  jmutils::string::Pool* pool,
  const absl::flat_hash_map<std::string, Element> &element_by_document_name
) : pool_(pool),
  element_by_document_name_(element_by_document_name),
  empty_(true)
{
}

void ElementMerger::add(
  const std::shared_ptr<PersistentLogger>& logger,
  const Element& element
) {
  logger_.push_back(logger);
  if (empty_) {
    root_ = element;
    empty_ = false;
  } else {
    root_ = override_with(root_, element);
  }
}

Element ElementMerger::finish() {
  if (empty_) return Element();
  if (!root_.is_undefined()) {
    auto r = apply_tags(root_, root_, 0);
    if (r.first) root_ = r.second;
  }
  root_.freeze();
  empty_ = true;
  return root_;
}

MultiPersistentLogger& ElementMerger::logger() {
  return logger_;
}

Element ElementMerger::override_with(
  const Element& a,
  const Element& b
) {
  if (b.is_override()) {
    logger_.debug("Overriding element", a, b);
    return b;
  }

  bool is_first_a_ref = a.tag() == Element::Tag::REF;
  if (is_first_a_ref || (b.tag() == Element::Tag::REF)) {
    auto referenced_element = apply_tag_ref(
      is_first_a_ref ? a : b
    );

    return override_with(
      is_first_a_ref ? referenced_element : a,
      is_first_a_ref ? b : referenced_element
    );
  }

  switch (get_virtual_node_type(b)) {
    case VirtualNode::LITERAL: {
      VirtualNode type = get_virtual_node_type(a);
      if (type != VirtualNode::LITERAL) {
        return without_override_error(a, b);
      }

      logger_.debug("Overriding element", a, b);
      return b;
    }

    case VirtualNode::MAP: {
      if (!a.is_map()) {
        return without_override_error(a, b);
      }

      logger_.debug("Merging maps", a, b);

      Element result(a);
      auto map_a = result.as_map_mut();
      auto map_b = b.as_map();
      map_a->reserve(map_a->size() + map_b->size());

      for (const auto& x : *map_b) {
        const auto& search = map_a->find(x.first);
        if (search == map_a->end()) {
          if (x.second.tag() != Element::Tag::DELETE) {
            logger_.debug("Adding new map key", a, x.second);
            (*map_a)[x.first] = x.second;
          } else {
            logger_.warn("Trying to remove a non-existent key", a, x.second);
          }
        } else if (x.second.tag() == Element::Tag::DELETE) {
          logger_.debug("Removed a map key", search->second, x.second);
          map_a->erase(search);
        } else if (search->second.tag() == Element::Tag::DELETE) {
          logger_.debug(
            "Replacing map value marked to remove",
            search->second,
            x.second
          );
          (*map_a)[x.first] = x.second;
        } else {
          logger_.debug("Merging map value", search->second, x.second);
          (*map_a)[x.first] = override_with(
            search->second,
            x.second
          );
        }
      }

      return result;
    }

    case VirtualNode::SEQUENCE: {
      if (!a.is_seq()) {
        return without_override_error(a, b);
      }

      logger_.debug("Appending sequence", a, b);

      Element result(a);
      auto seq_a = result.as_seq_mut();
      auto seq_b = b.as_seq();
      seq_a->reserve(seq_a->size() + seq_b->size());

      for (size_t i = 0, l = seq_b->size(); i < l; ++i) {
        seq_a->push_back((*seq_b)[i]);
      }

      return result;
    }
    case VirtualNode::REF:
      assert(false);
      break;
    case VirtualNode::UNDEFINED:
      break;
  }

  logger_.error("Can't override the provided elements", a, b);
  return Element().set_origin(a);
}

VirtualNode ElementMerger::get_virtual_node_type(
  const Element& element
) {
  if (element.tag() == Element::Tag::REF) {
    return VirtualNode::REF;
  }
  if (element.tag() == Element::Tag::SREF) {
    return VirtualNode::LITERAL;
  }

  switch (element.type()) {
    case Element::Type::UNDEFINED:
      return VirtualNode::UNDEFINED;
    case Element::Type::MAP:
      return VirtualNode::MAP;
    case Element::Type::SEQUENCE:
      return VirtualNode::SEQUENCE;
    case Element::Type::NONE: // Fallback
    case Element::Type::STR: // Fallback
    case Element::Type::BIN: // Fallback
    case Element::Type::INT64: // Fallback
    case Element::Type::DOUBLE: // Fallback
    case Element::Type::BOOL:
      return VirtualNode::LITERAL;
  }

  assert(false);
  return VirtualNode::UNDEFINED;
}

std::pair<bool, Element> ElementMerger::apply_tags(
  Element element,
  const Element& root,
  uint32_t depth
) {
  if (depth >= 100) {
    logger_.error(
      "Aborted construction since it is likely that one cycle exist",
      element
    );
    return std::make_pair(true, Element().set_origin(element));
  }

  bool any_changed = false;

  switch (element.type()) {
    case Element::Type::MAP: {
      std::vector<jmutils::string::String> to_remove;
      std::vector<std::pair<jmutils::string::String, Element>> to_modify;

      for (const auto& it : *element.as_map()) {
        if (it.second.tag() == Element::Tag::DELETE) {
          logger_.warn("Removing an unused deletion node", it.second);
          to_remove.push_back(it.first);
        } else {
          auto r = apply_tags(
            it.second,
            root,
            depth+1
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

    case Element::Type::SEQUENCE: {
      std::vector<Element> new_sequence;
      auto current_sequence = element.as_seq();
      new_sequence.reserve(current_sequence->size());

      for (size_t i = 0, l = current_sequence->size(); i < l; ++i) {
        if ((*current_sequence)[i].tag() == Element::Tag::DELETE) {
          logger_.warn(
            "A deletion node don't makes sense inside a sequence, removing it",
            (*current_sequence)[i]
          );
          any_changed = true;
        } else {
          auto r = apply_tags(
            (*current_sequence)[i],
            root,
            depth+1
          );
          any_changed |= r.first;
          new_sequence.push_back(r.second);
        }
      }

      if (any_changed) {
        auto sequence = element.as_seq_mut();
        std::swap(*sequence, new_sequence);
      }
      break;
    }
    default:
      break;
  }

  if (element.tag() == Element::Tag::REF) {
    element = apply_tag_ref(element);
    any_changed = true;
  }

  if (element.tag() == Element::Tag::SREF) {
    element = apply_tag_sref(element, root, depth);
    any_changed = true;
  }

  if (element.tag() == Element::Tag::FORMAT) {
    element = apply_tag_format(element, root, depth);
    any_changed = true;
  }

  return std::make_pair(any_changed, element);
}

Element ElementMerger::apply_tag_format(
  const Element& element,
  const Element& root,
  uint32_t depth
) {
  std::stringstream ss;
  auto slices = element.as_seq();
  for (size_t i = 0, l = slices->size(); i < l; ++i) {
    auto v = apply_tags((*slices)[i], root, depth+1);
    if (
      v.first
      || (element.document_id() != v.second.document_id())
      || (element.raw_config_id() != v.second.raw_config_id())
      || (element.line() != v.second.line())
      || (element.col() != v.second.col())
    ) {
      logger_.debug("Obtained template parameter", element, v.second);
    }
    if (auto r = v.second.try_as<std::string>(); r) {
      ss << *r;
    } else {
      logger_.error("The format tag references must be scalars", element);
      return Element().set_origin(element);
    }
  }
  logger_.debug("Formated string", element);
  return Element(pool_->add(ss.str())).set_origin(element);
}

Element ElementMerger::apply_tag_ref(
  const Element& element
) {
  const auto path = element.as_seq();

  auto key_result = (*path)[0].try_as<std::string>();
  if (!key_result) {
    logger_.error("The first sequence value must be a strings", element);
    return Element().set_origin(element);
  }
  auto search = element_by_document_name_.find(*key_result);
  if (search == element_by_document_name_.end()) {
    logger_.error("Can't ref to the document", element);
    return Element().set_origin(element);
  }

  auto referenced_element = search->second;
  for (size_t i = 1, l = path->size(); i < l; ++i) {
    auto k = (*path)[i].try_as<jmutils::string::String>();
    if (!k) {
      logger_.error("All the sequence values must be a strings", element);
      return Element().set_origin(element);
    }
    referenced_element = referenced_element.get(*k);
  }

  logger_.debug("Applied ref", element, referenced_element);
  return referenced_element;
}

Element ElementMerger::apply_tag_sref(
  const Element& element,
  const Element& root,
  uint32_t depth
) {
  Element new_element = root;
  const auto path = element.as_seq();
  for (size_t i = 0, l = path->size(); i < l; ++i) {
    auto k = (*path)[i].try_as<jmutils::string::String>();
    if (!k) {
      logger_.error("All the sequence values must be a strings", element);
      return Element().set_origin(element);
    }
    new_element = new_element.get(*k);
  }

  new_element = apply_tags(
    new_element,
    root,
    depth+1
  ).second;

  if (!new_element.is_scalar()) {
    logger_.error("The element referenced must be a scalar", element);
    return Element().set_origin(element);
  }

  logger_.debug("Applied sref", element, new_element);
  return new_element;
}

} /* mhconfig */
