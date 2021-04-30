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
    logger_.push_back_frozen(logger);
    if (empty_) {
        root_ = element;
        empty_ = false;
    } else {
        root_ = override_with(root_, element);
    }
}

Element ElementMerger::finish() {
    if (empty_) return Element();

    // In this phase the merge and delete tags are handled
    if (auto r = apply_tags(false, root_, root_, 0); r.first) {
        root_ = std::move(r.second);
    }

    // In this phase only remains references
    if (auto r = apply_tags(true, root_, root_, 0); r.first) {
        root_ = std::move(r.second);
    }

    // If the delete is the root we need to mark the element as undefined
    if (root_.tag() == Element::Tag::DELETE) {
        root_ = Element().set_origin(root_);
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
    switch (b.tag()) {
        case Element::Tag::OVERRIDE:
            logger_.debug("Overriding element", a, b);
            return b;
        case Element::Tag::DELETE:
            logger_.debug("Deleting element", a, b);
            return b;
        case Element::Tag::MERGE:
            logger_.debug("Applying merge in the second element", a, b);
            return apply_tag_merge(a, b);
        case Element::Tag::REF: {
            auto r = apply_tag_ref(b);
            return override_with(a, r);
        }
        default:
            break;
    }

    switch (a.tag()) {
        case Element::Tag::DELETE:
            logger_.debug("Overriding delete element", a, b);
            return b;
        case Element::Tag::MERGE: {
            logger_.debug("Applying merge in the first element", a, b);
            auto initial = apply_tag_merge(a);
            return override_with(initial, b);
        }
        case Element::Tag::REF: {
            auto r = apply_tag_ref(a);
            return override_with(r, b);
        }
        default:
            break;
    }

    switch (b.virtual_type()) {
            case Element::VirtualType::LITERAL: {
            if (a.virtual_type() != Element::VirtualType::LITERAL) {
                return without_override_error(a, b);
            }
            logger_.debug("Overriding element", a, b);
            return b;
        }

        case Element::VirtualType::MAP: {
            if (!a.is_map()) {
                return without_override_error(a, b);
            }

            Element result(a);
            auto map_a = result.as_map_mut();
            auto map_b = b.as_map();
            map_a->reserve(map_a->size() + map_b->size());

            for (const auto& x : *map_b) {
                const auto& search = map_a->find(x.first);
                if (search == map_a->end()) {
                    if (x.second.tag() != Element::Tag::DELETE) {
                        logger_.debug("Adding map key", a, x.second);
                        (*map_a)[x.first] = x.second;
                    } else {
                        logger_.warn("Trying to remove a non-existent key", a, x.second);
                    }
                } else {
                    logger_.debug("Merging map value", search->second, x.second);
                    auto r = override_with(search->second, x.second);
                    if (r.tag() == Element::Tag::DELETE) {
                        logger_.debug("Removed map key", search->second, x.second);
                        map_a->erase(search);
                    } else {
                        search->second = std::move(r);
                    }
                }
            }

            return result;
        }

        case Element::VirtualType::SEQUENCE: {
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
        case Element::VirtualType::SPECIAL:
            assert(false);
            break;
        case Element::VirtualType::UNDEFINED:
            break;
    }

    logger_.error("Can't override the provided elements", a, b);
    return Element().set_origin(a);
}

std::pair<bool, Element> ElementMerger::apply_tags(
    bool apply_references,
    Element element,
    const Element& root,
    uint32_t depth
) {
    bool any_changed = false;

    if (element.tag() == Element::Tag::MERGE) {
        logger_.debug("Applying merge element", element);
        element = apply_tag_merge(element);
        any_changed = true;
    }

    if (element.type() == Element::Type::MAP) {
        std::vector<jmutils::string::String> to_remove;
        std::vector<std::pair<jmutils::string::String, Element>> to_modify;

        for (const auto& it : *element.as_map()) {
            auto r = apply_tags(apply_references, it.second, root, depth+1);
            if (r.second.tag() == Element::Tag::DELETE) {
                logger_.warn("Removing an unused deletion node", r.second);
                to_remove.push_back(it.first);
            } else if (r.first) {
                to_modify.emplace_back(it.first, std::move(r.second));
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
    } else if (element.type() == Element::Type::SEQUENCE) {
        std::vector<Element> new_seq;
        auto seq = element.as_seq();
        new_seq.reserve(seq->size());

        for (size_t i = 0, l = seq->size(); i < l; ++i) {
            auto r = apply_tags(apply_references, (*seq)[i], root, depth+1);
            if (r.second.tag() == Element::Tag::DELETE) {
                logger_.warn(
                    "A deletion node don't makes sense inside a sequence, removing it",
                    (*seq)[i]
                );
                any_changed = true;
            } else {
                any_changed |= r.first;
                new_seq.push_back(std::move(r.second));
            }
        }

        if (any_changed) {
            auto seq_mut = element.as_seq_mut();
            std::swap(*seq_mut, new_seq);
        }
    }

    if (apply_references) {
        auto r = post_apply_tags(element, root, depth);
        any_changed |= r.first;
        element = std::move(r.second);
    }

    return std::make_pair(any_changed, std::move(element));
}

std::pair<bool, Element> ElementMerger::post_apply_tags(
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

    return std::make_pair(any_changed, std::move(element));
}

Element ElementMerger::apply_tag_format(
    const Element& element,
    const Element& root,
    uint32_t depth
) {
    std::stringstream ss;
    auto slices = element.as_seq();
    for (size_t i = 0, l = slices->size(); i < l; ++i) {
        auto v = post_apply_tags((*slices)[i], root, depth+1);
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
        logger_.error("The first sequence value must be a string", element);
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

    new_element = post_apply_tags(new_element, root, depth+1).second;

    if (!new_element.is_scalar()) {
        logger_.error("The element referenced must be a scalar", element);
        return Element().set_origin(element);
    }

    logger_.debug("Applied sref", element, new_element);
    return new_element;
}

Element ElementMerger::apply_tag_merge(
    Element initial,
    const Element& merge_element
) {
    auto seq = merge_element.as_seq();
    for (size_t i = 0, l = seq->size(); i < l; ++i) {
        logger_.debug("Doing merge", initial, (*seq)[i]);
        initial = override_with(initial, (*seq)[i]);
    }
    return initial;
}

Element ElementMerger::apply_tag_merge(
    const Element& merge_element
) {
    auto seq = merge_element.as_seq();
    Element initial = (*seq)[0];
    for (size_t i = 1, l = seq->size(); i < l; ++i) {
        logger_.debug("Doing merge", initial, (*seq)[i]);
        initial = override_with(initial, (*seq)[i]);
    }
    return initial;
}

} /* mhconfig */
