#ifndef MHCONFIG__ELEMENT_MERGER_H
#define MHCONFIG__ELEMENT_MERGER_H

#include "mhconfig/element.h"
#include "mhconfig/string_pool.h"
#include "mhconfig/logger/logger.h"
#include "mhconfig/logger/persistent_logger.h"
#include "mhconfig/logger/multi_persistent_logger.h"
#include "mhconfig/constants.h"

namespace mhconfig
{

using logger::ReplayLogger;
using logger::PersistentLogger;
using logger::MultiPersistentLogger;

enum class VirtualNode {
  UNDEFINED,
  MAP,
  SEQUENCE,
  LITERAL,
  REF
};

class ElementMerger final
{
public:
  ElementMerger(
    jmutils::string::Pool* pool,
    const absl::flat_hash_map<std::string, Element> &element_by_document_name
  );

  void add(
    const std::shared_ptr<PersistentLogger>& logger,
    const Element& element
  );

  Element finish();

  MultiPersistentLogger& logger();

private:
  MultiPersistentLogger logger_;
  jmutils::string::Pool* pool_;
  const absl::flat_hash_map<std::string, Element> &element_by_document_name_;

  Element root_;
  bool empty_;

  Element override_with(
    const Element& a,
    const Element& b
  );

  std::pair<bool, Element> apply_tags(
    Element element,
    const Element& root,
    uint32_t depth
  );

  VirtualNode get_virtual_node_type(
    const Element& element
  );

  Element apply_tag_format(
    const Element& element,
    const Element& root,
    uint32_t depth
  );

  Element apply_tag_ref(
    const Element& element
  );

  Element apply_tag_sref(
    const Element& element,
    const Element& root,
    uint32_t depth
  );

  inline Element without_override_error(
    const Element& a,
    const Element& b
  ) {
    logger_.error("Can't override different types without the override tag", a, b);
    return Element().set_origin(a);
  }

};

} /* mhconfig */

#endif
