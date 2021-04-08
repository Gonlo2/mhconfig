#ifndef MHCONFIG__API__COMMON_H
#define MHCONFIG__API__COMMON_H

#include <string>
#include <vector>

#include "mhconfig/element.h"
#include "mhconfig/constants.h"
#include "mhconfig/proto/mhconfig.grpc.pb.h"

namespace mhconfig
{
namespace api
{

typedef absl::flat_hash_set<std::pair<DocumentId, VersionId>> SourceIds;

enum class LogLevel {
  ERROR = 0,
  WARN = 1,
  DEBUG = 2,
  TRACE = 3
};

struct position_t {
  uint32_t source_id;
  uint16_t line;
  uint8_t col;
};

struct source_t {
  uint16_t document_id;
  uint16_t raw_config_id;
  uint32_t checksum;
  std::string path;
};

inline uint32_t make_source_id(
  DocumentId document_id,
  RawConfigId raw_config_id
) {
  //TODO Add size check
  return (document_id << 16) | raw_config_id;
}

template <typename T>
uint32_t fill_elements(
  const Element& root,
  T* container,
  proto::Element* output,
  bool with_position,
  SourceIds& source_ids
) {
  if (with_position && (root.document_id() != 0xffff) && (root.raw_config_id() != 0xffff)) {
    source_ids.emplace(root.document_id(), root.raw_config_id());
    auto position = output->mutable_position();
    auto id = make_source_id(root.document_id(), root.raw_config_id());
    position->set_present(true);
    position->set_source_id(id);
    position->set_line(root.line());
    position->set_col(root.col());
  }

  switch (root.type()) {
    case Element::Type::UNDEFINED: {
      output->set_value_type(proto::Element_ValueType_UNDEFINED);
      return 1;
    }

    case Element::Type::NONE: {
      output->set_value_type(proto::Element_ValueType_NONE);
      return 1;
    }

    case Element::Type::STR: {
      if (auto r = root.try_as<std::string>(); r) {
        output->set_value_type(proto::Element_ValueType_STR);
        output->set_value_str(*r);
      } else {
        output->set_value_type(proto::Element_ValueType_UNDEFINED);
      }
      return 1;
    }

    case Element::Type::BIN: {
      if (auto r = root.try_as<std::string>(); r) {
        output->set_value_type(proto::Element_ValueType_BIN);
        output->set_value_bin(*r);
      } else {
        output->set_value_type(proto::Element_ValueType_UNDEFINED);
      }
      return 1;
    }

    case Element::Type::INT64: {
      if (auto r = root.try_as<int64_t>(); r) {
        output->set_value_type(proto::Element_ValueType_INT64);
        output->set_value_int(*r);
      } else {
        output->set_value_type(proto::Element_ValueType_UNDEFINED);
      }
      return 1;
    }

    case Element::Type::DOUBLE: {
      if (auto r = root.try_as<double>(); r) {
        output->set_value_type(proto::Element_ValueType_DOUBLE);
        output->set_value_double(*r);
      } else {
        output->set_value_type(proto::Element_ValueType_UNDEFINED);
      }
      return 1;
    }

    case Element::Type::BOOL: {
      if (auto r = root.try_as<bool>(); r) {
        output->set_value_type(proto::Element_ValueType_BOOL);
        output->set_value_bool(*r);
      } else {
        output->set_value_type(proto::Element_ValueType_UNDEFINED);
      }
      return 1;
    }

    case Element::Type::MAP: {
      output->set_value_type(proto::Element_ValueType_MAP);
      auto map = root.as_map();
      output->set_size(map->size());

      uint32_t parent_sibling_offset = 1;
      ::mhconfig::proto::Element* value = output;
      for (const auto& it : *map) {
        value = container->add_elements();

        uint32_t sibling_offset = fill_elements(
          it.second,
          container,
          value,
          with_position,
          source_ids
        );
        value->set_key_type(proto::Element_KeyType_KSTR);
        value->set_key_str(it.first.str());
        value->set_sibling_offset(sibling_offset-1);

        parent_sibling_offset += sibling_offset;
      }
      value->set_sibling_offset(0);

      return parent_sibling_offset;
    }

    case Element::Type::SEQUENCE: {
      output->set_value_type(proto::Element_ValueType_SEQUENCE);
      auto seq = root.as_seq();
      output->set_size(seq->size());

      uint32_t parent_sibling_offset = 1;
      ::mhconfig::proto::Element* value = output;
      for (size_t i = 0, l = seq->size(); i < l; ++i) {
        value = container->add_elements();

        uint32_t sibling_offset = fill_elements(
          (*seq)[i],
          container,
          value,
          with_position,
          source_ids
        );
        value->set_sibling_offset(sibling_offset-1);

        parent_sibling_offset += sibling_offset;
      }
      value->set_sibling_offset(0);

      return parent_sibling_offset;
    }
  }

  output->set_value_type(proto::Element_ValueType_UNDEFINED);
  return 1;
}

template <typename T>
void fill_sources(
  const std::vector<source_t>& sources,
  T* container
) {
  for (const auto& source : sources) {
    auto csource = container->add_sources();
    auto id = make_source_id(
      source.document_id,
      source.raw_config_id
    );
    csource->set_id(id);
    csource->set_checksum(source.checksum);
    csource->set_path(source.path);
  }
}

inline mhconfig::proto::LogLevel level_to_proto(LogLevel level) {
  switch (level) {
    case LogLevel::ERROR:
      return mhconfig::proto::LogLevel::ERROR;
    case LogLevel::WARN:
      return mhconfig::proto::LogLevel::WARN;
    case LogLevel::DEBUG:
      return mhconfig::proto::LogLevel::DEBUG;
    case LogLevel::TRACE:
      return mhconfig::proto::LogLevel::TRACE;
  }
  assert(false);
  return mhconfig::proto::LogLevel::ERROR;
}

inline void fill_position(
  const position_t& position,
  mhconfig::proto::Position* out
) {
  out->set_present(true);
  out->set_source_id(position.source_id);
  out->set_line(position.line);
  out->set_col(position.col);
}

} /* api */
} /* mhconfig */

#endif
