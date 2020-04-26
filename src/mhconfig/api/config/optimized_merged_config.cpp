#include "mhconfig/api/config/optimized_merged_config.h"

namespace mhconfig
{
namespace api
{
namespace config
{


uint32_t make_elements_ranges_map_rec(
  mhconfig::ElementRef root,
  uint32_t& idx,
  const std::string& skey,
  bool add,
  std::unordered_map<std::string, std::pair<uint32_t, uint32_t>>& output
) {
  ++idx;

  switch (root->type()) {
    case ::mhconfig::UNDEFINED_NODE: // Fallback
    case ::mhconfig::NULL_NODE: // Fallback
    case ::mhconfig::SCALAR_NODE:
      return 1;

    case ::mhconfig::MAP_NODE: {
      uint32_t parent_sibling_offset = 1;

      for (const auto& it : root->as_map()) {
        uint32_t start_idx = idx;

        std::string new_skey = skey;
        new_skey += '/';
        new_skey += it.first.str();

        uint32_t sibling_offset = make_elements_ranges_map_rec(
          it.second,
          idx,
          new_skey,
          add,
          output
        );

        if (add) {
          spdlog::trace("The position range of the key '{}' is ({}, {})", new_skey, start_idx, start_idx + sibling_offset - 1);
          output[new_skey] = std::make_pair(start_idx, start_idx + sibling_offset);
        }

        parent_sibling_offset += sibling_offset;
      }

      return parent_sibling_offset;
    }

    case ::mhconfig::SEQUENCE_NODE: {
      uint32_t parent_sibling_offset = 1;

      for (const auto x : root->as_sequence()) {
        parent_sibling_offset += make_elements_ranges_map_rec(
          x,
          idx,
          skey,
          false,
          output
        );
      }

      return parent_sibling_offset;
    }
  }

  ++idx;
  return 1;
}


void make_elements_ranges_map(
  mhconfig::ElementRef root,
  std::unordered_map<std::string, std::pair<uint32_t, uint32_t>>& output
) {
  uint32_t idx = 0;
  std::string key = "";
  uint32_t size = make_elements_ranges_map_rec(root, idx, key, true, output);
  spdlog::trace("The position range of the key '' is ({}, {})", 0, size);
  output[key] = std::make_pair(0, size);
}


OptimizedMergedConfig::OptimizedMergedConfig() {
}

OptimizedMergedConfig::~OptimizedMergedConfig() {
}

bool OptimizedMergedConfig::init(ElementRef element) {
  make_elements_ranges_map(element, data_range_by_skey_);

  ::mhconfig::proto::GetResponse get_response;
  fill_elements(element, &get_response, get_response.add_elements());

  std::stringstream ss;

  std::vector<uint32_t> size_till_position;
  size_till_position.reserve(get_response.elements().size());
  size_till_position.push_back(0);
  ::mhconfig::proto::GetResponse tmp_get_response;
  auto tmp_element = tmp_get_response.add_elements();
  for (
      auto msg = get_response.mutable_elements()->begin();
      msg != get_response.mutable_elements()->end();
      ++msg
  ) {
    msg->Swap(tmp_element);
    if (!tmp_get_response.SerializeToOstream(&ss)) {
      spdlog::error("Some error take place serializing the msg '{}'", tmp_get_response.ShortDebugString());
      return false;
    }
    uint32_t l = ss.tellp();
    spdlog::trace("The size of the msg till '{}' is {}", tmp_get_response.ShortDebugString(), l);
    size_till_position.push_back(l);
  }

  data_ = ss.str();

  for (auto& it : data_range_by_skey_) {
    it.second.first = size_till_position[it.second.first];
    it.second.second = size_till_position[it.second.second] - it.second.first;
    spdlog::trace("The size range of the key '{}' is ({}, {})", it.first, it.second.first, it.second.second);
  }

  return true;
}

void OptimizedMergedConfig::add_elements(
  const std::vector<std::string>& key,
  ::mhconfig::proto::GetResponse& msg
) {
  std::string skey = get_skey(key);
  auto search = data_range_by_skey_.find(skey);
  if (search == data_range_by_skey_.end()) {
    spdlog::trace("Can't find the key '{}'", skey);
    fill_elements(mhconfig::UNDEFINED_ELEMENT, &msg, msg.add_elements());
  } else {
    spdlog::trace("Found the range of the key '{}' ({}, {})", search->first, search->second.first, search->second.second);
    std::string msg_data(&data_[search->second.first], search->second.second);
    msg.MergeFromString(msg_data);
  }
}

std::string OptimizedMergedConfig::get_skey(const std::vector<std::string>& key) {
  std::stringstream ss;
  //FIXME Ignore the first key
  for (uint32_t i = 1; i < key.size(); ++i) {
    ss << '/' << key[i];
  }
  return ss.str();
}

} /* config */
} /* api */
} /* mhconfig */
