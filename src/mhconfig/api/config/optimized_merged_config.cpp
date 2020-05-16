#include "mhconfig/api/config/optimized_merged_config.h"

namespace mhconfig
{
namespace api
{
namespace config
{

namespace
{
  std::string& get_skey_buffer() {
    thread_local static std::string buffer;
    return buffer;
  }
}

uint32_t make_elements_ranges_map_rec(
  mhconfig::ElementRef root,
  uint32_t& idx,
  const std::string& skey,
  bool add,
  std::vector<std::pair<std::string, std::pair<uint32_t, uint32_t>>>& output
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

        std::string new_skey(skey);
        jmutils::push_str(new_skey, it.first.str());

        uint32_t sibling_offset = make_elements_ranges_map_rec(
          it.second,
          idx,
          new_skey,
          add,
          output
        );

        if (add) {
          spdlog::trace("The position range of the key '{}' is ({}, {})", new_skey, start_idx, start_idx + sibling_offset - 1);
          output.emplace_back(new_skey, std::make_pair(start_idx, start_idx + sibling_offset));
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
  std::vector<std::pair<std::string, std::pair<uint32_t, uint32_t>>>& output
) {
  uint32_t idx = 0;
  std::string key = "";
  uint32_t size = make_elements_ranges_map_rec(
    root,
    idx,
    key,
    true,
    output
  );
  spdlog::trace("The position range of the key '' is ({}, {})", 0, size);
  output.emplace_back(key, std::make_pair(0, size));
}


OptimizedMergedConfig::OptimizedMergedConfig() {
}

OptimizedMergedConfig::~OptimizedMergedConfig() {
  if (hash_ != nullptr) {
    spdlog::trace("Deleted the cmph hash in {}", (void*)hash_);
    cmph_destroy(hash_);
    hash_ = nullptr;
  }
}

bool OptimizedMergedConfig::init(
  ElementRef element,
  std::shared_ptr<::string_pool::Pool> pool,
  metrics::MetricsService& metrics_service
) {
  std::vector<std::pair<std::string, std::pair<uint32_t, uint32_t>>> skeys_and_positions;
  make_elements_ranges_map(element, skeys_and_positions);

  std::vector<char*> skeys;
  skeys.reserve(skeys_and_positions.size());
  for (auto& x: skeys_and_positions) {
    skeys.push_back((char*)x.first.c_str());
  }
  cmph_io_adapter_t *source = cmph_io_vector_adapter(skeys.data(), skeys.size());
  spdlog::trace("Created a cmph source in {}", (void*)source);

  cmph_config_t *config = cmph_config_new(source);
  spdlog::trace("Created a cmph config in {}", (void*)config);
  cmph_config_set_graphsize(config, 0.99);
  //TODO Check and change the algorithm if it's neccesary
  cmph_config_set_algo(config, CMPH_CHD);
  hash_ = cmph_new(config);
  if (hash_ == nullptr) {
    spdlog::trace("Can't create the cmph hash");
    return false;
  } else {
    spdlog::trace("Created a cmph hash in {}", (void*)hash_);
  }

  cmph_io_vector_adapter_destroy(source);

  ::mhconfig::proto::GetResponse get_response;
  fill_elements(element, &get_response, get_response.add_elements());

  std::stringstream ss;

  std::vector<uint32_t> size_till_position;
  size_till_position.reserve(get_response.elements().size()+1);
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
      spdlog::error(
        "Some error take place serializing the msg '{}'",
        tmp_get_response.ShortDebugString()
      );
      return false;
    }
    uint32_t l = ss.tellp();
    spdlog::trace(
      "The size of the msg till '{}' is {}",
      tmp_get_response.ShortDebugString(),
      l
    );
    size_till_position.push_back(l);
  }

  data_ = ss.str();

  metrics_service.observe(
    metrics::MetricsService::ObservableId::OPTIMIZED_MERGED_CONFIG_USED_BYTES,
    {},
    data_.size()
  );

  position_.resize(skeys_and_positions.size());
  for (auto& it : skeys_and_positions) {
    uint32_t idx = cmph_search(
      hash_,
      (char*)it.first.c_str(),
      (cmph_uint32)it.first.size()
    );

    position_[idx].key = pool->add(it.first);
    position_[idx].start = size_till_position[it.second.first];
    position_[idx].size = size_till_position[it.second.second] - position_[idx].start;
  }

  return true;
}

void OptimizedMergedConfig::add_elements(
  request::GetRequest* api_request
) {
  std::string& skey = get_skey_buffer();
  skey.clear();
  //FIXME Ignore the first key
  for (uint32_t i = 1; i < api_request->key().size(); ++i) {
    jmutils::push_str(skey, api_request->key()[i]);
  }

  uint32_t idx = cmph_search(hash_, skey.c_str(), (cmph_uint32)skey.size());
  if (position_[idx].key == skey) {
    spdlog::trace(
      "Found the range of the key of the request {} ({}, {})",
      (void*)api_request,
      position_[idx].start,
      position_[idx].size
    );
    api_request->set_element_bytes(
      &data_[position_[idx].start],
      position_[idx].size
    );
  } else {
    spdlog::trace("Can't find the key '{}'", (void*)api_request);
    api_request->set_element(mhconfig::UNDEFINED_ELEMENT);
  }
}

} /* config */
} /* api */
} /* mhconfig */
