#ifndef MHCONFIG__WORKER__COMMON_H
#define MHCONFIG__WORKER__COMMON_H

#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <memory>

#include "mhconfig/api/request/request.h"
#include "mhconfig/api/config/merged_config.h"
#include "mhconfig/element.h"
#include "jmutils/common.h"

namespace mhconfig
{
namespace worker
{

const static uint8_t NUMBER_OF_GC_GENERATIONS{3};

struct raw_config_t {
  uint32_t id{0};
  ElementRef value{nullptr};
  std::unordered_set<std::string> reference_to;
};

enum MergedConfigStatus {
  UNDEFINED,
  BUILDING,
  OK,
  REMOVED
};

struct merged_config_t {
  MergedConfigStatus status{MergedConfigStatus::UNDEFINED};
  int64_t last_access_timestamp{0};
  ElementRef value{nullptr};
  std::shared_ptr<mhconfig::api::config::MergedConfig> api_merged_config{nullptr};
};

struct document_metadata_t {
  std::unordered_map<
    std::string,
    std::map<uint32_t, std::shared_ptr<raw_config_t>>
  > raw_config_by_version_by_override;

  std::unordered_map<std::string, ::jmutils::zero_value_t<uint32_t>> referenced_by;
};

struct merged_config_metadata_t {
  std::unordered_map<
    std::string,
    std::weak_ptr<merged_config_t>
  > merged_config_by_document;
};

struct config_namespace_t;

namespace command
{

  enum CommandType{
    API,
    API_GET,

    //OLD


    SETUP_REQUEST,
    SETUP_RESPONSE,
    GET_REQUEST,
    GET_RESPONSE,
    UPDATE_REQUEST,
    UPDATE_RESPONSE,
    BUILD_REQUEST,
    BUILD_RESPONSE,
    RUN_GC_REQUEST,
    RUN_GC_RESPONSE
  };

  std::string to_string(CommandType type);

  namespace setup {
    struct request_t {
      std::string root_path;
    };

    struct response_t {
      std::string root_path;
      std::shared_ptr<config_namespace_t> config_namespace;
    };
  }

  namespace update {
    struct request_t {
      mhconfig::api::request::Request* api_request;
      uint64_t namespace_id;
      std::shared_ptr<string_pool::Pool> pool;
    };

    struct item_t {
      std::string document;
      std::string override_;
      std::shared_ptr<raw_config_t> raw_config;

      item_t(
        const std::string& document,
        const std::string& override_,
        std::shared_ptr<raw_config_t> raw_config
      ) : document(document),
        override_(override_),
        raw_config(raw_config)
      {}
    };

    enum ResponseStatus {
      OK,
      ERROR
    };

    struct response_t {
      mhconfig::api::request::Request* api_request;
      ResponseStatus status;
      uint32_t version;
      uint64_t namespace_id;
      std::vector<item_t> items;
    };
  }

  namespace build {
    struct build_element_t {
      mhconfig::ElementRef config;

      std::string name;
      std::string overrides_key;

      std::unordered_map<
        std::string,
        std::shared_ptr<raw_config_t>
      > raw_config_by_override;
    };

    struct request_t {
      mhconfig::api::request::Request* request;
      uint64_t namespace_id;
      uint32_t specific_version;
      std::shared_ptr<string_pool::Pool> pool;
      std::vector<build_element_t> elements_to_build;
    };

    struct built_element_t {
      std::string overrides_key;
      mhconfig::ElementRef config;
    };

    struct response_t {
      mhconfig::api::request::Request* request;
      uint64_t namespace_id;
      uint32_t specific_version;
      std::unordered_map<std::string, built_element_t> built_elements_by_document;
    };
  }

  namespace run_gc {
    enum Type {
      CACHE_GENERATION_0 = 0,
      CACHE_GENERATION_1 = 1,
      CACHE_GENERATION_2 = 2,
      DEAD_POINTERS = 3,
      NAMESPACES = 4,
      VERSIONS = 5
    };

    struct request_t {
      Type type;
      uint32_t max_live_in_seconds;
    };

    struct response_t {
      void* id;
    };
  }

  struct command_t {
    CommandType type;
    mhconfig::api::request::Request* api_request;

    std::shared_ptr<setup::request_t> setup_request;
    std::shared_ptr<setup::response_t> setup_response;

    std::shared_ptr<build::request_t> build_request;
    std::shared_ptr<build::response_t> build_response;

    std::shared_ptr<update::request_t> update_request;
    std::shared_ptr<update::response_t> update_response;

    std::shared_ptr<run_gc::request_t> run_gc_request;
    std::shared_ptr<run_gc::response_t> run_gc_response;

    std::shared_ptr<mhconfig::api::config::MergedConfig> api_merged_config{nullptr};
  };
} /* command */

struct wait_built_t {
  std::unordered_map<std::string, uint32_t> pending_element_position_by_name;
  bool is_main;
  command::command_t command;
};

struct config_namespace_t {
  bool ok;
  uint32_t next_raw_config_id{1};
  uint32_t current_version{1};
  uint64_t id;
  uint64_t last_access_timestamp;
  std::string root_path;

  std::shared_ptr<string_pool::Pool> pool;

  std::unordered_map<
    std::string,
    std::shared_ptr<document_metadata_t>
  > document_metadata_by_document;

  std::unordered_map<
    std::string,
    std::shared_ptr<merged_config_metadata_t>
  > merged_config_metadata_by_overrides_key;

  std::vector<
    std::shared_ptr<merged_config_t>
  > merged_config_by_gc_generation[NUMBER_OF_GC_GENERATIONS];

  std::unordered_map<
    std::string,
    std::vector<std::shared_ptr<wait_built_t>>
  > wait_builts_by_key;

  std::list<std::pair<uint64_t, uint32_t>> stored_versions_by_deprecation_timestamp;
};

} /* worker */
} /* mhconfig */

#endif
