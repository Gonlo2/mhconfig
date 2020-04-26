#ifndef MHCONFIG__WORKER__BUILDER_H
#define MHCONFIG__WORKER__BUILDER_H

#include <thread>
#include <random>

#include "jmutils/container/queue.h"
#include "jmutils/parallelism/worker.h"
#include "jmutils/filesystem/common.h"

#include "mhconfig/worker/common.h"
#include "mhconfig/api/request/request.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/request/update_request.h"

namespace mhconfig
{
namespace worker
{

using jmutils::container::Queue;
using namespace mhconfig::api::request;

std::mt19937_64& prng_engine();

const static std::string TAG_FORMAT{"!format"};
const static std::string TAG_SREF{"!sref"};
const static std::string TAG_REF{"!ref"};
const static std::string TAG_DELETE{"!delete"};
const static std::string TAG_OVERRIDE{"!override"};

class Builder : public jmutils::parallelism::Worker<command::command_t>
{
public:
  Builder(
    Queue<command::command_t>& scheduler_queue,
    Queue<command::command_t>& builder_queue,
    size_t num_threads,
    Metrics& metrics
  );

  virtual ~Builder();

  Builder(Builder&& o);

protected:
  const std::string worker_name() const override {
    return "builder";
  }

  ProcessResult worker_process(const command::command_t& command) override;

private:
  enum LoadRawConfigStatus {
    OK,
    INVALID_FILE,
    FILE_DONT_EXISTS,
    ERROR
  };

  struct load_raw_config_result_t {
    LoadRawConfigStatus status;
    std::string document;
    std::string override_;
    std::shared_ptr<raw_config_t> raw_config{nullptr};
  };

  Metrics& metrics_;
  Queue<command::command_t>& scheduler_queue_;

  std::uniform_int_distribution<uint64_t> config_namespace_id_dist_{0, 0xffffffffffffffff};

  bool process_command_type_api(
    Request* api_request
  );

  bool process_command_type_api_get(
    get_request::GetRequest* api_request,
    std::shared_ptr<mhconfig::api::config::MergedConfig> merged_config
  );

  bool process_command_type_setup_request(
    const std::shared_ptr<command::setup::request_t> setup_request
  );

  bool process_command_type_build_request(
    const std::shared_ptr<command::build::request_t> build_request
  );

  bool process_command_type_update_request(
    const std::shared_ptr<command::update::request_t> update_request
  );

  // Setup logic

  std::shared_ptr<config_namespace_t> index_files(
    const std::string& root_path
  );

  load_raw_config_result_t load_raw_config(
    std::shared_ptr<string_pool::Pool> pool,
    const std::string& root_path,
    const std::string& relative_path
  );

  // Build logic

  ElementRef override_with(
    ElementRef a,
    ElementRef b,
    const std::unordered_map<std::string, ElementRef> &ref_elements_by_document
  );

  NodeType get_virtual_node_type(ElementRef element);

  ElementRef apply_tags(
    std::shared_ptr<string_pool::Pool> pool,
    ElementRef element,
    ElementRef root,
    const std::unordered_map<std::string, ElementRef> &ref_elements_by_document
  );

  ElementRef apply_tag_format(
    std::shared_ptr<string_pool::Pool> pool,
    ElementRef element
  );

  std::string format_str(
    const std::string& templ,
    uint32_t num_arguments,
    const std::vector<std::pair<std::string, std::string>>& template_arguments
  );

  ElementRef apply_tag_ref(
    ElementRef element,
    const std::unordered_map<std::string, ElementRef> &ref_elements_by_document
  );

  ElementRef apply_tag_sref(
    ElementRef child,
    ElementRef root
  );

  /*
   * All the structure checks must be done here
   */
  ElementRef make_and_check_element(
      std::shared_ptr<string_pool::Pool> pool,
      YAML::Node &node,
      std::unordered_set<std::string> &reference_to
  );

  ElementRef make_element(
      std::shared_ptr<string_pool::Pool> pool,
      YAML::Node &node,
      std::unordered_set<std::string> &reference_to
  );

};

} /* worker */
} /* mhconfig */


#endif
