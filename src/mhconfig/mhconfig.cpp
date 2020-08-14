#include "mhconfig/mhconfig.h"

namespace mhconfig
{

MHConfig::MHConfig(
  const std::string& config_path,
  const std::string& server_address,
  const std::string& prometheus_address,
  size_t num_threads_api,
  size_t num_threads_workers
) : config_path_(config_path),
  server_address_(server_address),
  prometheus_address_(prometheus_address),
  num_threads_api_(num_threads_api),
  num_threads_workers_(num_threads_workers)
{
}

MHConfig::~MHConfig() {
}

bool MHConfig::run() {
  if (running_) return false;


  ctx_ = std::make_unique<context_t>();


  ctx_->metrics.init(prometheus_address_);


  if (!ctx_->acl.load(config_path_)) return false;


  workers_.reserve(num_threads_workers_);
  for (size_t i = 0; i < num_threads_workers_; ++i) {
    auto worker = std::make_unique<mhconfig::Worker>(ctx_.get());
    if (!worker->start()) return false;
    workers_.push_back(std::move(worker));
  }


  service_ = std::make_unique<api::Service>(
    server_address_,
    num_threads_api_,
    ctx_.get()
  );
  service_->start();


  if (!run_time_worker()) return false;


  running_ = true;
  return true;
}

bool MHConfig::reload() {
  if (!running_) return false;

  if (!ctx_->acl.load(config_path_)) {
    return false;
  }

  return true;
}

bool MHConfig::join() {
  if (!running_) return false;

  time_worker_.join();
  service_->join();
  for (auto& worker : workers_) {
    worker->join();
  }

  return true;
}

bool MHConfig::run_time_worker() {
  auto current_time_ms = jmutils::monotonic_now_ms();

  time_worker_.set_function(
    static_cast<uint32_t>(TimeWorkerTag::RUN_GC_CACHE_GENERATION_0),
    current_time_ms + 20000,
    [ctx=ctx_.get()]() -> uint64_t {
      auto timelimit_s = jmutils::monotonic_now_sec() - 10;
      ctx->worker_queue.push(
        std::make_unique<worker::GCMergedConfigsCommand>(0, timelimit_s)
      );
      return jmutils::monotonic_now_ms() + 20000;
    }
  );

  time_worker_.set_function(
    static_cast<uint32_t>(TimeWorkerTag::RUN_GC_CACHE_GENERATION_1),
    current_time_ms + 100000,
    [ctx=ctx_.get()]() -> uint64_t {
      auto timelimit_s = jmutils::monotonic_now_sec() - 10;
      ctx->worker_queue.push(
        std::make_unique<worker::GCMergedConfigsCommand>(1, timelimit_s)
      );
      return jmutils::monotonic_now_ms() + 100000;
    }
  );

  time_worker_.set_function(
    static_cast<uint32_t>(TimeWorkerTag::RUN_GC_CACHE_GENERATION_2),
    current_time_ms + 340000,
    [ctx=ctx_.get()]() -> uint64_t {
      auto timelimit_s = jmutils::monotonic_now_sec() - 10;
      ctx->worker_queue.push(
        std::make_unique<worker::GCMergedConfigsCommand>(2, timelimit_s)
      );
      return jmutils::monotonic_now_ms() + 340000;
    }
  );

  time_worker_.set_function(
    static_cast<uint32_t>(TimeWorkerTag::RUN_GC_DEAD_POINTERS),
    current_time_ms + 140000,
    [ctx=ctx_.get()]() -> uint64_t {
      ctx->worker_queue.push(
        std::make_unique<worker::GCDeadPointersCommand>()
      );
      return jmutils::monotonic_now_ms() + 140000;
    }
  );


  time_worker_.set_function(
    static_cast<uint32_t>(TimeWorkerTag::RUN_GC_NAMESPACES),
    current_time_ms + 220000,
    [ctx=ctx_.get()]() -> uint64_t {
      auto timelimit_s = jmutils::monotonic_now_sec() - 10;
      ctx->worker_queue.push(
        std::make_unique<worker::GCConfigNamespacesCommand>(timelimit_s)
      );
      return jmutils::monotonic_now_ms() + 220000;
    }
  );

  time_worker_.set_function(
    static_cast<uint32_t>(TimeWorkerTag::RUN_GC_VERSIONS),
    current_time_ms + 60000,
    [ctx=ctx_.get()]() -> uint64_t {
      auto timelimit_s = jmutils::monotonic_now_sec() - 10;
      ctx->worker_queue.push(
        std::make_unique<worker::GCRawConfigVersionsCommand>(timelimit_s)
      );
      return jmutils::monotonic_now_ms() + 60000;
    }
  );

  return time_worker_.start();
}

} /* mhconfig */
