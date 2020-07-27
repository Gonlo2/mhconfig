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
  sync_metrics_service_(prometheus_address),
  num_threads_api_(num_threads_api),
  num_threads_workers_(num_threads_workers)
{
}

MHConfig::~MHConfig() {
}

bool MHConfig::run() {
  if (running_) return false;


  acl_ = std::make_unique<auth::Acl>();
  if (!acl_->load(config_path_)) return false;


  sync_metrics_service_.init();


  workers_.reserve(num_threads_workers_);
  for (size_t i = 0; i < num_threads_workers_; ++i) {
    auto worker = std::make_unique<mhconfig::Worker>(
      worker_queue_.new_receiver(),
      WorkerCommand::context_t(
        scheduler_queue_.new_sender(),
        std::make_unique<metrics::AsyncMetricsService>(metrics_queue_.new_sender()),
        sync_metrics_service_
      )
    );
    if (!worker->start()) return false;
    workers_.push_back(std::move(worker));
  }


  std::vector<std::pair<SchedulerQueue::SenderRef, metrics::AsyncMetricsService>> thread_vars;
  thread_vars.reserve(num_threads_api_);
  for (size_t i = 0; i < num_threads_api_; ++i) {
    thread_vars.emplace_back(
      scheduler_queue_.new_sender(),
      metrics::AsyncMetricsService(metrics_queue_.new_sender())
    );
  }
  service_ = std::make_unique<api::Service>(
    server_address_,
    std::move(thread_vars),
    acl_.get()
  );
  service_->start();


  if (!run_time_worker()) return false;


  scheduler_ = std::make_unique<Scheduler>(
    scheduler_queue_,
    worker_queue_,
    std::make_unique<metrics::AsyncMetricsService>(metrics_queue_.new_sender())
  );
  if (!scheduler_->start()) return false;


  metrics_worker_ = std::make_unique<metrics::MetricsWorker>(
    metrics_queue_,
    sync_metrics_service_
  );
  metrics_worker_->start();


  running_ = true;
  return true;
}

bool MHConfig::reload() {
  if (!running_) return false;

  if (!acl_->load(config_path_)) {
    return false;
  }

  return true;
}

bool MHConfig::join() {
  if (!running_) return false;

  time_worker_.join();
  service_->join();
  scheduler_->join();
  for (auto& worker : workers_) {
    worker->join();
  }

  return true;
}

bool MHConfig::run_time_worker() {
  auto current_time_ms = jmutils::monotonic_now_ms();
  auto time_worker_sender = scheduler_queue_.new_sender();

  time_worker_.set_function(
      static_cast<uint32_t>(TimeWorkerTag::RUN_GC_CACHE_GENERATION_0),
      current_time_ms + 20000,
      [sender=time_worker_sender.get()]() -> uint64_t {
        sender->push(
          std::make_unique<scheduler::RunGcCommand>(
            scheduler::RunGcCommand::Type::CACHE_GENERATION_0,
            10
          )
        );
        return jmutils::monotonic_now_ms() + 20000;
      }
  );

  time_worker_.set_function(
      static_cast<uint32_t>(TimeWorkerTag::RUN_GC_CACHE_GENERATION_1),
      current_time_ms + 100000,
      [sender=time_worker_sender.get()]() -> uint64_t {
        sender->push(
          std::make_unique<scheduler::RunGcCommand>(
            scheduler::RunGcCommand::Type::CACHE_GENERATION_1,
            10
          )
        );
        return jmutils::monotonic_now_ms() + 100000;
      }
  );

  time_worker_.set_function(
      static_cast<uint32_t>(TimeWorkerTag::RUN_GC_CACHE_GENERATION_2),
      current_time_ms + 340000,
      [sender=time_worker_sender.get()]() -> uint64_t {
        sender->push(
          std::make_unique<scheduler::RunGcCommand>(
            scheduler::RunGcCommand::Type::CACHE_GENERATION_2,
            10
          )
        );
        return jmutils::monotonic_now_ms() + 340000;
      }
  );

  time_worker_.set_function(
      static_cast<uint32_t>(TimeWorkerTag::RUN_GC_DEAD_POINTERS),
      current_time_ms + 140000,
      [sender=time_worker_sender.get()]() -> uint64_t {
        sender->push(
          std::make_unique<scheduler::RunGcCommand>(
            scheduler::RunGcCommand::Type::DEAD_POINTERS,
            10
          )
        );
        return jmutils::monotonic_now_ms() + 140000;
      }
  );

  time_worker_.set_function(
      static_cast<uint32_t>(TimeWorkerTag::RUN_GC_NAMESPACES),
      current_time_ms + 220000,
      [sender=time_worker_sender.get()]() -> uint64_t {
        sender->push(
          std::make_unique<scheduler::RunGcCommand>(
            scheduler::RunGcCommand::Type::NAMESPACES,
            10
          )
        );
        return jmutils::monotonic_now_ms() + 220000;
      }
  );

  time_worker_.set_function(
      static_cast<uint32_t>(TimeWorkerTag::RUN_GC_VERSIONS),
      current_time_ms + 60000,
      [sender=time_worker_sender.get()]() -> uint64_t {
        sender->push(
          std::make_unique<scheduler::RunGcCommand>(
            scheduler::RunGcCommand::Type::VERSIONS,
            10
          )
        );
        return jmutils::monotonic_now_ms() + 60000;
      }
  );

  return time_worker_.start();
}

} /* mhconfig */
