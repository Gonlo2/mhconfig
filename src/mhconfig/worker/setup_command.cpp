#include "mhconfig/worker/setup_command.h"

namespace mhconfig
{

namespace worker
{

SetupCommand::SetupCommand(std::shared_ptr<config_namespace_t>&& cn)
  : cn_(std::move(cn))
{
}

std::string SetupCommand::name() const {
  return "SETUP";
}

bool SetupCommand::force_take_metric() const {
  return true;
}

bool SetupCommand::execute(
  context_t* ctx
) {
  spdlog::debug("Initializating the config namespace '{}'", cn_->root_path);
  cn_->pool = std::make_shared<jmutils::string::Pool>(
    std::make_unique<string_pool::MetricsStatsObserver>(ctx->metrics, cn_->root_path)
  );
  bool ok = init_config_namespace(cn_.get());

  if (ok) {
    std::vector<std::shared_ptr<GetConfigTask>> get_config_tasks_waiting;
    std::vector<std::shared_ptr<api::stream::WatchInputMessage>> watch_requests_waiting;
    std::vector<std::shared_ptr<api::stream::TraceInputMessage>> trace_requests_waiting;

    cn_->mutex.Lock();
    bool update = !cn_->update_requests_waiting.empty();
    if (update) {
      cn_->status = ConfigNamespaceStatus::OK_UPDATING;
    } else {
      cn_->status = ConfigNamespaceStatus::OK;
      std::swap(watch_requests_waiting, cn_->watch_requests_waiting);
    }
    std::swap(get_config_tasks_waiting, cn_->get_config_tasks_waiting);
    std::swap(trace_requests_waiting, cn_->trace_requests_waiting);
    cn_->mutex.Unlock();

    for (size_t i = 0, l = trace_requests_waiting.size(); i < l; ++i) {
      bool partial_ok = process_trace_request(
        decltype(cn_)(cn_),
        std::move(trace_requests_waiting[i]),
        ctx
      );
      ok &= partial_ok;
    }

    if (update) {
      auto command = std::make_unique<worker::UpdateCommand>(cn_);
      execute_command(std::move(command), ctx);
    }

    for (size_t i = 0, l = get_config_tasks_waiting.size(); i < l; ++i) {
      bool partial_ok = process_get_config_task(
        decltype(cn_)(cn_),
        std::move(get_config_tasks_waiting[i]),
        ctx
      );
      ok &= partial_ok;
    }

    for (size_t i = 0, l = watch_requests_waiting.size(); i < l; ++i) {
      bool partial_ok = process_watch_request(
        decltype(cn_)(cn_),
        std::move(watch_requests_waiting[i]),
        ctx
      );
      ok &= partial_ok;
    }
  } else {
    remove_cn(ctx, cn_->root_path, cn_->id);
    delete_cn(cn_);

    ok = true;
  }

  return ok;
}

} /* worker */
} /* mhconfig */
