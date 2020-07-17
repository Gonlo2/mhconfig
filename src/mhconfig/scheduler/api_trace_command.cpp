#include "mhconfig/scheduler/api_trace_command.h"

namespace mhconfig
{
namespace scheduler
{

ApiTraceCommand::ApiTraceCommand(
  std::shared_ptr<api::stream::TraceInputMessage>&& trace_stream
) : trace_stream_(std::move(trace_stream))
{
}

ApiTraceCommand::~ApiTraceCommand() {
}

std::string ApiTraceCommand::name() const {
  return "API_TRACE";
}

SchedulerCommand::CommandType ApiTraceCommand::type() const {
  return CommandType::GET_NAMESPACE_BY_PATH;
}

const std::string& ApiTraceCommand::namespace_path() const {
  return trace_stream_->root_path();
}

SchedulerCommand::CommandResult ApiTraceCommand::execute_on_namespace(
  config_namespace_t& config_namespace,
  SchedulerQueue& scheduler_queue,
  WorkerQueue& worker_queue
) {
  bool ok = are_valid_arguments(
    trace_stream_->overrides(),
    trace_stream_->flavors(),
    trace_stream_->document(),
    trace_stream_->template_()
  );
  if (!ok) {
    auto output_message = trace_stream_->make_output_message();
    output_message->set_status(api::stream::TraceOutputMessage::Status::ERROR);

    worker_queue.push(
      std::make_unique<worker::ApiReplyCommand>(
        std::move(output_message)
      )
    );

    return CommandResult::OK;
  }

  bool trace_overrides = !trace_stream_->overrides().empty();
  bool trace_flavors = !trace_stream_->flavors().empty();
  bool trace_document = !trace_stream_->document().empty();
  bool trace_template = !trace_stream_->template_().empty();

  if (!trace_overrides && !trace_flavors && !trace_document && !trace_template) {
    // In this case we want to trace all the requests
    config_namespace.to_trace_always.push_back(trace_stream_);
  } else {
    for (const auto& override_ : trace_stream_->overrides()) {
      config_namespace.traces_by_override[override_].push_back(trace_stream_);
    }

    for (const auto& flavor : trace_stream_->flavors()) {
      config_namespace.traces_by_flavor[flavor].push_back(trace_stream_);
    }

    if (trace_document) {
      config_namespace.traces_by_document[trace_stream_->document()].push_back(trace_stream_);
    }

    if (trace_template) {
      config_namespace.traces_by_document[trace_stream_->template_()].push_back(trace_stream_);
    }
  }

  absl::flat_hash_set<std::string> overrides_to_trace;
  for (const auto& override_ : trace_stream_->overrides()) {
    overrides_to_trace.insert(override_);
  }

  absl::flat_hash_set<std::string> flavors_to_trace;
  for (const auto& flavor : trace_stream_->flavors()) {
    flavors_to_trace.insert(flavor);
  }

  std::vector<std::shared_ptr<api::Commitable>> traces;
  traces.reserve(config_namespace.watchers.size());

  uint8_t trace_flags = 0;
  if (trace_overrides) trace_flags |= 1;
  if (trace_flavors) trace_flags |= 2;
  if (trace_document) trace_flags |= 4;
  if (trace_template) trace_flags |= 8;

  for (size_t i = 0; i < config_namespace.watchers.size();) {
    if (auto watcher = config_namespace.watchers[i].lock()) {
      bool trigger_trace = true;

      if (trace_overrides) {
        size_t num_overrides_matches = 0;
        for (const auto& override_ : watcher->overrides()) {
          num_overrides_matches += overrides_to_trace.count(override_);
        }
        trigger_trace = num_overrides_matches == overrides_to_trace.size();
      }

      if (trigger_trace && trace_flavors) {
        size_t num_flavors_matches = 0;
        for (const auto& flavor : watcher->flavors()) {
          num_flavors_matches += flavors_to_trace.count(flavor);
        }
        trigger_trace = num_flavors_matches == flavors_to_trace.size();
      }

      if (trigger_trace && trace_document) {
        trigger_trace = trace_stream_->document() == watcher->document();
      }

      if (trigger_trace && trace_template) {
        trigger_trace = trace_stream_->template_() == watcher->template_();
      }

      if (trigger_trace) {
        traces.push_back(
          make_trace_output_message(
            trace_stream_.get(),
            api::stream::TraceOutputMessage::Status::EXISTING_WATCHER,
            config_namespace.id,
            watcher->version(),
            watcher.get()
          )
        );
      }

      ++i;
    } else {
      jmutils::swap_delete(config_namespace.watchers, i);
    }
  }

  if (!traces.empty()) {
    worker_queue.push(
      std::make_unique<worker::ApiBatchReplyCommand>(
        std::move(traces)
      )
    );
  }

  return CommandResult::OK;
}

bool ApiTraceCommand::on_get_namespace_error(
  WorkerQueue& worker_queue
) {
  auto output_message = trace_stream_->make_output_message();
  output_message->set_status(api::stream::TraceOutputMessage::Status::ERROR);

  worker_queue.push(
    std::make_unique<worker::ApiReplyCommand>(
      std::move(output_message)
    )
  );

  return true;
}

} /* scheduler */
} /* mhconfig */
