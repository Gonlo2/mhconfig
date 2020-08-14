#ifndef MHCONFIG__SCHEDULER__COMMON_H
#define MHCONFIG__SCHEDULER__COMMON_H

#include <vector>
#include <string>
#include <memory>

#include "mhconfig/api/stream/trace_stream.h"
#include "mhconfig/config_namespace.h"
#include "mhconfig/builder.h"

#include "spdlog/spdlog.h"


namespace mhconfig
{
namespace scheduler
{

namespace {
  struct trace_variables_counter_t {
    bool document : 1;
    uint16_t flavors : 15;
    uint16_t overrides;

    trace_variables_counter_t()
      : document(false),
      flavors(0),
      overrides(0)
    {
    }
  };
}

template <typename T>
std::shared_ptr<api::stream::TraceOutputMessage> make_trace_output_message(
  api::stream::TraceInputMessage* input_message,
  api::stream::TraceOutputMessage::Status status,
  uint64_t namespace_id,
  uint32_t version,
  T* message
) {
  auto output_message = input_message->make_output_message();

  output_message->set_status(status);
  output_message->set_namespace_id(namespace_id);
  output_message->set_version(version);
  output_message->set_overrides(message->overrides());
  output_message->set_flavors(message->flavors());
  output_message->set_document(message->document());
  output_message->set_peer(message->peer());

  return output_message;
}

template <typename T, typename F>
void for_each_trace_to_trigger(
  config_namespace_t& config_namespace,
  const T* message,
  F lambda
) {
  absl::flat_hash_map<
    std::shared_ptr<api::stream::TraceInputMessage>,
    trace_variables_counter_t
  > match_by_trace;

  if (!config_namespace.traces_by_override.empty()) {
    for (size_t i = 0, l = message->overrides().size(); i < l; ++i) {
      auto search = config_namespace.traces_by_override
        .find(message->overrides()[i]);

      if (search != config_namespace.traces_by_override.end()) {
        for (size_t j = 0; j < search->second.size();) {
          if (auto trace = search->second[j].lock()) {
            match_by_trace[trace].overrides += 1;
            ++j;
          } else {
            jmutils::swap_delete(search->second, j);
          }
        }

        if (search->second.empty()) {
          config_namespace.traces_by_override.erase(search);
        }
      }
    }
  }

  if (!config_namespace.traces_by_flavor.empty()) {
    for (size_t i = 0, l = message->flavors().size(); i < l; ++i) {
      auto search = config_namespace.traces_by_flavor
        .find(message->flavors()[i]);

      if (search != config_namespace.traces_by_flavor.end()) {
        for (size_t j = 0; j < search->second.size();) {
          if (auto trace = search->second[j].lock()) {
            match_by_trace[trace].flavors += 1;
            ++j;
          } else {
            jmutils::swap_delete(search->second, j);
          }
        }

        if (search->second.empty()) {
          config_namespace.traces_by_flavor.erase(search);
        }
      }
    }
  }

  if (!message->document().empty()){
    auto search = config_namespace.traces_by_document
      .find(message->document());

    if (search != config_namespace.traces_by_document.end()) {
      for (size_t i = 0; i < search->second.size();) {
        if (auto trace = search->second[i].lock()) {
          match_by_trace[trace].document = true;
          ++i;
        } else {
          jmutils::swap_delete(search->second, i);
        }
      }

      if (search->second.empty()) {
        config_namespace.traces_by_document.erase(search);
      }
    }
  }

  for (size_t i = 0; i < config_namespace.to_trace_always.size();) {
    if (auto trace = config_namespace.to_trace_always[i].lock()) {
      lambda(config_namespace.id, message, trace.get());
      ++i;
    } else {
      jmutils::swap_delete(config_namespace.to_trace_always, i);
    }
  }

  for (auto& it : match_by_trace) {
    bool trigger_trace = true;

    if (!it.first->overrides().empty()) {
      trigger_trace = it.first->overrides().size() == it.second.overrides;
    }
    if (trigger_trace && !it.first->flavors().empty()) {
      trigger_trace = it.first->flavors().size() == it.second.flavors;
    }
    if (trigger_trace && !it.first->document().empty()) {
      trigger_trace = it.second.document;
    }

    if (trigger_trace) {
      lambda(config_namespace.id, message, it.first.get());
    }
  }
}

} /* scheduler */
} /* mhconfig */

#endif
