#include "mhconfig/provider.h"

namespace mhconfig
{
  void send_existing_watcher_traces(
    config_namespace_t* cn,
    document_versions_t* dv,
    api::stream::TraceInputMessage* trace,
    absl::flat_hash_set<std::string>& overrides,
    absl::flat_hash_set<std::string>& flavors
  ) {
    dv->watchers.for_each(
      [cn, trace, &overrides, &flavors](auto&& watcher) {
        bool trigger = true;

        if (!overrides.empty()) {
          size_t matches = 0;
          for (const auto& override_ : watcher->overrides()) {
            matches += overrides.count(override_);
          }
          trigger = matches == overrides.size();
        }

        if (trigger && !flavors.empty()) {
          size_t matches = 0;
          for (const auto& flavor : watcher->flavors()) {
            matches += flavors.count(flavor);
          }
          trigger = matches == flavors.size();
        }

        if (trigger) {
          auto om = make_trace_output_message(
            trace,
            api::stream::TraceOutputMessage::Status::EXISTING_WATCHER,
            cn->id,
            0,
            watcher.get()
          );
          om->commit();
        }
      }
    );
  }
} /* mhconfig */
