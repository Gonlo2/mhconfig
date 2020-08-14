#ifndef MHCONFIG__GC_H
#define MHCONFIG__GC_H

#include "mhconfig/config_namespace.h"
#include "mhconfig/command.h"
#include "mhconfig/builder.h"
#include "jmutils/common.h"

namespace mhconfig
{

void gc_cn(
  context_t* ctx,
  uint64_t timelimit_s
);

void gc_cn_dead_pointers(
  config_namespace_t* cn
);

void gc_cn_raw_config_versions(
  config_namespace_t* cn,
  uint64_t timelimit_s
);

bool need_clean_cn_raw_config_versions(
  config_namespace_t* cn,
  uint64_t timelimit_s,
  VersionId& oldest_version
);

bool gc_document_raw_config_versions(
  document_t* document,
  VersionId oldest_version
);

void gc_document_merged_configs(
  document_t* document,
  uint8_t generation,
  uint64_t timelimit_s
);

void gc_cn_merged_configs(
  config_namespace_t* cn,
  uint8_t generation,
  uint64_t timelimit_s
);

void gc_document_merged_configs(
  document_t* document,
  uint8_t generation,
  uint64_t timelimit_s
);

struct GCMergedConfigResult {
  int32_t processed{0};
  int32_t removed{0};
  std::shared_ptr<merged_config_t> last;
  std::shared_ptr<merged_config_t> next_last;
};

int32_t count_same_gc_merged_config(
  merged_config_t* mc,
  uint64_t timelimit_s,
  bool has_next
);

GCMergedConfigResult gc_merged_config(
  std::shared_ptr<merged_config_t>&& src,
  uint64_t timelimit_s,
  bool has_next
);

enum class MCGeneration {
  SAME,
  NEXT,
  NONE
};

inline MCGeneration obtain_mc_generation(
  merged_config_t* mc,
  uint64_t timelimit_s,
  bool has_next
) {
  if(
    ((mc->status == MergedConfigStatus::OK_CONFIG_NO_OPTIMIZED)
      || (mc->status == MergedConfigStatus::OK_CONFIG_OPTIMIZING)
      || (mc->status == MergedConfigStatus::OK_CONFIG_OPTIMIZED))
    && (mc->creation_timestamp <= timelimit_s)
  ) {
    if (mc->last_access_timestamp <= timelimit_s) {
      return MCGeneration::NONE;
    }
    return has_next ? MCGeneration::NEXT : MCGeneration::SAME;
  }
  return MCGeneration::SAME;
}

} /* mhconfig */

#endif
