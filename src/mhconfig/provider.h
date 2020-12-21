#ifndef MHCONFIG__PROVIDER_H
#define MHCONFIG__PROVIDER_H

#include <bits/stdint-uintn.h>
#include <memory>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "jmutils/container/label_set.h"
#include "mhconfig/api/request/get_request.h"
#include "mhconfig/api/request/update_request.h"
#include "mhconfig/api/stream/trace_stream.h"
#include "mhconfig/api/stream/watch_stream.h"
#include "mhconfig/auth/common.h"
#include "mhconfig/auth/tokens.h"
#include "mhconfig/builder.h"
#include "mhconfig/config_namespace.h"
#include "mhconfig/worker/build_command.h"
#include "mhconfig/worker/optimize_command.h"
#include "mhconfig/worker/setup_command.h"
#include "mhconfig/worker/update_command.h"

namespace mhconfig
{

using jmutils::container::label_t;
using mhconfig::api::request::GetRequest;

class ApiGetConfigTask final : public GetConfigTask
{
public:
  template <typename T>
  ApiGetConfigTask(
    T&& request
  ) : request_(std::forward<T>(request)) {
  };
  ~ApiGetConfigTask();

  const std::string& root_path() const override;
  uint32_t version() const override;
  const Labels& labels() const override;
  const std::string& document() const override;

  void on_complete(
    Status status,
    std::shared_ptr<config_namespace_t>& cn,
    VersionId version,
    const Element& element,
    void* payload
  ) override;

private:
  std::shared_ptr<GetRequest> request_;

  GetRequest::Status to_api_status(Status status);
};

bool process_get_config_task(
  std::shared_ptr<config_namespace_t>&& cn,
  std::shared_ptr<GetConfigTask>&& task,
  context_t* ctx
);

bool process_update_request(
  std::shared_ptr<config_namespace_t>&& cn,
  std::shared_ptr<api::request::UpdateRequest>&& request,
  context_t* ctx
);

bool process_watch_request(
  std::shared_ptr<config_namespace_t>&& cn,
  std::shared_ptr<api::stream::WatchInputMessage>&& request,
  context_t* ctx
);

void send_existing_watcher_traces(
  config_namespace_t* cn,
  document_versions_t* dv,
  api::stream::TraceInputMessage* trace,
  absl::flat_hash_set<label_t>& labels
);

bool process_trace_request(
  std::shared_ptr<config_namespace_t>&& cn,
  std::shared_ptr<api::stream::TraceInputMessage>&& request,
  context_t* ctx
);

class AuthPolicyGetConfigTask final : public GetConfigTask
{
public:
  AuthPolicyGetConfigTask(
    VersionId version,
    Labels&& labels,
    std::string&& root_path,
    std::shared_ptr<PolicyCheck>&& pc
  );
  ~AuthPolicyGetConfigTask();

  const std::string& root_path() const override;
  uint32_t version() const override;
  const Labels& labels() const override;
  const std::string& document() const override;

  void on_complete(
    Status status,
    std::shared_ptr<config_namespace_t>& cn,
    VersionId version,
    const Element& element,
    void* payload
  ) override;

private:
  VersionId version_;
  Labels labels_;
  std::string root_path_;
  std::shared_ptr<PolicyCheck> pc_;
};

class AuthTokenGetConfigTask final : public GetConfigTask
{
public:
  AuthTokenGetConfigTask(
    std::string& root_path,
    std::string&& token,
    std::shared_ptr<PolicyCheck>&& pc,
    std::shared_ptr<context_t>& ctx
  );
  ~AuthTokenGetConfigTask();

  const std::string& root_path() const override;
  uint32_t version() const override;
  const Labels& labels() const override;
  const std::string& document() const override;

  void on_complete(
    Status status,
    std::shared_ptr<config_namespace_t>& cn,
    VersionId version,
    const Element& element,
    void* payload
  ) override;

private:
  std::string root_path_;
  std::string token_;
  std::shared_ptr<PolicyCheck> pc_;
  std::shared_ptr<context_t> ctx_;
};

} /* mhconfig */

#endif
