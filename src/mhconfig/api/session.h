#ifndef MHCONFIG__API__SESSION_H
#define MHCONFIG__API__SESSION_H

#include <assert.h>
#include <bits/stdint-uintn.h>
#include <google/protobuf/repeated_field.h>
#include <grpcpp/impl/codegen/completion_queue.h>
#include <grpcpp/impl/codegen/server_context.h>
#include <grpcpp/impl/codegen/status.h>
#include <grpcpp/impl/codegen/status_code_enum.h>
#include <grpcpp/impl/codegen/string_ref.h>
#include <spdlog/spdlog.h>
#include <stdint.h>
#include <stdlib.h>
#include <iostream>
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "jmutils/container/label_set.h"
#include "mhconfig/auth/common.h"
#include "mhconfig/config_namespace.h"
#include "mhconfig/context.h"
#include "mhconfig/provider.h"
#include "mhconfig/proto/mhconfig.grpc.pb.h"

namespace mhconfig
{

namespace api
{

using google::protobuf::Arena;

using CustomService = mhconfig::proto::MHConfig::AsyncService;

template <typename T>
std::vector<T> to_vector(const ::google::protobuf::RepeatedPtrField<T>& proto_repeated) {
  std::vector<T> result;
  result.reserve(proto_repeated.size());
  result.insert(result.begin(), proto_repeated.cbegin(), proto_repeated.cend());
  return result;
}

template <typename T>
jmutils::container::Labels to_labels(
  const ::google::protobuf::RepeatedPtrField<T>& proto_repeated
) {
  std::vector<jmutils::container::label_t> labels;
  labels.reserve(proto_repeated.size());
  for (auto& it : proto_repeated) {
    labels.emplace_back(it.key(), it.value());
  }
  return jmutils::container::make_labels(std::move(labels));
}

template <typename T>
void delete_object(T* o) {
  o->~T();
  free(o);
}

template <typename T, typename... Args>
inline std::shared_ptr<T> make_session(Args&&... args)
{
  // This allow use the last 3 bits to store the status of the event
  void* data = aligned_alloc(8, sizeof(T));
  assert (data != nullptr);
  T* ptr = new (data) T(std::forward<Args>(args)...);
  auto session = std::shared_ptr<T>(ptr, delete_object<T>);
  session->init(session);
  return session;
}

class Session
{
public:
  template <typename T>
  Session(
    T&& ctx
  ) : ctx_(std::forward<T>(ctx)) {
  }

  virtual ~Session();

  virtual bool finish(const grpc::Status& status = grpc::Status::OK) = 0;

  bool finish_with_unauthenticated();
  bool finish_with_unknown();
  bool finish_with_invalid_argument();

private:
  std::shared_ptr<Session> this_shared_{nullptr};
  uint32_t cq_refcount_{0};
  bool closed_{false};

  inline std::shared_ptr<Session> decrement_cq_refcount() {
    std::shared_ptr<Session> tmp{nullptr};
    if (--cq_refcount_ == 0) {
      closed_ = true;
      tmp.swap(this_shared_);
      spdlog::trace("Destroying the gRPC event {}", (void*) this);
      on_destroy();
    }
    return tmp;
  }

  std::shared_ptr<Session> proceed(
    uint8_t status,
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  );

protected:
  template <typename T, typename... Args>
  friend std::shared_ptr<T> make_session(Args&&... args);

  friend class Service;

  enum class GrpcStatus {
    CREATE = 0,
    READ = 1,
    WRITE = 2,
    CLOSE = 7,
  };

  absl::Mutex mutex_;
  std::shared_ptr<context_t> ctx_;
  grpc::ServerContext server_ctx_;

  void init(std::shared_ptr<Session> this_shared) {
    this_shared_ = std::move(this_shared);
    if (auto t = make_tag_locked(GrpcStatus::CLOSE)) {
      server_ctx_.AsyncNotifyWhenDone(t);
    }
  }

  virtual void subscribe(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) = 0;
  virtual std::shared_ptr<PolicyCheck> on_create(
    CustomService* service,
    grpc::ServerCompletionQueue* cq
  ) = 0;
  virtual void on_write() = 0;
  virtual std::shared_ptr<PolicyCheck> parse_message() = 0;

  void do_policy_check(
    std::string&& token,
    std::shared_ptr<PolicyCheck>&& policy_check
  );

  virtual void on_destroy() {};

  std::shared_ptr<Session> error();

  inline void* make_tag(GrpcStatus status) {
    mutex_.Lock();
    void* t = make_tag_locked(status);
    mutex_.Unlock();
    return t;
  }

  inline void* make_tag_locked(GrpcStatus status) {
    if (closed_) return nullptr;
    ++cq_refcount_;
    return (void*) (((uintptr_t) this_shared_.get()) | static_cast<uint8_t>(status));
  }

  std::optional<std::string> get_auth_token();

  bool check_auth(auth::AuthResult auth_result);
};

} /* api */
} /* mhconfig */

#endif
