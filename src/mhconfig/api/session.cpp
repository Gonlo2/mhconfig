#include "mhconfig/api/session.h"

namespace mhconfig
{
namespace api
{

Session::~Session() {
}

std::shared_ptr<Session> Session::proceed(
  uint8_t status,
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  mutex_.ReaderLock();
  bool proceed = !closed_;
  mutex_.ReaderUnlock();

  if (proceed) {
    switch (static_cast<GrpcStatus>(status)) {
      case GrpcStatus::CREATE:
        if (auto token = get_auth_token()) {
          if (auto policy_check = on_create(service, cq)) {
            do_policy_check(std::move(*token), std::move(policy_check));
            break;
          }
        } else {
          finish_with_unauthenticated();
          break;
        }
      case GrpcStatus::READ: // Fallback
        if (auto token = get_auth_token()) {
          if (auto policy_check = parse_message()) {
            do_policy_check(std::move(*token), std::move(policy_check));
          } else {
            finish_with_unknown();
          }
        } else {
          finish_with_unauthenticated();
        }
        break;
      case GrpcStatus::WRITE:
        on_write();
        break;
      case GrpcStatus::CLOSE:
        break;
    }
  }

  mutex_.Lock();
  auto result = decrement_cq_refcount();
  mutex_.Unlock();
  return result;
}

void Session::do_policy_check(
  std::string&& token,
  std::shared_ptr<PolicyCheck>&& policy_check
) {
  auto cn = get_or_build_cn(ctx_.get(), ctx_->mhc_root_path);
  process_get_config_task(
    std::move(cn),
    std::make_shared<AuthTokenGetConfigTask>(
      ctx_->mhc_root_path,
      std::move(token),
      std::move(policy_check),
      ctx_
    ),
    ctx_.get()
  );
}

std::shared_ptr<Session> Session::error() {
  mutex_.Lock();
  closed_ = true;
  auto result = decrement_cq_refcount();
  mutex_.Unlock();
  return result;
}

std::optional<std::string> Session::get_auth_token() {
  auto search = server_ctx_.client_metadata().find("mhconfig-auth-token");
  if (search == server_ctx_.client_metadata().end()) {
    return std::optional<std::string>();
  }
  return std::optional<std::string>(
    std::string(search->second.data(), search->second.size())
  );
}

bool Session::check_auth(auth::AuthResult auth_result) {
  switch (auth_result) {
    case auth::AuthResult::AUTHENTICATED:
      return true;
    case auth::AuthResult::EXPIRED_TOKEN:  // Fallback
    case auth::AuthResult::UNAUTHENTICATED:
      finish_with_unauthenticated();
      break;
    case auth::AuthResult::PERMISSION_DENIED:
      finish(
        grpc::Status(
          grpc::StatusCode::PERMISSION_DENIED,
          "The auth token don't have permissions to do this operation"
        )
      );
      break;
  }
  return false;
}

bool Session::finish_with_unauthenticated() {
  return finish(
    grpc::Status(
      grpc::StatusCode::UNAUTHENTICATED,
      "The auth token hasn't been provided or is incorrect"
    )
  );
}

bool Session::finish_with_unknown() {
  return finish(
    grpc::Status(
      grpc::StatusCode::UNKNOWN,
      "Some unknown error take place"
    )
  );
}

bool Session::finish_with_invalid_argument() {
  return finish(
    grpc::Status(
      grpc::StatusCode::INVALID_ARGUMENT,
      "Some of the request parameters are wrong"
    )
  );
}

} /* api */
} /* mhconfig */
