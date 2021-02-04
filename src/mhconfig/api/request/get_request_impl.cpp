#include "mhconfig/api/request/get_request_impl.h"

namespace mhconfig
{
namespace api
{
namespace request
{


GetRequestImpl::~GetRequestImpl() {
}

const std::string& GetRequestImpl::root_path() const {
  return request_->root_path();
}

uint32_t GetRequestImpl::version() const {
  return request_->version();
}

const Labels& GetRequestImpl::labels() const {
  return labels_;
}

const std::string& GetRequestImpl::document() const {
  return request_->document();
}

LogLevel GetRequestImpl::log_level() const {
  switch (request_->log_level()) {
    case proto::LogLevel::ERROR:
      return LogLevel::ERROR;
    case proto::LogLevel::WARN:
      return LogLevel::WARN;
    case proto::LogLevel::INFO:
      return LogLevel::INFO;
    case proto::LogLevel::DEBUG:
      return LogLevel::DEBUG;
    case proto::LogLevel::TRACE:
      return LogLevel::TRACE;
  }
  return LogLevel::ERROR;
}

void GetRequestImpl::set_namespace_id(uint64_t namespace_id) {
  response_->set_namespace_id(namespace_id);
}

void GetRequestImpl::set_version(uint32_t version) {
  response_->set_version(version);
}

void GetRequestImpl::set_element(const mhconfig::Element& element) {
  response_->clear_elements();
  SourceIds source_ids;
  fill_elements(
    element,
    response_,
    response_->add_elements(),
    false,
    source_ids
  );
}

SourceIds GetRequestImpl::set_element_with_position(
  const mhconfig::Element& element
) {
  response_->clear_elements();
  SourceIds source_ids;
  fill_elements(
    element,
    response_,
    response_->add_elements(),
    true,
    source_ids
  );
  return source_ids;
}

void GetRequestImpl::add_log(
  LogLevel level,
  const std::string_view& message
) {
  auto log = response_->add_logs();
  log->set_level(level_to_proto(level));
  log->set_message(message.data(), message.size());
}

void GetRequestImpl::add_log(
  LogLevel level,
  const std::string_view& message,
  const position_t& position
) {
  auto log = response_->add_logs();
  log->set_level(level_to_proto(level));
  log->set_message(message.data(), message.size());
  fill_position(position, log->mutable_position());
}

void GetRequestImpl::add_log(
  LogLevel level,
  const std::string_view& message,
  const position_t& position,
  const position_t& source
) {
  auto log = response_->add_logs();
  log->set_level(level_to_proto(level));
  log->set_message(message.data(), message.size());
  fill_position(position, log->mutable_position());
  fill_position(source, log->mutable_origin());
}

void GetRequestImpl::set_sources(
  const std::vector<source_t>& sources
) {
  response_->clear_sources();
  fill_sources(sources, response_);
}

void GetRequestImpl::set_checksum(const uint8_t* data, size_t len) {
  response_->set_checksum(data, len);
}


bool GetRequestImpl::commit() {
  return finish();
}

void GetRequestImpl::subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  if (auto t = make_tag(GrpcStatus::CREATE)) {
    service->RequestGet(&server_ctx_, request_, &responder_, cq, cq, t);
  }
}

std::shared_ptr<PolicyCheck> GetRequestImpl::on_create(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  make_session<GetRequestImpl>(ctx_)->subscribe(service, cq);
  return nullptr;
}

std::shared_ptr<PolicyCheck> GetRequestImpl::parse_message() {
  labels_ = to_labels(request_->labels());
  return shared_from_this();
}

void GetRequestImpl::on_check_policy(
  auth::AuthResult auth_result,
  auth::Policy* policy
) {
  if (check_auth(auth_result)) {
    auth_result = policy->document_auth(
      auth::Capability::GET,
      root_path(),
      labels()
    );
    if (check_auth(auth_result)) {
      bool ok = validator::are_valid_arguments(
        root_path(),
        labels(),
        document()
      );
      if (ok) {
        auto cn = get_or_build_cn(ctx_.get(), root_path());
        process_get_config_task(
          std::move(cn),
          std::make_shared<ApiGetConfigTask>(shared_from_this()),
          ctx_.get()
        );
      } else {
        finish_with_invalid_argument();
      }
    }
  }
}

void GetRequestImpl::on_check_policy_error() {
  finish_with_unknown();
}

bool GetRequestImpl::finish(const grpc::Status& status) {
  if (auto t = make_tag(GrpcStatus::WRITE)) {
    if (status.ok()) {
      responder_.Finish(*response_, status, t);
    } else {
      responder_.FinishWithError(status, t);
    }
    return true;
  }
  return false;
}

} /* request */
} /* api */
} /* mhconfig */
