#include "mhconfig/api/stream/watch_stream_impl.h"

namespace mhconfig
{
class Element;

namespace api
{
namespace stream
{

WatchOutputMessageImpl::WatchOutputMessageImpl(
  std::weak_ptr<WatchStreamImpl>& stream
)
  : stream_(stream)
{
  proto_response_ = Arena::CreateMessage<mhconfig::proto::WatchResponse>(&arena_);
}

WatchOutputMessageImpl::~WatchOutputMessageImpl() {
}

void WatchOutputMessageImpl::set_uid(uint32_t uid) {
  proto_response_->set_uid(uid);
}

void WatchOutputMessageImpl::set_status(WatchStatus status) {
  switch (status) {
    case WatchStatus::OK:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_OK);
      break;
    case WatchStatus::ERROR:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_ERROR);
      break;
    case WatchStatus::INVALID_VERSION:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_INVALID_VERSION);
      break;
    case WatchStatus::REF_GRAPH_IS_NOT_DAG:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_REF_GRAPH_IS_NOT_DAG);
      break;
    case WatchStatus::UID_IN_USE:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_UID_IN_USE);
      break;
    case WatchStatus::UNKNOWN_UID:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_UNKNOWN_UID);
      break;
    case WatchStatus::REMOVED:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_REMOVED);
      break;
    case WatchStatus::PERMISSION_DENIED:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_PERMISSION_DENIED);
      break;
    case WatchStatus::INVALID_ARGUMENT:
      proto_response_->set_status(::mhconfig::proto::WatchResponse_Status::WatchResponse_Status_INVALID_ARGUMENT);
      break;
  }
}

void WatchOutputMessageImpl::set_namespace_id(uint64_t namespace_id) {
  proto_response_->set_namespace_id(namespace_id);
}

void WatchOutputMessageImpl::set_version(uint32_t version) {
  proto_response_->set_version(version);
}

void WatchOutputMessageImpl::set_element(const mhconfig::Element& element) {
  mhconfig::api::config::fill_elements(
    element,
    proto_response_,
    proto_response_->add_elements()
  );
}

void WatchOutputMessageImpl::set_checksum(const uint8_t* data, size_t len) {
  proto_response_->set_checksum(data, len);
}

void WatchOutputMessageImpl::set_preprocessed_payload(const char* data, size_t len) {
  preprocessed_payload_.write(data, len);
}

bool WatchOutputMessageImpl::send(bool finish) {
  if (auto stream = stream_.lock()) {
    if (proto_response_->SerializeToOstream(&preprocessed_payload_)) {
      slice_ = grpc::Slice(preprocessed_payload_.str());
      response_ = grpc::ByteBuffer(&slice_, 1);
      return stream->send(shared_from_this(), finish);
    }
  }
  return false;
}


WatchInputMessageImpl::WatchInputMessageImpl(
  std::unique_ptr<mhconfig::proto::WatchRequest>&& request,
  std::weak_ptr<WatchStreamImpl>&& stream
)
  : request_(std::move(request)),
    stream_(std::move(stream)),
    labels_(to_labels(request_->labels()))
{
}

WatchInputMessageImpl::~WatchInputMessageImpl() {
}

uint32_t WatchInputMessageImpl::uid() const {
  return request_->uid();
}

bool WatchInputMessageImpl::remove() const {
  return request_->remove();
}

const std::string& WatchInputMessageImpl::root_path() const {
  return request_->root_path();
}

const Labels& WatchInputMessageImpl::labels() const {
  return labels_;
}

const std::string& WatchInputMessageImpl::document() const {
  return request_->document();
}

std::optional<std::optional<uint64_t>> WatchInputMessageImpl::unregister() {
  if (auto stream = stream_.lock()) {
    return std::optional<std::optional<uint64_t>>(
      stream->unregister(uid())
    );
  }
  return std::optional<uint64_t>();
}

std::shared_ptr<WatchOutputMessage> WatchInputMessageImpl::make_output_message() {
  auto msg = std::make_shared<WatchOutputMessageImpl>(stream_);
  msg->set_uid(request_->uid());
  return msg;
}

void WatchInputMessageImpl::on_check_policy(
  auth::AuthResult auth_result,
  auth::Policy* policy
) {
  if (check_auth(auth_result)) {
    if (remove()) {
      auth_result = policy->basic_auth(
        auth::Capability::WATCH
      );
      if (check_auth(auth_result)) {
        spdlog::debug(
          "Removing watcher with uid {} of the stream {}",
          uid(),
          (void*) this
        );

        auto msg = make_output_message();
        if (auto namespace_id = unregister()) {
          if (*namespace_id) {
            msg->set_namespace_id(**namespace_id);
            msg->set_status(WatchStatus::REMOVED);
          } else {
            msg->set_status(WatchStatus::UNKNOWN_UID);
          }
        } else {
          msg->set_status(WatchStatus::ERROR);
        }
        msg->send();
      }
    } else {
      auto auth_res = policy->document_auth(
        auth::Capability::WATCH,
        root_path(),
        labels()
      );
      if (check_auth(auth_res)) {
        bool ok = validator::are_valid_arguments(
          root_path(),
          labels(),
          document()
        );

        if (ok) {
          if (auto stream = stream_.lock()) {
            stream->register_(shared_from_this());
          } else {
            auto msg = make_output_message();
            msg->set_status(WatchStatus::ERROR);
            msg->send();
          }
        } else {
          auto msg = make_output_message();
          msg->set_status(WatchStatus::INVALID_ARGUMENT);
          msg->send();
        }
      }
    }
  }
}

void WatchInputMessageImpl::on_check_policy_error() {
  auto msg = make_output_message();
  msg->set_status(WatchStatus::ERROR);
  msg->send();
}

bool WatchInputMessageImpl::check_auth(auth::AuthResult auth_result) {
  switch (auth_result) {
    case auth::AuthResult::AUTHENTICATED:
      return true;
    case auth::AuthResult::EXPIRED_TOKEN:  // Fallback
    case auth::AuthResult::UNAUTHENTICATED: {
      if (auto stream = stream_.lock()) {
        stream->finish_with_unauthenticated();
      } else {
        auto msg = make_output_message();
        msg->set_status(WatchStatus::ERROR);
        msg->send();
      }
      break;
    }
    case auth::AuthResult::PERMISSION_DENIED: {
      auto msg = make_output_message();
      msg->set_status(WatchStatus::PERMISSION_DENIED);
      msg->send();
      break;
    }
  }
  return false;
}

WatchStreamImpl::~WatchStreamImpl() {
}

void WatchStreamImpl::subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  if (auto t = make_tag(GrpcStatus::CREATE)) {
    service->RequestWatch(&server_ctx_, &stream_, cq, cq, t);
  }
}

std::shared_ptr<PolicyCheck> WatchStreamImpl::on_create(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  make_session<WatchStreamImpl>(ctx_)->subscribe(service, cq);
  return shared_from_this();
}

std::shared_ptr<PolicyCheck> WatchStreamImpl::parse_message() {
  auto req = std::make_unique<mhconfig::proto::WatchRequest>();
  auto status = grpc::SerializationTraits<mhconfig::proto::WatchRequest>::Deserialize(
    &next_req_,
    req.get()
  );
  if (!status.ok()) return nullptr;

  prepare_next_request();

  return std::make_shared<WatchInputMessageImpl>(
    std::move(req),
    shared_from_this()
  );
}

void WatchStreamImpl::on_check_policy(
  auth::AuthResult auth_result,
  auth::Policy* policy
) {
  spdlog::trace("Checking watch policy: {} {}", (uint32_t)auth_result, (void*) policy);
  if (check_auth(auth_result)) {
    auth_result = policy->basic_auth(
      auth::Capability::WATCH
    );
    if (check_auth(auth_result)) {
      prepare_next_request();
    }
  }
}

void WatchStreamImpl::on_check_policy_error() {
  finish_with_unknown();
}

void WatchStreamImpl::on_destroy() {
  absl::flat_hash_map<
    std::string,
    std::vector<std::shared_ptr<WatchInputMessage>>
  > watchers_by_root_path;

  mutex_.Lock();
  for (auto& it : watcher_by_id_) {
    watchers_by_root_path[it.second->root_path()].push_back(std::move(it.second));
  }
  watcher_by_id_.clear();
  mutex_.Unlock();

  for (auto& it : watchers_by_root_path) {
    if (auto cn = get_cn(ctx_.get(), it.first)) {
      for (size_t i = 0, l = it.second.size(); i < l; ++i) {
        on_removed_watcher(cn.get(), it.second[i].get());
      }
    }
  }
}

void WatchStreamImpl::register_(
  std::shared_ptr<WatchInputMessage>&& msg
) {
  spdlog::debug(
    "Adding watcher with uid {} to the stream {}",
    msg->uid(),
    (void*) this
  );
  mutex_.Lock();
  auto inserted = watcher_by_id_.try_emplace(msg->uid(), msg);
  mutex_.Unlock();
  if (inserted.second) {
    auto cn = get_or_build_cn(ctx_.get(), msg->root_path());
    process_watch_request(
      std::move(cn),
      std::move(msg),
      ctx_.get()
    );
  } else {
    auto out_msg = msg->make_output_message();
    out_msg->set_status(WatchStatus::UID_IN_USE);
    out_msg->send();
  }
}

std::optional<uint64_t> WatchStreamImpl::unregister(uint32_t uid) {
  mutex_.Lock();
  auto node = watcher_by_id_.extract(uid);
  mutex_.Unlock();

  if (node) {
    if (auto cn = get_cn(ctx_.get(), node.mapped()->root_path())) {
      on_removed_watcher(cn.get(), node.mapped().get());
      return std::optional<uint64_t>(cn->id);
    }
  }
  return std::optional<uint64_t>();
}

void WatchStreamImpl::on_removed_watcher(
  config_namespace_t* cn,
  const WatchInputMessage* request
) {
  for_each_trace_to_trigger(
    cn,
    request,
    [cn, request](auto* trace) {
      auto om = make_trace_output_message(
        trace,
        TraceOutputMessage::Status::REMOVED_WATCHER,
        cn->id,
        0,
        request
      );
      om->commit();
    }
  );
}

} /* stream */
} /* api */
} /* mhconfig */
