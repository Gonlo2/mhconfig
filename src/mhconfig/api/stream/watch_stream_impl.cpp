#include "mhconfig/api/stream/watch_stream_impl.h"

namespace mhconfig
{
namespace api
{
namespace stream
{

WatchOutputMessageImpl::WatchOutputMessageImpl(
  std::weak_ptr<WatchStreamImpl>& stream
)
  : stream_(stream)
{
  proto_response_ = google::protobuf::Arena::CreateMessage<mhconfig::proto::WatchResponse>(&arena_);
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
  }
}

void WatchOutputMessageImpl::set_namespace_id(uint64_t namespace_id) {
  proto_response_->set_namespace_id(namespace_id);
}

void WatchOutputMessageImpl::set_version(uint32_t version) {
  proto_response_->set_version(version);
}

void WatchOutputMessageImpl::set_element(const mhconfig::Element& element) {
  elements_data_.clear();
  proto_response_->clear_elements();
  mhconfig::api::config::fill_elements(
    element,
    proto_response_,
    proto_response_->add_elements()
  );
}

void WatchOutputMessageImpl::set_element_bytes(const char* data, size_t len) {
  elements_data_.clear();
  elements_data_.write(data, len);
  proto_response_->clear_elements();
}

void WatchOutputMessageImpl::set_template_rendered(const std::string& data) {
  proto_response_->set_template_rendered(data);
}

bool WatchOutputMessageImpl::send(bool finish) {
  if (auto stream = stream_.lock()) {
    if (proto_response_->SerializeToOstream(&elements_data_)) {
      slice_ = grpc::Slice(elements_data_.str());
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
    overrides_(to_vector(request_->overrides())),
    flavors_(to_vector(request_->flavors()))
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

const std::vector<std::string>& WatchInputMessageImpl::overrides() const {
  return overrides_;
}

const std::vector<std::string>& WatchInputMessageImpl::flavors() const {
  return flavors_;
}

uint32_t WatchInputMessageImpl::version() const {
  return request_->version();
}

const std::string& WatchInputMessageImpl::document() const {
  return request_->document();
}

const std::string& WatchInputMessageImpl::template_() const {
  return request_->template_();
}

bool WatchInputMessageImpl::unregister() {
  if (auto stream = stream_.lock()) {
    return stream->unregister(uid());
  }
  return false;
}

std::string WatchInputMessageImpl::peer() const {
  if (auto stream = stream_.lock()) {
    return stream->session_peer();
  }
  return "";
}

std::shared_ptr<WatchOutputMessage> WatchInputMessageImpl::make_output_message() {
  auto msg = std::make_shared<WatchOutputMessageImpl>(stream_);
  msg->set_uid(request_->uid());
  return msg;
}

WatchGetRequest::WatchGetRequest(
  std::shared_ptr<WatchInputMessage> input_message,
  std::shared_ptr<WatchOutputMessage> output_message
)
  : input_message_(input_message),
  output_message_(output_message)
{
}

WatchGetRequest::~WatchGetRequest() {
}

const std::string& WatchGetRequest::root_path() const {
  return input_message_->root_path();
}

uint32_t WatchGetRequest::version() const {
  return input_message_->version();
}

const std::vector<std::string>& WatchGetRequest::overrides() const {
  return input_message_->overrides();
}

const std::vector<std::string>& WatchGetRequest::flavors() const {
  return input_message_->flavors();
}

const std::string& WatchGetRequest::document() const {
  return input_message_->document();
}

const std::string& WatchGetRequest::template_() const {
  return input_message_->template_();
}

void WatchGetRequest::set_status(::mhconfig::api::request::GetRequest::Status status) {
  switch (status) {
    case ::mhconfig::api::request::GetRequest::Status::OK:
      output_message_->set_status(WatchStatus::OK);
      break;
    case ::mhconfig::api::request::GetRequest::Status::ERROR:
      output_message_->set_status(WatchStatus::ERROR);
      break;
    case ::mhconfig::api::request::GetRequest::Status::INVALID_VERSION:
      output_message_->set_status(WatchStatus::INVALID_VERSION);
      break;
    case ::mhconfig::api::request::GetRequest::Status::REF_GRAPH_IS_NOT_DAG:
      output_message_->set_status(WatchStatus::REF_GRAPH_IS_NOT_DAG);
      break;
  }
}

void WatchGetRequest::set_namespace_id(uint64_t namespace_id) {
  output_message_->set_namespace_id(namespace_id);
}

void WatchGetRequest::set_version(uint32_t version) {
  output_message_->set_version(version);
}

void WatchGetRequest::set_element(const mhconfig::Element& element) {
  output_message_->set_element(element);
}

void WatchGetRequest::set_element_bytes(const char* data, size_t len) {
  output_message_->set_element_bytes(data, len);
}

void WatchGetRequest::set_template_rendered(const std::string& data) {
  output_message_->set_template_rendered(data);
}

bool WatchGetRequest::commit() {
  return output_message_->commit();
}

std::string WatchGetRequest::peer() const {
  return input_message_->peer();
}

WatchStreamImpl::WatchStreamImpl()
  : Stream()
{
}

WatchStreamImpl::~WatchStreamImpl() {
}

const std::string WatchStreamImpl::name() const {
  return "WATCH";
}

void WatchStreamImpl::clone_and_subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  return make_session<WatchStreamImpl>()->subscribe(service, cq);
}

void WatchStreamImpl::subscribe(
  CustomService* service,
  grpc::ServerCompletionQueue* cq
) {
  if (auto t = tag(Status::CREATE)) {
    service->RequestWatch(&ctx_, &stream_, cq, cq, t);
  }
}

bool WatchStreamImpl::unregister(uint32_t uid) {
  return unregister(scheduler_sender_, uid);
}

void WatchStreamImpl::on_create(
  SchedulerQueue::Sender* scheduler_sender
) {
  mutex_.Lock();
  scheduler_sender_ = scheduler_sender;
  mutex_.Unlock();
  prepare_next_request();
}

void WatchStreamImpl::on_read(
  SchedulerQueue::Sender* scheduler_sender
) {
  auto req = std::make_unique<mhconfig::proto::WatchRequest>();
  auto status = grpc::SerializationTraits<mhconfig::proto::WatchRequest>::Deserialize(
    &next_req_,
    req.get()
  );
  auto msg = std::make_shared<WatchInputMessageImpl>(std::move(req), shared_from_this());
  if (status.ok()) {
    if (msg->remove()) {
      spdlog::debug("Removing watcher with uid {} of the stream {}", msg->uid(), (void*) this);
      bool removed = unregister(scheduler_sender, msg->uid());
      auto out_msg = msg->make_output_message();
      out_msg->set_status(removed ? WatchStatus::REMOVED : WatchStatus::UNKNOWN_UID);
      out_msg->send();
    } else {
      spdlog::debug("Adding watcher with uid {} to the stream {}", msg->uid(), (void*) this);
      mutex_.Lock();
      auto inserted = watcher_by_id_.try_emplace(msg->uid(), msg);
      mutex_.Unlock();
      if (inserted.second) {
        scheduler_sender->push(
          std::make_unique<scheduler::ApiWatchCommand>(
            msg
          )
        );
      } else {
        auto out_msg = msg->make_output_message();
        out_msg->set_status(WatchStatus::UID_IN_USE);
        out_msg->send();
      }
    }
  } else {
    auto out_msg = msg->make_output_message();
    out_msg->set_status(WatchStatus::ERROR);
    out_msg->send(true); // We probably don't know the uid so we need to finish the stream u.u
  }

  prepare_next_request();
}

void WatchStreamImpl::on_destroy(
  SchedulerQueue::Sender* scheduler_sender,
  metrics::MetricsService& metrics
) {
  absl::flat_hash_map<std::string, std::vector<std::shared_ptr<WatchInputMessage>>> watchers_by_root_path;

  mutex_.Lock();
  for (auto& it : watcher_by_id_) {
    watchers_by_root_path[it.second->root_path()].push_back(std::move(it.second));
  }
  mutex_.Unlock();

  for (auto& it : watchers_by_root_path) {
    scheduler_sender->push(
      std::make_unique<scheduler::OnWatchersRemovedCommand>(
        it.first,
        std::move(it.second)
      )
    );
  }
}

bool WatchStreamImpl::unregister(SchedulerQueue::Sender* scheduler_sender, uint32_t uid) {
  mutex_.Lock();
  auto node = watcher_by_id_.extract(uid);
  mutex_.Unlock();
  if (node && (scheduler_sender != nullptr)) {
    scheduler_sender->push(
      std::make_unique<scheduler::OnWatchersRemovedCommand>(
        std::move(node.mapped())
      )
    );
  }
  return node.operator bool();
}


} /* stream */
} /* api */
} /* mhconfig */
