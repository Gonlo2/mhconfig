#ifndef MHCONFIG__LOGGER__MULTI_PERSISTENT_LOGGER_H
#define MHCONFIG__LOGGER__MULTI_PERSISTENT_LOGGER_H

#include "mhconfig/logger/replay_logger.h"
#include "mhconfig/logger/persistent_logger.h"
#include "jmutils/container/linked_list.h"

#define define_multi_persistent_logger_method(LEVEL) \
  void LEVEL( \
    const char* message \
  ) override { \
    get_last_logger()->LEVEL(message); \
  } \
\
  void LEVEL( \
    const char* message, \
    const Element& element \
  ) override { \
    get_last_logger()->LEVEL(message, element); \
  } \
\
  void LEVEL( \
    const char* message, \
    const Element& element, \
    const Element& origin \
  ) override { \
    get_last_logger()->LEVEL(message, element, origin); \
  } \
\
  void LEVEL( \
    jmutils::string::String&& message \
  ) override { \
    get_last_logger()->LEVEL(std::move(message)); \
  } \
\
  void LEVEL( \
    jmutils::string::String&& message, \
    const Element& element \
  ) override { \
    get_last_logger()->LEVEL(std::move(message), element); \
  } \
\
  void LEVEL( \
    jmutils::string::String&& message, \
    const Element& element, \
    const Element& origin \
  ) override { \
    get_last_logger()->LEVEL(std::move(message), element, origin); \
  }

namespace mhconfig
{
namespace logger
{

class MultiPersistentLogger final : public ReplayLogger
{
public:
  define_multi_persistent_logger_method(error)
  define_multi_persistent_logger_method(warn)
  define_multi_persistent_logger_method(info)
  define_multi_persistent_logger_method(debug)
  define_multi_persistent_logger_method(trace)

  void replay(
    Logger& logger,
    Level lower_or_equal_that = Level::trace
  ) const override {
    for (auto& x : loggers_) {
      x->replay(logger, lower_or_equal_that);
    }
    if (last_logger_ != nullptr) {
      last_logger_->replay(logger, lower_or_equal_that);
    }
  }

  void inject_back(
    const MultiPersistentLogger& logger
  ) {
    if (!logger.empty()) {
      move_back_if_not_empty();
      for (auto& x : logger.loggers_) {
        loggers_.push_back(x);
      }
      if ((logger.last_logger_ != nullptr) && !logger.last_logger_->empty()) {
        loggers_.push_back(logger.last_logger_);
      }
    }
  }

  void push_back(
    const std::shared_ptr<ReplayLogger>& logger
  ) {
    if (!logger->empty()) {
      move_back_if_not_empty();
      loggers_.push_back(logger);
    }
  }

  void remove_duplicates() {
    absl::flat_hash_set<std::shared_ptr<ReplayLogger>> unique_loggers;
    loggers_.remove_if(
      [&unique_loggers](const auto& logger) -> bool {
        return !unique_loggers.insert(logger).second;
      }
    );
  }

  bool empty() const override {
    return loggers_.empty() && ((last_logger_ == nullptr) || last_logger_->empty());
  }

private:
  jmutils::container::LinkedList<std::shared_ptr<ReplayLogger>> loggers_;
  std::shared_ptr<ReplayLogger> last_logger_{nullptr};

  inline void move_back_if_not_empty() {
    if ((last_logger_ != nullptr) && !last_logger_->empty()) {
      loggers_.push_back(std::move(last_logger_));
      last_logger_ = nullptr;
    }
  }

  inline ReplayLogger* get_last_logger() {
    if (last_logger_ == nullptr) {
      last_logger_ = std::make_shared<PersistentLogger>();
    }
    return last_logger_.get();
  }
};

} /* logger */
} /* mhconfig */

#endif
