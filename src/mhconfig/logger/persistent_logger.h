#ifndef MHCONFIG__LOGGER__PERSISTENT_LOGGER_H
#define MHCONFIG__LOGGER__PERSISTENT_LOGGER_H

#include "mhconfig/logger/replay_logger.h"

#define define_persistent_logger_method(LEVEL) \
  void LEVEL( \
    const char* message \
  ) override { \
    log_t* log = new log_t{ \
      .level = Level::LEVEL, \
      .call_type = CallType::STATIC_STR, \
    }; \
    log->str.static_ = message; \
    insert(log); \
  } \
\
  void LEVEL( \
    const char* message, \
    const Element& element \
  ) override { \
    log_t* log = new log_t{ \
      .level = Level::LEVEL, \
      .call_type = CallType::STATIC_STR_POSITION, \
      .position_document_id = element.document_id(), \
      .position_raw_config_id = element.raw_config_id(), \
      .position_line = element.line(), \
      .position_col = element.col(), \
    }; \
    log->str.static_ = message; \
    insert(log); \
  } \
\
  void LEVEL( \
    const char* message, \
    const Element& element, \
    const Element& origin \
  ) override { \
    log_t* log = new log_t{ \
      .level = Level::LEVEL, \
      .call_type = CallType::STATIC_STR_POSITION_ORIGIN, \
      .position_document_id = element.document_id(), \
      .position_raw_config_id = element.raw_config_id(), \
      .position_line = element.line(), \
      .position_col = element.col(), \
      .origin_col = origin.col(), \
      .origin_line = origin.line(), \
      .origin_raw_config_id = origin.raw_config_id(), \
      .origin_document_id = origin.document_id(), \
    }; \
    log->str.static_ = message; \
    insert(log); \
  } \
\
  void LEVEL( \
    jmutils::string::String&& message \
  ) override { \
    log_t* log = new log_t{ \
      .level = Level::LEVEL, \
      .call_type = CallType::DINAMIC_STR, \
    }; \
    new (&log->str.dinamic) jmutils::string::String(std::move(message)); \
    insert(log); \
  } \
\
  void LEVEL( \
    jmutils::string::String&& message, \
    const Element& element \
  ) override { \
    log_t* log = new log_t{ \
      .level = Level::LEVEL, \
      .call_type = CallType::DINAMIC_STR_POSITION, \
      .position_document_id = element.document_id(), \
      .position_raw_config_id = element.raw_config_id(), \
      .position_line = element.line(), \
      .position_col = element.col(), \
    }; \
    new (&log->str.dinamic) jmutils::string::String(std::move(message)); \
    insert(log); \
  } \
\
  void LEVEL( \
    jmutils::string::String&& message, \
    const Element& element, \
    const Element& origin \
  ) override { \
    log_t* log = new log_t{ \
      .level = Level::LEVEL, \
      .call_type = CallType::DINAMIC_STR_POSITION_ORIGIN, \
      .position_document_id = element.document_id(), \
      .position_raw_config_id = element.raw_config_id(), \
      .position_line = element.line(), \
      .position_col = element.col(), \
      .origin_col = origin.col(), \
      .origin_line = origin.line(), \
      .origin_raw_config_id = origin.raw_config_id(), \
      .origin_document_id = origin.document_id(), \
    }; \
    new (&log->str.dinamic) jmutils::string::String(std::move(message)); \
    insert(log); \
  }

#define define_persistent_logger_replay_method(LEVEL) \
  switch (log->call_type) { \
    case CallType::STATIC_STR: \
      logger.LEVEL(log->str.static_); \
      break; \
    case CallType::STATIC_STR_POSITION: \
      logger.LEVEL(log->str.static_, position); \
      break; \
    case CallType::STATIC_STR_POSITION_ORIGIN: \
      logger.LEVEL(log->str.static_, position, origin); \
      break; \
    case CallType::DINAMIC_STR: { \
      auto s = log->str.dinamic.str(); \
      logger.LEVEL(s.c_str()); \
      break; \
    } \
    case CallType::DINAMIC_STR_POSITION: { \
      auto s = log->str.dinamic.str(); \
      logger.LEVEL(s.c_str(), position); \
      break; \
    } \
    case CallType::DINAMIC_STR_POSITION_ORIGIN: { \
      auto s = log->str.dinamic.str(); \
      logger.LEVEL(s.c_str(), position, origin); \
      break; \
    } \
  }

namespace mhconfig
{
namespace logger
{

class PersistentLogger final : public ReplayLogger
{
public:
  ~PersistentLogger() {
    log_t* log = firsts_[LOGGER_NUM_LEVELS-1];
    while (log != nullptr) {
      log_t* next_log = log->next_le[LOGGER_NUM_LEVELS-1];
      delete_log(log);
      log = next_log;
    }
  }

  define_persistent_logger_method(error)
  define_persistent_logger_method(warn)
  define_persistent_logger_method(info)
  define_persistent_logger_method(debug)
  define_persistent_logger_method(trace)

  void replay(
    Logger& logger,
    Level lower_or_equal_that = Level::trace
  ) const override {
    Element position;
    Element origin;

    size_t le_level = static_cast<size_t>(lower_or_equal_that);
    for (log_t* log = firsts_[le_level]; log != nullptr; log = log->next_le[le_level]) {
      position.set_position(log->position_line, log->position_col);
      position.set_document_id(log->position_document_id);
      position.set_raw_config_id(log->position_raw_config_id);

      origin.set_position(log->origin_line, log->origin_col);
      origin.set_document_id(log->origin_document_id);
      origin.set_raw_config_id(log->origin_raw_config_id);

      switch (log->level) {
        case Level::error:
          define_persistent_logger_replay_method(error)
          break;
        case Level::warn:
          define_persistent_logger_replay_method(warn)
          break;
        case Level::info:
          define_persistent_logger_replay_method(info)
          break;
        case Level::debug:
          define_persistent_logger_replay_method(debug)
          break;
        case Level::trace:
          define_persistent_logger_replay_method(trace)
          break;
      }
    }
  }

  void change_all(
    uint16_t document_id,
    uint16_t raw_config_id
  ) {
    for (
      log_t* log = firsts_[LOGGER_NUM_LEVELS-1];
      log != nullptr;
      log = log->next_le[LOGGER_NUM_LEVELS-1]
    ) {
      log->position_document_id = document_id;
      log->position_raw_config_id = raw_config_id;
    }
  }

  bool empty() const override {
    return firsts_[LOGGER_NUM_LEVELS-1] == nullptr;
  }

private:
  enum class CallType : uint8_t {
    STATIC_STR = 0,
    STATIC_STR_POSITION = 1,
    STATIC_STR_POSITION_ORIGIN = 2,
    DINAMIC_STR = 3,
    DINAMIC_STR_POSITION = 4,
    DINAMIC_STR_POSITION_ORIGIN = 5
  };

  union str_t {
    const char* static_;
    jmutils::string::String dinamic;

    str_t() noexcept {}
    ~str_t() noexcept {}
  };

  struct log_t {
    log_t* next_le[LOGGER_NUM_LEVELS] = {nullptr, nullptr, nullptr, nullptr, nullptr};

    str_t str;
    Level level;
    CallType call_type;

    uint16_t position_document_id;
    uint16_t position_raw_config_id;
    uint16_t position_line;
    uint8_t position_col;

    uint8_t origin_col;
    uint16_t origin_line;
    uint16_t origin_raw_config_id;
    uint16_t origin_document_id;
  };

  log_t* firsts_[LOGGER_NUM_LEVELS] = {nullptr, nullptr, nullptr, nullptr, nullptr};
  log_t* lasts_[LOGGER_NUM_LEVELS] = {nullptr, nullptr, nullptr, nullptr, nullptr};

  void insert(log_t* log) {
    size_t log_level = static_cast<size_t>(log->level);

    for (size_t le_level = log_level; le_level < LOGGER_NUM_LEVELS; ++le_level) {
      if (firsts_[le_level] == nullptr) {
        firsts_[le_level] = log;
      }
    }

    for (size_t level = 0; level < LOGGER_NUM_LEVELS; ++level) {
      if (lasts_[level] != nullptr) {
        for (size_t le_level = log_level; le_level < LOGGER_NUM_LEVELS; ++le_level) {
          if (lasts_[level]->next_le[le_level] == nullptr) {
            lasts_[level]->next_le[le_level] = log;
          }
        }
      }
    }

    lasts_[log_level] = log;
  }

  inline void delete_log(log_t* log) {
    switch (log->call_type) {
      case CallType::STATIC_STR:
      case CallType::STATIC_STR_POSITION:
      case CallType::STATIC_STR_POSITION_ORIGIN:
        break;
      case CallType::DINAMIC_STR:
      case CallType::DINAMIC_STR_POSITION:
      case CallType::DINAMIC_STR_POSITION_ORIGIN:
        log->str.dinamic.~String();
        break;
    }
    delete log;
  }
};


} /* logger */
} /* mhconfig */

#endif
