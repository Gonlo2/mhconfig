#ifndef MHCONFIG__LOGGER__PERSISTENT_LOGGER_H
#define MHCONFIG__LOGGER__PERSISTENT_LOGGER_H

#include "mhconfig/logger/replay_logger.h"
#include "jmutils/aligned_ptr.h"

#define LOGGER_CHUNK_SIZE 5

#define define_persistent_logger_method(LEVEL) \
    void LEVEL( \
        const char* message \
    ) override { \
        log_t* log = new_log(static_cast<size_t>(Level::LEVEL)); \
        log->call_type = CallType::STATIC_STR; \
        log->str.static_ = message; \
    } \
\
    void LEVEL( \
        const char* message, \
        const Element& element \
    ) override { \
        log_t* log = new_log(static_cast<size_t>(Level::LEVEL)); \
        log->call_type = CallType::STATIC_STR_POSITION; \
        log->position_document_id = element.document_id(); \
        log->position_raw_config_id = element.raw_config_id(); \
        log->position_line = element.line(); \
        log->position_col = element.col(); \
        log->str.static_ = message; \
    } \
\
    void LEVEL( \
        const char* message, \
        const Element& element, \
        const Element& origin \
    ) override { \
        log_t* log = new_log(static_cast<size_t>(Level::LEVEL)); \
        log->call_type = CallType::STATIC_STR_POSITION_ORIGIN; \
        log->position_document_id = element.document_id(); \
        log->position_raw_config_id = element.raw_config_id(); \
        log->position_line = element.line(); \
        log->position_col = element.col(); \
        log->origin_col = origin.col(); \
        log->origin_line = origin.line(); \
        log->origin_raw_config_id = origin.raw_config_id(); \
        log->origin_document_id = origin.document_id(); \
        log->str.static_ = message; \
    } \
\
    void LEVEL( \
        jmutils::string::String&& message \
    ) override { \
        log_t* log = new_log(static_cast<size_t>(Level::LEVEL)); \
        log->call_type = CallType::DINAMIC_STR, \
        new (&log->str.dinamic) jmutils::string::String(std::move(message)); \
    } \
\
    void LEVEL( \
        jmutils::string::String&& message, \
        const Element& element \
    ) override { \
        log_t* log = new_log(static_cast<size_t>(Level::LEVEL)); \
        log->call_type = CallType::DINAMIC_STR_POSITION; \
        log->position_document_id = element.document_id(); \
        log->position_raw_config_id = element.raw_config_id(); \
        log->position_line = element.line(); \
        log->position_col = element.col(); \
        new (&log->str.dinamic) jmutils::string::String(std::move(message)); \
    } \
\
    void LEVEL( \
        jmutils::string::String&& message, \
        const Element& element, \
        const Element& origin \
    ) override { \
        log_t* log = new_log(static_cast<size_t>(Level::LEVEL)); \
        log->call_type = CallType::DINAMIC_STR_POSITION_ORIGIN; \
        log->position_document_id = element.document_id(); \
        log->position_raw_config_id = element.raw_config_id(); \
        log->position_line = element.line(); \
        log->position_col = element.col(); \
        log->origin_col = origin.col(); \
        log->origin_line = origin.line(); \
        log->origin_raw_config_id = origin.raw_config_id(); \
        log->origin_document_id = origin.document_id(); \
        new (&log->str.dinamic) jmutils::string::String(std::move(message)); \
    }

#define define_persistent_logger_replay_method(LOG, POSITION, ORIGIN, LEVEL, LOGGER) \
    switch (LOG->call_type) { \
        case CallType::STATIC_STR: \
            LOGGER.LEVEL(LOG->str.static_); \
            break; \
        case CallType::STATIC_STR_POSITION: \
            LOGGER.LEVEL(LOG->str.static_, POSITION); \
            break; \
        case CallType::STATIC_STR_POSITION_ORIGIN: \
            LOGGER.LEVEL(LOG->str.static_, POSITION, ORIGIN); \
            break; \
        case CallType::DINAMIC_STR: { \
            auto s = LOG->str.dinamic.str(); \
            LOGGER.LEVEL(s.c_str()); \
            break; \
        } \
        case CallType::DINAMIC_STR_POSITION: { \
            auto s = LOG->str.dinamic.str(); \
            LOGGER.LEVEL(s.c_str(), POSITION); \
            break; \
        } \
        case CallType::DINAMIC_STR_POSITION_ORIGIN: { \
            auto s = LOG->str.dinamic.str(); \
            LOGGER.LEVEL(s.c_str(), POSITION, ORIGIN); \
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
    PersistentLogger() {
        for (size_t i = 0; i < LOGGER_NUM_LEVELS; ++i) {
            heads_[i] = LogChunkPtr(7);
        }
    }

    ~PersistentLogger() {
        for (size_t i = 0; i < LOGGER_NUM_LEVELS; ++i) {
            delete_log_chunk(heads_[i]);
        }
    }

    define_persistent_logger_method(error)
    define_persistent_logger_method(warn)
    define_persistent_logger_method(debug)
    define_persistent_logger_method(trace)

    void replay(
        Logger& logger,
        Level lower_or_equal_that = Level::trace
    ) const override {
        size_t le_lvl = static_cast<size_t>(lower_or_equal_that);
        size_t lvl = heads_[le_lvl].value();
        if (lvl == 7) return;

        log_chunk_t* log_chunk_by_lvl[LOGGER_NUM_LEVELS];
        uint8_t log_idx_by_lvl[LOGGER_NUM_LEVELS];
        for (size_t i = 0; i <= le_lvl; ++i) {
            log_chunk_by_lvl[i] = heads_[i].ptr();
            log_idx_by_lvl[i] = 0;
        }

        Element position;
        Element origin;

        while (true) {
            log_chunk_t* log_chunk = log_chunk_by_lvl[lvl];
            spdlog::trace("Reading the log {} of the level {} and offset {}",
                          (void*) log_chunk, lvl, (size_t) log_idx_by_lvl[lvl]);
            log_t* log = &log_chunk->logs[log_idx_by_lvl[lvl]];
            if (log_idx_by_lvl[lvl] == LOGGER_CHUNK_SIZE-1) {
                log_chunk_by_lvl[lvl] = log_chunk->next.ptr();
                log_idx_by_lvl[lvl] = 0;
            } else {
                log_idx_by_lvl[lvl] += 1;
            }

            position.set_position(log->position_line, log->position_col);
            position.set_document_id(log->position_document_id);
            position.set_raw_config_id(log->position_raw_config_id);

            origin.set_position(log->origin_line, log->origin_col);
            origin.set_document_id(log->origin_document_id);
            origin.set_raw_config_id(log->origin_raw_config_id);

            switch (static_cast<Level>(lvl)) {
                case Level::error:
                    define_persistent_logger_replay_method(log, position, origin, error, logger)
                    break;
                case Level::warn:
                    define_persistent_logger_replay_method(log, position, origin, warn, logger)
                    break;
                case Level::debug:
                    define_persistent_logger_replay_method(log, position, origin, debug, logger)
                    break;
                case Level::trace:
                    define_persistent_logger_replay_method(log, position, origin, trace, logger)
                    break;
            }

            if (log->start > le_lvl) break;
            lvl = (log->next >> get_next_offset(le_lvl)) & LOGGER_NUM_LEVELS_MASK;
        }
    }

    void freeze() override {
        reverse();
    }

    void reverse() {
        for (size_t i = 0; i < LOGGER_NUM_LEVELS; ++i) {
            log_chunk_t* prev = heads_[i].ptr();
            if (prev != nullptr) {
                log_chunk_t* ptr = prev->next.ptr();
                prev->next.ptr(nullptr);
                while (ptr != nullptr) {
                    log_chunk_t* next = ptr->next.ptr();
                    ptr->next.ptr(prev);
                    prev = ptr;
                    ptr = next;
                }
                heads_[i].ptr(prev);
            }
        }
    }

    void change_all(
        uint16_t document_id,
        uint16_t raw_config_id
    ) {
        for (size_t i = 0; i < LOGGER_NUM_LEVELS; ++i) {
            log_chunk_t* ptr = heads_[i].ptr();
            while (ptr != nullptr) {
                for (size_t i = 0, l = ptr->next.value(); i < l; ++i) {
                    ptr->logs[i].position_document_id = document_id;
                    ptr->logs[i].position_raw_config_id = raw_config_id;
                }
                ptr = ptr->next.ptr();
            }
        }
    }

    bool empty() const override {
        return heads_[LOGGER_NUM_LEVELS-1].value() == 7;
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
        str_t str;
        CallType call_type : 4;
        uint8_t start: 4;
        uint8_t next;

        uint16_t position_document_id;
        uint16_t position_raw_config_id;
        uint16_t position_line;
        uint8_t position_col;

        uint8_t origin_col;
        uint16_t origin_line;
        uint16_t origin_raw_config_id;
        uint16_t origin_document_id;
    };

    struct log_chunk_t;

    using LogChunkPtr = jmutils::AlignedPtr<3, log_chunk_t, size_t>;

    struct log_chunk_t {
        LogChunkPtr next;
        log_t logs[LOGGER_CHUNK_SIZE];
    };

    LogChunkPtr heads_[LOGGER_NUM_LEVELS];

    log_t* new_log(size_t lvl) {
        for (size_t i = lvl; i < LOGGER_NUM_LEVELS; ++i) {
            if (heads_[i].value() == 7) {
                heads_[i].value(lvl);
            }
        }

        for (size_t i = 0; i < LOGGER_NUM_LEVELS; ++i) {
            auto ptr = heads_[i].ptr();
            if (ptr != nullptr) {
                size_t j = ptr->next.value()-1;
                for (size_t k = lvl, l = ptr->logs[j].start; k < l; ++k) {
                    ptr->logs[j].next |= lvl << get_next_offset(k);
                }
                if (lvl < ptr->logs[j].start) {
                    ptr->logs[j].start = lvl;
                }
            }
        }

        auto head = heads_[lvl].ptr();
        // If this is the first chunk of if the chunk if full we create a new one
        if ((head == nullptr) || (head->next.value() == LOGGER_CHUNK_SIZE)) {
            heads_[lvl] = jmutils::new_aligned_ptr<3, log_chunk_t, size_t>(heads_[lvl].value());
            heads_[lvl].ptr()->next.ptr(head);
            head = heads_[lvl].ptr();
        }
        log_t* log = &head->logs[head->next.value()];
        log->start = LOGGER_NUM_LEVELS;
        log->next = 0;
        head->next.incr();
        return log;
    }

    void delete_log_chunk(LogChunkPtr head) {
        while (true) {
            log_chunk_t* ptr = head.ptr();
            if (ptr == nullptr) break;
            for (size_t i = 0, l = ptr->next.value(); i < l; ++i) {
                delete_log(ptr->logs[i]);
            }
            LogChunkPtr next = ptr->next;
            head.destroy();
            head = next;
        }
    }

    void delete_log(log_t& log) {
        switch (log.call_type) {
            case CallType::STATIC_STR:
            case CallType::STATIC_STR_POSITION:
            case CallType::STATIC_STR_POSITION_ORIGIN:
                break;
            case CallType::DINAMIC_STR:
            case CallType::DINAMIC_STR_POSITION:
            case CallType::DINAMIC_STR_POSITION_ORIGIN:
                log.str.dinamic.~String();
                break;
        }
    }

    inline size_t get_next_offset(size_t lvl) const {
        return lvl * LOGGER_NUM_LEVELS_BITS;
    }
};

} /* logger */
} /* mhconfig */

#endif
