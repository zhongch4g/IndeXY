#pragma once

#ifndef LOGGER_H
#define LOGGER_H

#include <pthread.h>
#include <sys/time.h>

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <sstream>

class PosixLogger {
public:
    // Creates a logger that writes to the given file.
    //
    // The PosixLogger instance takes ownership of the file handle.
    explicit PosixLogger (std::FILE* fp) : fp_ (fp) { assert (fp != nullptr); }

    ~PosixLogger () { std::fclose (fp_); }

    static inline PosixLogger* instance = nullptr;

    static PosixLogger* getInstance () {
        if (instance == nullptr) {
            instance = new PosixLogger (openFile ());
        }
        return instance;
    }

    static void Destroy () { delete instance; }

    static std::FILE* openFile () {
        // std::string filename = "log" + std::to_string(time(0)) + ".log";
        std::string filename = "log.log";
        std::FILE* fp = std::fopen (filename.c_str (), "w");
        return fp;
    }

    void Logv (const char* format, va_list arguments) {
        // Record the time as close to the Logv() call as possible.
        struct ::timeval now_timeval;
        ::gettimeofday (&now_timeval, nullptr);
        const std::time_t now_seconds = now_timeval.tv_sec;
        struct std::tm now_components;
        ::localtime_r (&now_seconds, &now_components);

        // Record the thread ID.
        constexpr const int kMaxThreadIdSize = 32;
        std::ostringstream thread_stream;
        thread_stream << pthread_self ();
        std::string thread_id = thread_stream.str ();
        if (thread_id.size () > kMaxThreadIdSize) {
            thread_id.resize (kMaxThreadIdSize);
        }

        // We first attempt to print into a stack-allocated buffer. If this attempt
        // fails, we make a second attempt with a dynamically allocated buffer.
        constexpr const int kStackBufferSize = 512;
        char stack_buffer[kStackBufferSize];
        static_assert (sizeof (stack_buffer) == static_cast<size_t> (kStackBufferSize),
                       "sizeof(char) is expected to be 1 in C++");

        int dynamic_buffer_size = 0;  // Computed in the first iteration.
        for (int iteration = 0; iteration < 2; ++iteration) {
            const int buffer_size = (iteration == 0) ? kStackBufferSize : dynamic_buffer_size;
            char* const buffer = (iteration == 0) ? stack_buffer : new char[dynamic_buffer_size];

            // Print the header into the buffer.
            int buffer_offset =
                snprintf (buffer, buffer_size, "[%04d-%02d-%02d %02d:%02d:%02d.%06d][%s] ",
                          now_components.tm_year + 1900, now_components.tm_mon + 1,
                          now_components.tm_mday, now_components.tm_hour, now_components.tm_min,
                          now_components.tm_sec, static_cast<int> (now_timeval.tv_usec),
                          thread_id.substr (thread_id.size () - 5).c_str ());

            // The header can be at most 28 characters (10 date + 15 time +
            // 3 spacing) plus the thread ID, which should fit comfortably into the
            // static buffer.
            // assert(buffer_offset <= 28 + kMaxThreadIdSize);
            static_assert (28 + kMaxThreadIdSize < kStackBufferSize,
                           "stack-allocated buffer may not fit the message header");
            assert (buffer_offset < buffer_size);

            // Print the message into the buffer.
            std::va_list arguments_copy;
            va_copy (arguments_copy, arguments);
            buffer_offset += std::vsnprintf (buffer + buffer_offset, buffer_size - buffer_offset,
                                             format, arguments_copy);
            va_end (arguments_copy);

            // The code below may append a newline at the end of the buffer, which
            // requires an extra character.
            if (buffer_offset >= buffer_size - 1) {
                // The message did not fit into the buffer.
                if (iteration == 0) {
                    // Re-run the loop and use a dynamically-allocated buffer. The buffer
                    // will be large enough for the log message, an extra newline and a
                    // null terminator.
                    dynamic_buffer_size = buffer_offset + 2;
                    continue;
                }

                // The dynamically-allocated buffer was incorrectly sized. This should
                // not happen, assuming a correct implementation of (v)snprintf. Fail
                // in tests, recover by truncating the log message in production.
                assert (false);
                buffer_offset = buffer_size - 1;
            }

            // Add a newline if necessary.
            if (buffer[buffer_offset - 1] != '\n') {
                buffer[buffer_offset] = '\n';
                ++buffer_offset;
            }

            assert (buffer_offset <= buffer_size);
            std::fwrite (buffer, 1, buffer_offset, fp_);
            std::fflush (fp_);

            if (iteration != 0) {
                delete[] buffer;
            }
            break;
        }
    }

private:
    std::FILE* const fp_;
};

inline void Log (PosixLogger* info_log, const char* format, ...) {
    if (info_log != nullptr) {
        va_list ap;
        va_start (ap, format);
        info_log->Logv (format, ap);
        va_end (ap);
    }
}

#define __FILENAME__ ((strrchr (__FILE__, '/') ? strrchr (__FILE__, '/') + 1 : __FILE__))

#define INFO(M, ...)                                                               \
    do {                                                                           \
        char buffer[1024] = "[INFO] ";                                             \
        sprintf (buffer + 8, "[%s %s:%d] ", __FILENAME__, __FUNCTION__, __LINE__); \
        sprintf (buffer + strlen (buffer), M, ##__VA_ARGS__);                      \
        Log (PosixLogger::getInstance (), "%s", buffer);                           \
    } while (0);

#ifndef NDEBUG
#define DEBUG(M, ...)                                                              \
    do {                                                                           \
        char buffer[1024] = "[DEBUG] ";                                            \
        sprintf (buffer + 8, "[%s %s:%d] ", __FILENAME__, __FUNCTION__, __LINE__); \
        sprintf (buffer + strlen (buffer), M, ##__VA_ARGS__);                      \
        Log (PosixLogger::getInstance (), "%s", buffer);                           \
    } while (0);
#else
#define DEBUG(M, ...) \
    do {              \
    } while (0);
#endif

#define ERROR(M, ...)                                                              \
    do {                                                                           \
        char buffer[1024] = "[ERROR] ";                                            \
        sprintf (buffer + 8, "[%s %s:%d] ", __FILENAME__, __FUNCTION__, __LINE__); \
        sprintf (buffer + strlen (buffer), M, ##__VA_ARGS__);                      \
        Log (PosixLogger::getInstance (), "%s", buffer);                           \
    } while (0);

#define WARNING(M, ...)                                                                          \
    do {                                                                                         \
        char buffer[1024] = "[ WARN] ";                                                          \
        sprintf (buffer + strlen (buffer), "[%s %s:%d] ", __FILENAME__, __FUNCTION__, __LINE__); \
        sprintf (buffer + strlen (buffer), M, ##__VA_ARGS__);                                    \
        Log (logger_, "%s", buffer);                                                             \
    } while (0);

#endif