// Minimal stand-ins for the runtime's logger and vnet::ip_str, so coop_channel.cpp + coop_stream.cpp build alone.
#include <cstdarg>
#include <cstdio>
#include <string>
#include "nxrt/log.h"
namespace nxrt {
void log_set_thread_name(const char*) {}
bool log_enabled(LogLevel l) { return l <= LogLevel::Info; }
void logf(LogLevel, const char* tag, const char* fmt, ...) {
    va_list ap; va_start(ap, fmt); fprintf(stderr, "  [%s] ", tag); vfprintf(stderr, fmt, ap); fputc('\n', stderr); va_end(ap);
}
}
namespace nxrt::vnet {
std::string ip_str(uint32_t a) { char b[32]; snprintf(b, sizeof b, "%u.%u.%u.%u", a >> 24, (a >> 16) & 255, (a >> 8) & 255, a & 255); return b; }
}
