#ifndef DEBUG_H_
#define DEBUG_H_

#include <ga.h>

#include <string>

using std::string;

void print_zero(const char *fmt, ...);
void print_zero(const string &str);
void print_zero_dummy(const char *fmt, ...);
void print_zero_dummy(const string &str);

void print_sync(const char *fmt, ...);
void print_sync(const string &str);
void print_sync_dummy(const char *fmt, ...);
void print_sync_dummy(const string &str);

#define PRINT_ZERO print_zero
#define PRINT_SYNC print_sync

#if defined(DEBUG) || defined(TRACE)
#   define DEBUG_ZERO print_zero
#   define DEBUG_SYNC print_sync
#else
#   define DEBUG_ZERO print_zero_dummy
#   define DEBUG_SYNC print_sync_dummy
#endif

#ifdef TRACE
#   define TRACER print_sync
#else
#   define TRACER print_sync_dummy
#endif

#endif // DEBUG_H_
