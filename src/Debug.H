#ifndef DEBUG_H_
#define DEBUG_H_

#include <sstream>
#include <string>
#include <vector>

using std::ostringstream;
using std::string;
using std::vector;

#define TAG_DEBUG 33284

namespace pagoda {

void print_zero(const char *fmt, ...);
void print_zero(const string &str);
void print_zero(FILE *stream, const char *fmt, ...);
void print_zero(FILE *stream, const string &str);

void print_zero_dummy(const char *fmt, ...);
void print_zero_dummy(const string &str);
void print_zero_dummy(FILE *stream, const char *fmt, ...);
void print_zero_dummy(FILE *stream, const string &str);

void print_sync(const char *fmt, ...);
void print_sync(const string &str);
void print_sync(FILE *stream, const char *fmt, ...);
void print_sync(FILE *stream, const string &str);

void print_sync_dummy(const char *fmt, ...);
void print_sync_dummy(const string &str);
void print_sync_dummy(FILE *stream, const char *fmt, ...);
void print_sync_dummy(FILE *stream, const string &str);

template <class Collection>
string vec_to_string(
        const Collection &collection,
        char const * delimiter,
        const string &name)
{
    typedef typename Collection::const_iterator iter;
    std::ostringstream os;
    iter beg = collection.begin();
    iter end = collection.end();

    if (!name.empty()) {
        os << name << "=";
    }

    if (beg == end) {
        return "{}";
    }

    os << "{" << *(beg++);
    for ( ; beg != end; ++beg) {
        os << delimiter << *beg;
    }
    os << "}\n";

    return os.str();
}

}

#if defined(DEBUG) || defined(TRACE)
#   define     DEBUG_ZERO pagoda::print_zero
#   define     DEBUG_SYNC pagoda::print_sync
#   define VEC_DEBUG_ZERO pagoda::vec_print_zero
#   define VEC_DEBUG_SYNC pagoda::vec_print_sync
#else
#   define     DEBUG_ZERO pagoda::print_zero_dummy
#   define     DEBUG_SYNC pagoda::print_sync_dummy
#   define VEC_DEBUG_ZERO pagoda::vec_print_zero_dummy
#   define VEC_DEBUG_SYNC pagoda::vec_print_sync_dummy
#endif

#ifdef TRACE
#   define TRACER pagoda::print_sync
#else
#   define TRACER pagoda::print_sync_dummy
#endif

#endif // DEBUG_H_
