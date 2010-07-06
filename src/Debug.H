#ifndef DEBUG_H_
#define DEBUG_H_

#include <sstream>
#include <string>
#include <vector>

using std::ostringstream;
using std::string;
using std::vector;

#define TAG_DEBUG 33284

void print_zero(const char *fmt, ...);
void print_zero(const string &str);
void print_zero_dummy(const char *fmt, ...);
void print_zero_dummy(const string &str);

void print_sync(const char *fmt, ...);
void print_sync(const string &str);
void print_sync_dummy(const char *fmt, ...);
void print_sync_dummy(const string &str);

template <class Collection>
static inline string vec_to_string(
        const Collection &collection,
        char const * delimiter,
        const string &name)
{
    typedef typename Collection::const_iterator iter;
    std::ostringstream os;
    iter beg = collection.begin();
    iter end = collection.end();

    if (beg == end) {
        return "{}";
    }

    if (!name.empty()) {
        os << name << "=";
    }
    os << "{" << *(beg++);
    for ( ; beg != end; ++beg) {
        os << delimiter << *beg;
    }
    os << "}\n";

    return os.str();
}

template <class Collection>
void vec_print_zero(const Collection &collection, const string &name="")
{
    print_zero(vec_to_string(collection, ",", name));
}


template <class Collection>
void vec_print_zero_dummy(const Collection &collection, const string &name="")
{
}

template <class Collection>
void vec_print_sync(const Collection &collection, const string &name="")
{
    print_sync(vec_to_string(collection, ",", name));
}

template <class Collection>
void vec_print_sync_dummy(const Collection &collection, const string &name="")
{
}

#define     PRINT_ZERO     print_zero
#define     PRINT_SYNC     print_sync
#define VEC_PRINT_ZERO vec_print_zero
#define VEC_PRINT_SYNC vec_print_sync

#if defined(DEBUG) || defined(TRACE)
#   define     DEBUG_ZERO     print_zero
#   define     DEBUG_SYNC     print_sync
#   define VEC_DEBUG_ZERO vec_print_zero
#   define VEC_DEBUG_SYNC vec_print_sync
#else
#   define     DEBUG_ZERO     print_zero_dummy
#   define     DEBUG_SYNC     print_sync_dummy
#   define VEC_DEBUG_ZERO vec_print_zero_dummy
#   define VEC_DEBUG_SYNC vec_print_sync_dummy
#endif

#ifdef TRACE
#   define TRACER print_sync
#else
#   define TRACER print_sync_dummy
#endif

#endif // DEBUG_H_
