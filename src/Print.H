#ifndef PRINT_H_
#define PRINT_H_

#include <sstream>
#include <string>
#include <vector>

using std::ostringstream;
using std::string;
using std::vector;

#define TAG_PRINT 77448

namespace pagoda
{

    void print_zero(const char *fmt, ...);
    void print_zero(const string &str);
    void print_zero(FILE *stream, const char *fmt, ...);
    void print_zero(FILE *stream, const string &str);

    void println_zero(const char *fmt, ...);
    void println_zero(const string &str);
    void println_zero(FILE *stream, const char *fmt, ...);
    void println_zero(FILE *stream, const string &str);

    void print_zero_dummy(const char *fmt, ...);
    void print_zero_dummy(const string &str);
    void print_zero_dummy(FILE *stream, const char *fmt, ...);
    void print_zero_dummy(FILE *stream, const string &str);

    void println_zero_dummy(const char *fmt, ...);
    void println_zero_dummy(const string &str);
    void println_zero_dummy(FILE *stream, const char *fmt, ...);
    void println_zero_dummy(FILE *stream, const string &str);

    void print_sync(const char *fmt, ...);
    void print_sync(const string &str);
    void print_sync(FILE *stream, const char *fmt, ...);
    void print_sync(FILE *stream, const string &str);

    void println_sync(const char *fmt, ...);
    void println_sync(const string &str);
    void println_sync(FILE *stream, const char *fmt, ...);
    void println_sync(FILE *stream, const string &str);

    void print_sync_dummy(const char *fmt, ...);
    void print_sync_dummy(const string &str);
    void print_sync_dummy(FILE *stream, const char *fmt, ...);
    void print_sync_dummy(FILE *stream, const string &str);

    void println_sync_dummy(const char *fmt, ...);
    void println_sync_dummy(const string &str);
    void println_sync_dummy(FILE *stream, const char *fmt, ...);
    void println_sync_dummy(FILE *stream, const string &str);

    template <class Collection>
    string vec_to_string(
        const Collection &collection,
        char const * delimiter=",",
        const string &name="")
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
        for (; beg != end; ++beg) {
            os << delimiter << *beg;
        }
        os << "}";

        return os.str();
    }

}

#endif // PRINT_H_