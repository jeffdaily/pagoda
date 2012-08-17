#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <mpi.h>

#include <cstdarg>
#include <cstdio>
#include <string>
#include <inttypes.h>

using std::fprintf;
using std::string;

#include "Bootstrap.H"
#include "Collectives.H"
#include "Print.H"
#include "Util.H"


#define DUMMY_AP(fmt) do { \
    va_list ap; \
    va_start(ap, fmt); \
    va_end(ap); \
} while (0)


static inline int get_precision()
{
    int precision = 1;
    int nproc = pagoda::npe;


    while (nproc > 10) {
        ++precision;
        nproc /= 10;
    }
    return precision;
}


/* not used, but perhaps in the future
static inline string va_to_string(const char *fmt, ...)
{
    size_t size = 200;
    char result[size];
    va_list ap;         // will point to each unnamed argument in turn

    va_start(ap, fmt);  // point to the first element after fmt
    vsnprintf(result, size, fmt, ap);
    va_end(ap);         // cleanup

    return string(result);
}
*/


static inline string va_to_string(const char *fmt, va_list ap)
{
    size_t size = 200;
    char result[size];

    vsnprintf(result, size, fmt, ap);

    return string(result);
}


void pagoda::print_zero(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    print_zero(stderr, va_to_string(fmt, ap));
    va_end(ap);
}


void pagoda::print_zero(const string &str)
{
    print_zero(stderr, str);
}


void pagoda::print_zero(FILE *stream, const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    print_zero(stream, va_to_string(fmt, ap));
    va_end(ap);
}


void pagoda::print_zero(FILE *stream, const string &str)
{
    if (0 == pagoda::me) {
        fprintf(stream, str.c_str());
        fflush(stream);
    }
    pagoda::barrier();
}


void pagoda::println_zero(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    println_zero(stderr, va_to_string(fmt, ap));
    va_end(ap);
}


void pagoda::println_zero(const string &str)
{
    println_zero(stderr, str);
}


void pagoda::println_zero(FILE *stream, const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    println_zero(stream, va_to_string(fmt, ap));
    va_end(ap);
}


void pagoda::println_zero(FILE *stream, const string &str)
{
    print_zero(stream, str+"\n");
}


void pagoda::print_zero_dummy(const char *fmt, ...)
{
    DUMMY_AP(fmt);
}
void pagoda::print_zero_dummy(const string &str) { }
void pagoda::print_zero_dummy(FILE *stream, const char *fmt, ...)
{
    DUMMY_AP(fmt);
}
void pagoda::print_zero_dummy(FILE *stream, const string &str) { }

void pagoda::println_zero_dummy(const char *fmt, ...)
{
    DUMMY_AP(fmt);
}
void pagoda::println_zero_dummy(const string &str) { }
void pagoda::println_zero_dummy(FILE *stream, const char *fmt, ...)
{
    DUMMY_AP(fmt);
}
void pagoda::println_zero_dummy(FILE *stream, const string &str) { }


void pagoda::print_sync(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    print_sync(stderr, va_to_string(fmt, ap));
    va_end(ap);
}


void pagoda::print_sync(const string &str)
{
    print_sync(stderr, str);
}


void pagoda::print_sync(FILE *stream, const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    print_sync(stream, va_to_string(fmt, ap));
    va_end(ap);
}


void pagoda::print_sync(FILE *stream, const string &str)
{
    if (0 == pagoda::me) {
        fprintf(stream, "[%*d] ", get_precision(), 0);
        fprintf(stream, str.c_str());
        for (int proc=1,nproc=pagoda::npe; proc<nproc; ++proc) {
            MPI_Status stat;
            int count;
            char *msg;

            MPI_Recv(&count, 1, MPI_INT, proc, TAG_PRINT, pagoda::COMM_WORLD,
                     &stat);
            msg = new char[count];
            MPI_Recv(msg, count, MPI_CHAR, proc, TAG_PRINT, pagoda::COMM_WORLD,
                     &stat);
            fprintf(stream, "[%*d] ", get_precision(), proc);
            fprintf(stream, msg);
            delete [] msg;
        }
        fflush(stream);
    }
    else {
        int count = str.size() + 1;

        MPI_Send(&count, 1, MPI_INT, 0, TAG_PRINT, pagoda::COMM_WORLD);
        MPI_Send((void*)str.c_str(), count, MPI_CHAR, 0, TAG_PRINT,
                 pagoda::COMM_WORLD);
    }
}


void pagoda::println_sync(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    println_sync(stderr, va_to_string(fmt, ap));
    va_end(ap);
}


void pagoda::println_sync(const string &str)
{
    println_sync(stderr, str);
}


void pagoda::println_sync(FILE *stream, const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    println_sync(stream, va_to_string(fmt, ap));
    va_end(ap);
}


void pagoda::println_sync(FILE *stream, const string &str)
{
    print_sync(stream, str+"\n");
}


void pagoda::print_sync_dummy(const char *fmt, ...)
{
    DUMMY_AP(fmt);
}
void pagoda::print_sync_dummy(const string &str) { }
void pagoda::print_sync_dummy(FILE *stream, const char *fmt, ...)
{
    DUMMY_AP(fmt);
}
void pagoda::print_sync_dummy(FILE *stream, const string &str) { }

void pagoda::println_sync_dummy(const char *fmt, ...)
{
    DUMMY_AP(fmt);
}
void pagoda::println_sync_dummy(const string &str) { }
void pagoda::println_sync_dummy(FILE *stream, const char *fmt, ...)
{
    DUMMY_AP(fmt);
}
void pagoda::println_sync_dummy(FILE *stream, const string &str) { }
