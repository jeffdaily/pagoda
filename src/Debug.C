#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <mpi.h>

#include <cstdarg>
#include <cstdio>
#include <string>

using std::fprintf;
using std::string;

#include "Debug.H"
#include "Timing.H"
#include "Util.H"


#define DUMMY_AP(fmt) do { \
    va_list ap; \
    va_start(ap, fmt); \
    va_end(ap); \
} while (0)


static inline int get_precision()
{
    int precision = 1;
    int nproc = pagoda::num_nodes();

    TIMING("get_precision()");

    while (nproc > 10) {
        ++precision;
        nproc /= 10;
    }
    return precision;
}


/* not used, but perhaps in the future
static inline string to_string(const char *fmt, ...)
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


static inline string to_string(const char *fmt, va_list ap)
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
    print_zero(stderr, to_string(fmt, ap));
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
    print_zero(stream, to_string(fmt, ap));
    va_end(ap);
}


void pagoda::print_zero(FILE *stream, const string &str)
{
    if (0 == pagoda::nodeid()) {
        fprintf(stream, str.c_str());
        fflush(stream);
    }
    pagoda::barrier();
}


void pagoda::print_zero_dummy(const char *fmt, ...) { DUMMY_AP(fmt); }
void pagoda::print_zero_dummy(const string &str) { }
void pagoda::print_zero_dummy(FILE *stream, const char *fmt, ...) { DUMMY_AP(fmt); }
void pagoda::print_zero_dummy(FILE *stream, const string &str) { }


void pagoda::print_sync(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    print_sync(stderr, to_string(fmt, ap));
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
    print_sync(stream, to_string(fmt, ap));
    va_end(ap);
}


void pagoda::print_sync(FILE *stream, const string &str)
{
    if (0 == pagoda::nodeid()) {
        fprintf(stream, "[%*d] ", get_precision(), 0);
        fprintf(stream, str.c_str());
        for (int proc=1,nproc=pagoda::num_nodes(); proc<nproc; ++proc) {
            MPI_Status stat;
            int count;
            char *msg;
           
            MPI_Recv(&count, 1, MPI_INT, proc, TAG_DEBUG, MPI_COMM_WORLD,
                    &stat);
            msg = new char[count];
            MPI_Recv(msg, count, MPI_CHAR, proc, TAG_DEBUG, MPI_COMM_WORLD,
                    &stat);
            fprintf(stream, "[%*d] ", get_precision(), proc);
            fprintf(stream, msg);
            delete [] msg;
        }
        fflush(stream);
    } else {
        int count = str.size() + 1;
       
        MPI_Send(&count, 1, MPI_INT, 0, TAG_DEBUG, MPI_COMM_WORLD);
        MPI_Send((void*)str.c_str(), count, MPI_CHAR, 0, TAG_DEBUG,
                MPI_COMM_WORLD);
    }
}


void pagoda::print_sync_dummy(const char *fmt, ...) { DUMMY_AP(fmt); }
void pagoda::print_sync_dummy(const string &str) { }
void pagoda::print_sync_dummy(FILE *stream, const char *fmt, ...) { DUMMY_AP(fmt); }
void pagoda::print_sync_dummy(FILE *stream, const string &str) { }
