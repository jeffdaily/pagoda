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


void print_zero_dummy(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    va_end(ap);
}


void print_zero(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    print_zero(to_string(fmt, ap));
    va_end(ap);
}


void print_zero_dummy(const string &str)
{
}


void print_zero(const string &str)
{
    if (0 == pagoda::nodeid()) {
        fprintf(stderr, str.c_str());
        fflush(stderr);
    }
    pagoda::barrier();
}


void print_sync_dummy(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    va_end(ap);
}



void print_sync(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    print_sync(to_string(fmt, ap));
    va_end(ap);
}


void print_sync_dummy(const string &str)
{
}


void print_sync(const string &str)
{
    if (0 == pagoda::nodeid()) {
        fprintf(stderr, "[%*d] ", get_precision(), 0);
        fprintf(stderr, str.c_str());
        for (int proc=1,nproc=pagoda::num_nodes(); proc<nproc; ++proc) {
            MPI_Status stat;
            int count;
            char *msg;
           
            MPI_Recv(&count, 1, MPI_INT, proc, TAG_DEBUG, MPI_COMM_WORLD,
                    &stat);
            msg = new char[count];
            MPI_Recv(msg, count, MPI_CHAR, proc, TAG_DEBUG, MPI_COMM_WORLD,
                    &stat);
            fprintf(stderr, "[%*d] ", get_precision(), proc);
            fprintf(stderr, msg);
            delete [] msg;
        }
        fflush(stderr);
    } else {
        int count = str.size() + 1;
       
        MPI_Send(&count, 1, MPI_INT, 0, TAG_DEBUG, MPI_COMM_WORLD);
        MPI_Send((void*)str.c_str(), count, MPI_CHAR, 0, TAG_DEBUG,
                MPI_COMM_WORLD);
    }
}
