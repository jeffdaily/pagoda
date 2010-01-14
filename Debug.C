#include <mpi.h>
#include <ga.h>

#include <cstdarg>
#include <cstdio>
#include <string>

#include "Debug.H"
#include "Timing.H"


static inline int get_precision()
{
    TIMING("get_precision()");
    int precision = 1;
    int nproc = GA_Nnodes();
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


void print_zero_dummy(const char *fmt, ...) {}
void print_zero(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    print_zero(to_string(fmt, ap));
    va_end(ap);
}


void print_zero_dummy(const string &str) {}
void print_zero(const string &str)
{
    if (0 == GA_Nodeid()) {
        fprintf(stderr, str.c_str());
        fflush(stderr);
    }
    GA_Sync();
}


void print_sync_dummy(const char *fmt, ...) {}
void print_sync(const char *fmt, ...)
{
    va_list ap;

    va_start(ap, fmt);
    print_sync(to_string(fmt, ap));
    va_end(ap);
}


void print_sync_dummy(const string &str) {}
void print_sync(const string &str)
{
    if (0 == GA_Nodeid()) {
        fprintf(stderr, "[%*d] ", get_precision(), 0);
        fprintf(stderr, str.c_str());
        for (int proc=1,nproc=GA_Nnodes(); proc<nproc; ++proc) {
            MPI_Status stat;
            int count;
            char *msg;
           
            MPI_Recv(&count, 1, MPI_INT, proc, 1, MPI_COMM_WORLD, &stat);
            msg = new char[count];
            MPI_Recv(msg, count, MPI_CHAR, proc, 1, MPI_COMM_WORLD, &stat);
            fprintf(stderr, "[%*d] ", get_precision(), proc);
            fprintf(stderr, msg);
            delete [] msg;
        }
        fflush(stderr);
    } else {
        int count = str.size() + 1;
       
        MPI_Send(&count, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send((void*)str.c_str(), count, MPI_CHAR, 0, 1, MPI_COMM_WORLD);
    }
}
