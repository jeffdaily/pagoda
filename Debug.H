#ifndef DEBUG_H_
#define DEBUG_H_

#include <mpi.h>

#include <cstdio>
#include <string>

#include <ga.h>

using std::fflush;
using std::fprintf;
using std::string;

#define ME GA_Nodeid()
#define NPROC GA_Nnodes()

extern int __gcrm_subsetter_get_precision();

#define PRINT_ZERO(FMT) do { \
    if (ME == 0) { \
        fprintf(stderr,FMT); \
        fflush(stderr); \
    } \
    GA_Sync(); \
} while (0)
#define PRINT_ZERO1(FMT,A1) do { \
    if (ME == 0) { \
        fprintf(stderr,FMT,A1); \
        fflush(stderr); \
    } \
    GA_Sync(); \
} while (0)
#define PRINT_ZERO2(FMT,A1,A2) do { \
    if (ME == 0) { \
        fprintf(stderr,FMT,A1,A2); \
        fflush(stderr); \
    } \
    GA_Sync(); \
} while (0)
#define PRINT_ZERO3(FMT,A1,A2,A3) do { \
    if (ME == 0) { \
        fprintf(stderr,FMT,A1,A2,A3); \
        fflush(stderr); \
    } \
    GA_Sync(); \
} while (0)
#define PRINT_ZERO4(FMT,A1,A2,A3,A4) do { \
    if (ME == 0) { \
        fprintf(stderr,FMT,A1,A2,A3,A4); \
        fflush(stderr); \
    } \
    GA_Sync(); \
} while (0)


#define PRINT_STRING_ZERO(STRING) do { \
    string _s = STRING; \
    if (ME == 0) { \
        fprintf(stderr,_s.c_str()); \
        fflush(stderr); \
    } \
    GA_Sync(); \
} while (0)


#define PRINT_STRING_SYNC(STRING) do { \
    if (0 == ME) { \
        fprintf(stderr, "[%*d] ", __gcrm_subsetter_get_precision(), 0); \
        fprintf(stderr, STRING.c_str()); \
        for (int proc=1,nproc=NPROC; proc<nproc; ++proc) { \
            MPI_Status _stat; \
            int _count; \
            char *_msg; \
            \
            MPI_Recv(&_count, 1, MPI_INT, proc, 1, MPI_COMM_WORLD, &_stat); \
            _msg = new char[_count]; \
            MPI_Recv(_msg, _count, MPI_CHAR, proc, 1, MPI_COMM_WORLD, &_stat); \
            fprintf(stderr, "[%*d] ", __gcrm_subsetter_get_precision(), proc); \
            fprintf(stderr, _msg); \
            delete [] _msg; \
        } \
        fflush(stderr); \
    } else { \
        int _count = STRING.size() + 1; \
        \
        MPI_Send(&_count, 1, MPI_INT, 0, 1, MPI_COMM_WORLD); \
        MPI_Send(STRING.c_str(), _count, MPI_CHAR, 0, 1, MPI_COMM_WORLD); \
    } \
} while (0)


#define PRINT_SYNC(fmt) do { \
    MPI_Status stat; \
    int _count=100; \
    char msg[_count]; \
    if (0 == ME) { \
        fprintf(stderr, "[%*d] ", __gcrm_subsetter_get_precision(), 0); \
        fprintf(stderr, fmt); \
        for (int proc=1; proc<NPROC; ++proc) { \
            MPI_Recv(msg, _count, MPI_CHAR, proc, 1, MPI_COMM_WORLD, &stat); \
            fprintf(stderr, "[%*d] ", __gcrm_subsetter_get_precision(), proc); \
            fprintf(stderr, msg); \
        } \
        fflush(stderr); \
        fflush(stdout); \
    } else { \
        snprintf(msg, _count, fmt); \
        MPI_Send(msg, _count, MPI_CHAR, 0, 1, MPI_COMM_WORLD); \
    } \
} while (0)
#define PRINT_SYNC1(fmt,a1) do { \
    MPI_Status stat; \
    int _count=100; \
    char msg[_count]; \
    if (0 == ME) { \
        fprintf(stderr, "[%*d] ", __gcrm_subsetter_get_precision(), 0); \
        fprintf(stderr, fmt, a1); \
        for (int proc=1; proc<NPROC; ++proc) { \
            MPI_Recv(msg, _count, MPI_CHAR, proc, 1, MPI_COMM_WORLD, &stat); \
            fprintf(stderr, "[%*d] ", __gcrm_subsetter_get_precision(), proc); \
            fprintf(stderr, msg); \
        } \
        fflush(stderr); \
        fflush(stdout); \
    } else { \
        snprintf(msg, _count, fmt, a1); \
        MPI_Send(msg, _count, MPI_CHAR, 0, 1, MPI_COMM_WORLD); \
    } \
} while (0)
#define PRINT_SYNC2(fmt,a1,a2) do { \
    MPI_Status stat; \
    int _count=100; \
    char msg[_count]; \
    if (0 == ME) { \
        fprintf(stderr, "[%*d] ", __gcrm_subsetter_get_precision(), 0); \
        fprintf(stderr, fmt,a1,a2); \
        for (int proc=1; proc<NPROC; ++proc) { \
            MPI_Recv(msg, _count, MPI_CHAR, proc, 1, MPI_COMM_WORLD, &stat); \
            fprintf(stderr, "[%*d] ", __gcrm_subsetter_get_precision(), proc); \
            fprintf(stderr, msg); \
        } \
        fflush(stderr); \
        fflush(stdout); \
    } else { \
        snprintf(msg, _count, fmt, a1, a2); \
        MPI_Send(msg, _count, MPI_CHAR, 0, 1, MPI_COMM_WORLD); \
    } \
} while (0)
#define PRINT_SYNC3(fmt,a1,a2,a3) do { \
    MPI_Status stat; \
    int _count=100; \
    char msg[_count]; \
    if (0 == ME) { \
        fprintf(stderr, "[%*d] ", __gcrm_subsetter_get_precision(), 0); \
        fprintf(stderr, fmt,a1,a2,a3); \
        for (int proc=1; proc<NPROC; ++proc) { \
            MPI_Recv(msg, _count, MPI_CHAR, proc, 1, MPI_COMM_WORLD, &stat); \
            fprintf(stderr, "[%*d] ", __gcrm_subsetter_get_precision(), proc); \
            fprintf(stderr, msg); \
        } \
        fflush(stderr); \
        fflush(stdout); \
    } else { \
        snprintf(msg, _count, fmt, a1, a2, a3); \
        MPI_Send(msg, _count, MPI_CHAR, 0, 1, MPI_COMM_WORLD); \
    } \
} while (0)
#define PRINT_SYNC4(fmt,a1,a2,a3,a4) do { \
    MPI_Status stat; \
    int _count=100; \
    char msg[_count]; \
    if (0 == ME) { \
        fprintf(stderr, "[%*d] ", __gcrm_subsetter_get_precision(), 0); \
        fprintf(stderr, fmt,a1,a2,a3,a4); \
        for (int proc=1; proc<NPROC; ++proc) { \
            MPI_Recv(msg, _count, MPI_CHAR, proc, 1, MPI_COMM_WORLD, &stat); \
            fprintf(stderr, "[%*d] ", __gcrm_subsetter_get_precision(), proc); \
            fprintf(stderr, msg); \
        } \
        fflush(stderr); \
        fflush(stdout); \
    } else { \
        snprintf(msg, _count, fmt, a1, a2, a3, a4); \
        MPI_Send(msg, _count, MPI_CHAR, 0, 1, MPI_COMM_WORLD); \
    } \
} while (0)


#if defined(DEBUG) || defined(TRACE)
#   define DEBUG_ZERO(FMT)              PRINT_ZERO(FMT)
#   define DEBUG_ZERO1(FMT,A1)          PRINT_ZERO1(FMT,A1)
#   define DEBUG_ZERO2(FMT,A1,A2)       PRINT_ZERO2(FMT,A1,A2)
#   define DEBUG_ZERO3(FMT,A1,A2,A3)    PRINT_ZERO3(FMT,A1,A2,A3)
#   define DEBUG_ZERO4(FMT,A1,A2,A3,A4) PRINT_ZERO4(FMT,A1,A2,A3,A4)
#   define DEBUG_SYNC(FMT)              PRINT_SYNC(FMT)
#   define DEBUG_SYNC1(FMT,A1)          PRINT_SYNC1(FMT,A1)
#   define DEBUG_SYNC2(FMT,A1,A2)       PRINT_SYNC2(FMT,A1,A2)
#   define DEBUG_SYNC3(FMT,A1,A2,A3)    PRINT_SYNC3(FMT,A1,A2,A3)
#   define DEBUG_SYNC4(FMT,A1,A2,A3,A4) PRINT_SYNC4(FMT,A1,A2,A3,A4)
#   define DEBUG_STRING(STRING)         PRINT_STRING_SYNC(STRING)
#else
#   define DEBUG_ZERO(FMT)              do {} while (0)
#   define DEBUG_ZERO1(FMT,A1)          do {} while (0)
#   define DEBUG_ZERO2(FMT,A1,A2)       do {} while (0)
#   define DEBUG_ZERO3(FMT,A1,A2,A3)    do {} while (0)
#   define DEBUG_ZERO4(FMT,A1,A2,A3,A4) do {} while (0)
#   define DEBUG_SYNC(FMT)              do {} while (0)
#   define DEBUG_SYNC1(FMT,A1)          do {} while (0)
#   define DEBUG_SYNC2(FMT,A1,A2)       do {} while (0)
#   define DEBUG_SYNC3(FMT,A1,A2,A3)    do {} while (0)
#   define DEBUG_SYNC4(FMT,A1,A2,A3,A4) do {} while (0)
#   define DEBUG_STRING(STRING)         do {} while (0)
#endif

#ifdef TRACE
#   define TRACER(FMT)              PRINT_SYNC(FMT)
#   define TRACER1(FMT,A1)          PRINT_SYNC1(FMT,A1)
#   define TRACER2(FMT,A1,A2)       PRINT_SYNC2(FMT,A1,A2)
#   define TRACER3(FMT,A1,A2,A3)    PRINT_SYNC3(FMT,A1,A2,A3)
#   define TRACER4(FMT,A1,A2,A3,A4) PRINT_SYNC4(FMT,A1,A2,A3,A4)
#   define TRACE_STRING(STRING)     PRINT_STRING_SYNC(STRING)
#else
#   define TRACER(FMT)              do {} while (0)
#   define TRACER1(FMT,A1)          do {} while (0)
#   define TRACER2(FMT,A1,A2)       do {} while (0)
#   define TRACER3(FMT,A1,A2,A3)    do {} while (0)
#   define TRACER4(FMT,A1,A2,A3,A4) do {} while (0)
#   define TRACE_STRING(STRING)     do {} while (0)
#endif

#endif // DEBUG_H_
