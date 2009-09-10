#ifndef DEBUG_H_
#define DEBUG_H_

#include <mpi.h>

#include <cstdio>

#include <ga.h>

#define ME GA_Nodeid()
#define NPROC GA_Nnodes()
#define PRECISION (((int)(NPROC/10))+1)

#if defined(DEBUG) || defined(TRACE)
#   define DEBUG_PRINT(FMT) \
    do { \
        fprintf(stderr, "[%*d] ", PRECISION, ME); \
        fprintf(stderr,FMT); \
    } while (0)
#   define DEBUG_PRINT1(FMT,A1) \
    do { \
        fprintf(stderr, "[%*d] ", PRECISION, ME); \
        fprintf(stderr,FMT,A1); \
    } while (0)
#   define DEBUG_PRINT2(FMT,A1,A2) \
    do { \
        fprintf(stderr, "[%*d] ", PRECISION, ME); \
        fprintf(stderr,FMT,A1,A2); \
    } while (0)
#   define DEBUG_PRINT3(FMT,A1,A2,A3) \
    do { \
        fprintf(stderr, "[%*d] ", PRECISION, ME); \
        fprintf(stderr,FMT,A1,A2,A3); \
    } while (0)
#   define DEBUG_PRINT_ME(FMT) \
    do { \
        if (ME == 0) { \
            fprintf(stderr, FMT); \
        } \
    } while (0)
#   define DEBUG_PRINT_ME1(FMT,A1) \
    do { \
        if (ME == 0) { \
            fprintf(stderr, FMT,A1); \
        } \
    } while (0)
#   define DEBUG_PRINT_ME2(FMT,A1,A2) \
    do { \
        if (ME == 0) { \
            fprintf(stderr, FMT,A1,A2); \
        } \
    } while (0)
#   define DEBUG_PRINT_ME3(FMT,A1,A2,A3) \
    do { \
        if (ME == 0) { \
            fprintf(stderr, FMT,A1,A2,A3); \
        } \
    } while (0)
#else
#   define DEBUG_PRINT(FMT) do {} while (0)
#   define DEBUG_PRINT1(FMT,A1) do {} while (0)
#   define DEBUG_PRINT2(FMT,A1,A2) do {} while (0)
#   define DEBUG_PRINT3(FMT,A1,A2,A3) do {} while (0)
#   define DEBUG_PRINT_ME(FMT) do {} while (0)
#   define DEBUG_PRINT_ME1(FMT,A1) do {} while (0)
#   define DEBUG_PRINT_ME2(FMT,A1,A2) do {} while (0)
#   define DEBUG_PRINT_ME3(FMT,A1,A2,A3) do {} while (0)
#endif

#ifdef TRACE
#   define TRACER(fmt) \
    do { \
        MPI_Status stat; \
        int _count=100; \
        char msg[_count]; \
        if (0 == ME) { \
            fprintf(stderr, "[%*d] ", PRECISION, 0); \
            fprintf(stderr, fmt); \
            for (int proc=1; proc<NPROC; ++proc) { \
                MPI_Recv(msg, _count, MPI_BYTE, proc, 1, MPI_COMM_WORLD, &stat); \
                fprintf(stderr, "[%*d] ", PRECISION, proc); \
                fprintf(stderr, msg); \
            } \
            fflush(stderr); \
            fflush(stdout); \
        } else { \
            sprintf(msg, fmt); \
            MPI_Send(msg, _count, MPI_BYTE, 0, 1, MPI_COMM_WORLD); \
        } \
    } while (0)
#   define TRACER1(fmt,a1) \
    do { \
        MPI_Status stat; \
        int _count=100; \
        char msg[_count]; \
        if (0 == ME) { \
            fprintf(stderr, "[%*d] ", PRECISION, 0); \
            fprintf(stderr, fmt, a1); \
            for (int proc=1; proc<NPROC; ++proc) { \
                MPI_Recv(msg, _count, MPI_BYTE, proc, 1, MPI_COMM_WORLD, &stat); \
                fprintf(stderr, "[%*d] ", PRECISION, proc); \
                fprintf(stderr, msg); \
            } \
            fflush(stderr); \
            fflush(stdout); \
        } else { \
            sprintf(msg, fmt, a1); \
            MPI_Send(msg, _count, MPI_BYTE, 0, 1, MPI_COMM_WORLD); \
        } \
    } while (0)
#   define TRACER2(fmt,a1,a2) \
    do { \
        MPI_Status stat; \
        int _count=100; \
        char msg[_count]; \
        if (0 == ME) { \
            fprintf(stderr, "[%*d] ", PRECISION, 0); \
            fprintf(stderr, fmt,a1,a2); \
            for (int proc=1; proc<NPROC; ++proc) { \
                MPI_Recv(msg, _count, MPI_BYTE, proc, 1, MPI_COMM_WORLD, &stat); \
                fprintf(stderr, "[%*d] ", PRECISION, proc); \
                fprintf(stderr, msg); \
            } \
            fflush(stderr); \
            fflush(stdout); \
        } else { \
            sprintf(msg, fmt,a1,a2); \
            MPI_Send(msg, _count, MPI_BYTE, 0, 1, MPI_COMM_WORLD); \
        } \
    } while (0)
#   define TRACER3(fmt,a1,a2,a3) \
    do { \
        MPI_Status stat; \
        int _count=100; \
        char msg[_count]; \
        if (0 == ME) { \
            fprintf(stderr, "[%*d] ", PRECISION, 0); \
            fprintf(stderr, fmt,a1,a2,a3); \
            for (int proc=1; proc<NPROC; ++proc) { \
                MPI_Recv(msg, _count, MPI_BYTE, proc, 1, MPI_COMM_WORLD, &stat); \
                fprintf(stderr, "[%*d] ", PRECISION, proc); \
                fprintf(stderr, msg); \
            } \
            fflush(stderr); \
            fflush(stdout); \
        } else { \
            sprintf(msg, fmt,a1,a2,a3); \
            MPI_Send(msg, _count, MPI_BYTE, 0, 1, MPI_COMM_WORLD); \
        } \
    } while (0)
#   define TRACER4(fmt,a1,a2,a3,a4) \
    do { \
        MPI_Status stat; \
        int _count=100; \
        char msg[_count]; \
        if (0 == ME) { \
            fprintf(stderr, "[%*d] ", PRECISION, 0); \
            fprintf(stderr, fmt,a1,a2,a3,a4); \
            for (int proc=1; proc<NPROC; ++proc) { \
                MPI_Recv(msg, _count, MPI_BYTE, proc, 1, MPI_COMM_WORLD, &stat); \
                fprintf(stderr, "[%*d] ", PRECISION, proc); \
                fprintf(stderr, msg); \
            } \
            fflush(stderr); \
            fflush(stdout); \
        } else { \
            sprintf(msg, fmt,a1,a2,a3,a4); \
            MPI_Send(msg, _count, MPI_BYTE, 0, 1, MPI_COMM_WORLD); \
        } \
    } while (0)
#else
#   define TRACER(fmt) do {} while (0)
#   define TRACER1(fmt,a1) do {} while (0)
#   define TRACER2(fmt,a1,a2) do {} while (0)
#   define TRACER3(fmt,a1,a2,a3) do {} while (0)
#   define TRACER4(fmt,a1,a2,a3,a4) do {} while (0)
#endif

#endif // DEBUG_H_
