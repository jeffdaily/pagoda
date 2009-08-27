#ifndef UTIL_H_
#define UTIL_H_

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif // HAVE_CONFIG_H

#include <algorithm>
    using std::reverse;
    using std::reverse_copy;
    using std::rotate;
    using std::transform;
#include <cmath>
#include <functional>
    using std::bind2nd;
    using std::divides;
    using std::modulus;
    using std::multiplies;
    using std::ptr_fun;
#include <numeric>
    using std::partial_sum;
#include <sstream>
    using std::endl;
    using std::ostringstream;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include <ga.h>
#include <mpi.h>

#include "StringComparator.H"
#include "SubsetterException.H"

class Attribute;
class Dimension;
class Variable;

#define ME GA_Nodeid()
#define NPROC GA_Nnodes()
#define MAX_NAME 80
#define MAX_DIM 8
#define RAD_PER_DEG (M_PI / 180.0)
#define DEG_PER_RAD (180.0 / M_PI)
#define PRECISION (((int)(NPROC/10))+1)

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define AT __FILE__ ":" TOSTRING(__LINE__)

#define ERR(e) \
{ \
    ostringstream __os; \
    __os << "Error: " << e << endl; \
    throw SubsetterException(AT, __os.str()); \
}

#define ERRNO(n) \
{ \
    ostringstream __os; \
    __os << "Error: " << ncmpi_strerror(n) << endl; \
    throw SubsetterException(AT, __os.str()); \
}

#define ERRNO_CHECK(n) \
{ \
    if (n != NC_NOERR) { \
        ERRNO(n); \
    } \
}

#if defined(DEBUG) || defined(TRACE)
#   define DEBUG_PRINT(FMT) \
    { \
        fprintf(stderr, "[%*d] ", PRECISION, ME); \
        fprintf(stderr,FMT); \
    }
#   define DEBUG_PRINT1(FMT,A1) \
    { \
        fprintf(stderr, "[%*d] ", PRECISION, ME); \
        fprintf(stderr,FMT,A1); \
    }
#   define DEBUG_PRINT2(FMT,A1,A2) \
    { \
        fprintf(stderr, "[%*d] ", PRECISION, ME); \
        fprintf(stderr,FMT,A1,A2); \
    }
#   define DEBUG_PRINT3(FMT,A1,A2,A3) \
    { \
        fprintf(stderr, "[%*d] ", PRECISION, ME); \
        fprintf(stderr,FMT,A1,A2,A3); \
    }
#   define DEBUG_PRINT_ME(FMT) if (ME == 0) fprintf(stderr, FMT)
#   define DEBUG_PRINT_ME1(FMT,A1) if (ME == 0) fprintf(stderr, FMT,A1)
#   define DEBUG_PRINT_ME2(FMT,A1,A2) if (ME == 0) fprintf(stderr, FMT,A1,A2)
#   define DEBUG_PRINT_ME3(FMT,A1,A2,A3) if (ME == 0) fprintf(stderr, FMT,A1,A2,A3)
#else
#   define DEBUG_PRINT(FMT)
#   define DEBUG_PRINT1(FMT,A1)
#   define DEBUG_PRINT2(FMT,A1,A2)
#   define DEBUG_PRINT3(FMT,A1,A2,A3)
#   define DEBUG_PRINT_ME(FMT)
#   define DEBUG_PRINT_ME1(FMT,A1)
#   define DEBUG_PRINT_ME2(FMT,A1,A2)
#   define DEBUG_PRINT_ME3(FMT,A1,A2,A3)
#endif

#ifdef TRACE
#   define TRACER(fmt) \
    do { \
        MPI_Status stat; \
        int count=100; \
        char msg[count]; \
        if (0 == ME) { \
            fprintf(stderr, "[%*d] ", PRECISION, 0); \
            fprintf(stderr, fmt); \
            for (int proc=1; proc<NPROC; ++proc) { \
                MPI_Recv(msg, count, MPI_BYTE, proc, 1, MPI_COMM_WORLD, &stat); \
                fprintf(stderr, "[%*d] ", PRECISION, proc); \
                fprintf(stderr, msg); \
            } \
            fflush(stderr); \
            fflush(stdout); \
        } else { \
            sprintf(msg, fmt); \
            MPI_Send(msg, count, MPI_BYTE, 0, 1, MPI_COMM_WORLD); \
        } \
    } while (0)
#   define TRACER1(fmt,a1) \
    do { \
        MPI_Status stat; \
        int count=100; \
        char msg[count]; \
        if (0 == ME) { \
            fprintf(stderr, "[%*d] ", PRECISION, 0); \
            fprintf(stderr, fmt, a1); \
            for (int proc=1; proc<NPROC; ++proc) { \
                MPI_Recv(msg, count, MPI_BYTE, proc, 1, MPI_COMM_WORLD, &stat); \
                fprintf(stderr, "[%*d] ", PRECISION, proc); \
                fprintf(stderr, msg); \
            } \
            fflush(stderr); \
            fflush(stdout); \
        } else { \
            sprintf(msg, fmt, a1); \
            MPI_Send(msg, count, MPI_BYTE, 0, 1, MPI_COMM_WORLD); \
        } \
    } while (0)
#   define TRACER2(fmt,a1,a2) \
    do { \
        MPI_Status stat; \
        int count=100; \
        char msg[count]; \
        if (0 == ME) { \
            fprintf(stderr, "[%*d] ", PRECISION, 0); \
            fprintf(stderr, fmt,a1,a2); \
            for (int proc=1; proc<NPROC; ++proc) { \
                MPI_Recv(msg, count, MPI_BYTE, proc, 1, MPI_COMM_WORLD, &stat); \
                fprintf(stderr, "[%*d] ", PRECISION, proc); \
                fprintf(stderr, msg); \
            } \
            fflush(stderr); \
            fflush(stdout); \
        } else { \
            sprintf(msg, fmt,a1,a2); \
            MPI_Send(msg, count, MPI_BYTE, 0, 1, MPI_COMM_WORLD); \
        } \
    } while (0)
#   define TRACER3(fmt,a1,a2,a3) \
    do { \
        MPI_Status stat; \
        int count=100; \
        char msg[count]; \
        if (0 == ME) { \
            fprintf(stderr, "[%*d] ", PRECISION, 0); \
            fprintf(stderr, fmt,a1,a2,a3); \
            for (int proc=1; proc<NPROC; ++proc) { \
                MPI_Recv(msg, count, MPI_BYTE, proc, 1, MPI_COMM_WORLD, &stat); \
                fprintf(stderr, "[%*d] ", PRECISION, proc); \
                fprintf(stderr, msg); \
            } \
            fflush(stderr); \
            fflush(stdout); \
        } else { \
            sprintf(msg, fmt,a1,a2,a3); \
            MPI_Send(msg, count, MPI_BYTE, 0, 1, MPI_COMM_WORLD); \
        } \
    } while (0)
#   define TRACER4(fmt,a1,a2,a3,a4) \
    do { \
        MPI_Status stat; \
        int count=100; \
        char msg[count]; \
        if (0 == ME) { \
            fprintf(stderr, "[%*d] ", PRECISION, 0); \
            fprintf(stderr, fmt,a1,a2,a3,a4); \
            for (int proc=1; proc<NPROC; ++proc) { \
                MPI_Recv(msg, count, MPI_BYTE, proc, 1, MPI_COMM_WORLD, &stat); \
                fprintf(stderr, "[%*d] ", PRECISION, proc); \
                fprintf(stderr, msg); \
            } \
            fflush(stderr); \
            fflush(stdout); \
        } else { \
            sprintf(msg, fmt,a1,a2,a3,a4); \
            MPI_Send(msg, count, MPI_BYTE, 0, 1, MPI_COMM_WORLD); \
        } \
    } while (0)
#else
#   define TRACER(fmt) do {} while (0)
#   define TRACER1(fmt,a1) do {} while (0)
#   define TRACER2(fmt,a1,a2) do {} while (0)
#   define TRACER3(fmt,a1,a2,a3) do {} while (0)
#   define TRACER4(fmt,a1,a2,a3,a4) do {} while (0)
#endif


namespace Util
{


bool ends_with(const string &str, const string &end);
void calculate_required_memory(const vector<Variable*> &vars);


template <class T>
bool cmp_name(T ptr_to_thing, StringComparator comparator)
{
    return comparator(ptr_to_thing->get_name());
}


template <class T>
class fob_cmp_name {
  public:
    fob_cmp_name(StringComparator cmp)
      : cmp(cmp)
    { }
    bool operator () (T ptr_to_thing) {
      return cmp(ptr_to_thing->get_name());
    }
  protected:
    StringComparator cmp;
};


/**
 * Generic "find" based on a "get_name()" function and a given string.
 */
template <class T>
typename vector<T>::iterator
find(vector<T> &vec, const string &name, bool ignore_case=true)
{
    StringComparator cmp(name, ignore_case);
    fob_cmp_name<T> cmp_name(cmp);
    //return find_if(vec.begin(), vec.end(), bind2nd(ptr_fun(&cmp_name<T>), cmp));
    return find_if(vec.begin(), vec.end(), cmp_name);
}


/**
 * Generic "find" based on a "get_name()" function and a given string.
 * Takes multiple names to match.
 */
template <class T>
typename vector<T>::iterator
find(vector<T> &vec, const vector<string> &names, bool ignore_case=true)
{
    typename vector<T>::iterator result;
    vector<string>::const_iterator it;
    for (it=names.begin(); it!=names.end(); ++it) {
        result = find(vec, *it, ignore_case);
        if (result != vec.end())
            return result;
    }
    return vec.end();
}


/**
 * Generic unary function for calling "delete" on the given object.
 *
 * The class T is assumed to be a pointer.
 */
template <class T>
T ptr_deleter(T object)
{
    delete object;
    return NULL;
}


/**
 * Generic binary test for equality of the "get_name()" accessor.
 */
template <class T>
bool same_name(const T *first, const T *second)
{
    return first->get_name() == second->get_name();
}


/**
 * Given a 1-D index, determine its equivalent n-D index based on dims.
 *
 * Adapted from Python's NumPy.
 */
template <class T>
vector<T> unravel_index(T x, const vector<T> &dims)
{
    vector<T> result(dims.rbegin(), dims.rend());
    // [d,c,b,a]
    rotate(result.begin(), result.end()-1, result.end());
    // [a,d,c,b]
    *(result.begin()) = 1;
    // [1,d,c,b]
    partial_sum(result.begin(), result.end(), result.begin(), multiplies<T>());
    // [1,d,dc,dcb]
    reverse(result.begin(), result.end());
    // [dcb,dc,d,1]
    transform(result.begin(), result.end(), result.begin(),
            bind1st(divides<T>(),x));
    // [x/dcb, x/dc, x/d, x/1]
    transform(result.begin(), result.end(), dims.begin(), result.begin(),
            modulus<T>());
    // [x/dcb % a, x/dc % b, x/d % c, x/1 % d]
    return result;
}


/**
 * Given an n-D index, determine its equivalent 1-D index based on dims.
 *
 * Adapted from Python's NumPy.
 */
template <class T>
T ravel_index(vector<T> indices, const vector<T> &dims)
{
    // indices [w,x,y,z], dims [a,b,c,d]
    vector<T> offsets(dims.begin()+1, dims.end());
    // [b,c,d]
    reverse(offsets.begin(), offsets.end());
    // [d,c,b]
    partial_sum(offsets.begin(), offsets.end(), offsets.begin(),
            multiplies<T>());
    // [d,cd,bcd]
    reverse(offsets.begin(), offsets.end());
    // [bcd,cd,d]
    transform(indices.begin(), indices.end()-1, offsets.begin(),
            indices.begin(), multiplies<T>());
    // [wd,xcd,ybcd,z]
    return accumulate(indices.begin(), indices.end()-1, 0) + indices.back();
}

} // namespace Util

#endif // UTIL_H_
