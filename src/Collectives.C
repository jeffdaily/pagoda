#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <fstream>
#include <vector>

#if HAVE_MPI
#   include <mpi.h>
#endif

#include "Bootstrap.H"
#include "Collectives.H"
#include "ProcessGroup.H"

using std::ifstream;
using std::vector;


/**
 * Parallel barrier.
 */
void pagoda::barrier()
{
    ProcessGroup::get_default().barrier();
}


#if HAVE_MPI
#   define GOP_IMPL(N,C,M,O)                                        \
void pagoda::N(vector<C> &values)                                   \
{                                                                   \
    vector<C> values_copy(values);                                  \
    MPI_Allreduce(&values_copy[0], &values[0], values.size(),       \
                  M, O, ProcessGroup::get_default().get_comm());    \
}
#else
#   error
#endif
GOP_IMPL(gop_min,char,              MPI_CHAR,               MPI_MIN)
GOP_IMPL(gop_min,short,             MPI_SHORT,              MPI_MIN)
GOP_IMPL(gop_min,int,               MPI_INT,                MPI_MIN)
GOP_IMPL(gop_min,long,              MPI_LONG,               MPI_MIN)
GOP_IMPL(gop_min,long long,         MPI_LONG_LONG,          MPI_MIN)
GOP_IMPL(gop_min,float,             MPI_FLOAT,              MPI_MIN)
GOP_IMPL(gop_min,double,            MPI_DOUBLE,             MPI_MIN)
GOP_IMPL(gop_min,long double,       MPI_LONG_DOUBLE,        MPI_MIN)
GOP_IMPL(gop_min,unsigned char,     MPI_UNSIGNED_CHAR,      MPI_MIN)
GOP_IMPL(gop_min,unsigned short,    MPI_UNSIGNED_SHORT,     MPI_MIN)
GOP_IMPL(gop_min,unsigned int,      MPI_UNSIGNED,           MPI_MIN)
GOP_IMPL(gop_min,unsigned long,     MPI_UNSIGNED_LONG,      MPI_MIN)
//GOP_IMPL(gop_min,unsigned long long,MPI_UNSIGNED_LONG_LONG, MPI_MIN)
GOP_IMPL(gop_max,char,              MPI_CHAR,               MPI_MAX)
GOP_IMPL(gop_max,short,             MPI_SHORT,              MPI_MAX)
GOP_IMPL(gop_max,int,               MPI_INT,                MPI_MAX)
GOP_IMPL(gop_max,long,              MPI_LONG,               MPI_MAX)
GOP_IMPL(gop_max,long long,         MPI_LONG_LONG,          MPI_MAX)
GOP_IMPL(gop_max,float,             MPI_FLOAT,              MPI_MAX)
GOP_IMPL(gop_max,double,            MPI_DOUBLE,             MPI_MAX)
GOP_IMPL(gop_max,long double,       MPI_LONG_DOUBLE,        MPI_MAX)
GOP_IMPL(gop_max,unsigned char,     MPI_UNSIGNED_CHAR,      MPI_MAX)
GOP_IMPL(gop_max,unsigned short,    MPI_UNSIGNED_SHORT,     MPI_MAX)
GOP_IMPL(gop_max,unsigned int,      MPI_UNSIGNED,           MPI_MAX)
GOP_IMPL(gop_max,unsigned long,     MPI_UNSIGNED_LONG,      MPI_MAX)
//GOP_IMPL(gop_max,unsigned long long,MPI_UNSIGNED_LONG_LONG, MPI_MAX)
GOP_IMPL(gop_sum,char,              MPI_CHAR,               MPI_SUM)
GOP_IMPL(gop_sum,short,             MPI_SHORT,              MPI_SUM)
GOP_IMPL(gop_sum,int,               MPI_INT,                MPI_SUM)
GOP_IMPL(gop_sum,long,              MPI_LONG,               MPI_SUM)
GOP_IMPL(gop_sum,long long,         MPI_LONG_LONG,          MPI_SUM)
GOP_IMPL(gop_sum,float,             MPI_FLOAT,              MPI_SUM)
GOP_IMPL(gop_sum,double,            MPI_DOUBLE,             MPI_SUM)
GOP_IMPL(gop_sum,long double,       MPI_LONG_DOUBLE,        MPI_SUM)
GOP_IMPL(gop_sum,unsigned char,     MPI_UNSIGNED_CHAR,      MPI_SUM)
GOP_IMPL(gop_sum,unsigned short,    MPI_UNSIGNED_SHORT,     MPI_SUM)
GOP_IMPL(gop_sum,unsigned int,      MPI_UNSIGNED,           MPI_SUM)
GOP_IMPL(gop_sum,unsigned long,     MPI_UNSIGNED_LONG,      MPI_SUM)
//GOP_IMPL(gop_sum,unsigned long long,MPI_UNSIGNED_LONG_LONG, MPI_SUM)
#undef GOP_IMPL

#if NEED_VECTOR_INT64_T_GOP
/**
 * Returns the sum of all values from all processes.
 *
 * @param[in,out] values the values to take the sums of
 */
void pagoda::gop_sum(vector<int64_t> &values)
{
    vector<int64_t> values_copy(values);
#if HAVE_MPI
#   if SIZEOF_INT64_T == SIZEOF_LONG_LONG
    MPI_Allreduce(&values_copy[0], &values[0], values.size(), MPI_LONG_LONG, MPI_SUM, ProcessGroup::get_default().get_comm());
#   elif SIZEOF_INT64_T == SIZEOF_LONG
    MPI_Allreduce(&values_copy[0], &values[0], values.size(), MPI_LONG, MPI_SUM, ProcessGroup::get_default().get_comm());
#   elif SIZEOF_INT64_T == SIZEOF_INT
    MPI_Allreduce(&values_copy[0], &values[0], values.size(), MPI_INT, MPI_SUM, ProcessGroup::get_default().get_comm());
#   else
#       error
#   endif
#else
#   error
#endif
}
#endif /* NEED_VECTOR_INT64_T_GOP */

#if HAVE_MPI
#   define GOP_IMPL(N,C,M,O)                                        \
void pagoda::N(C &value)                                            \
{                                                                   \
    C value_copy(value);                                            \
    MPI_Allreduce(&value_copy, &value, 1,                           \
                  M, O, ProcessGroup::get_default().get_comm());    \
}
#else
#   error
#endif
GOP_IMPL(gop_min,char,              MPI_CHAR,               MPI_MIN)
GOP_IMPL(gop_min,short,             MPI_SHORT,              MPI_MIN)
GOP_IMPL(gop_min,int,               MPI_INT,                MPI_MIN)
GOP_IMPL(gop_min,long,              MPI_LONG,               MPI_MIN)
GOP_IMPL(gop_min,long long,         MPI_LONG_LONG,          MPI_MIN)
GOP_IMPL(gop_min,float,             MPI_FLOAT,              MPI_MIN)
GOP_IMPL(gop_min,double,            MPI_DOUBLE,             MPI_MIN)
GOP_IMPL(gop_min,long double,       MPI_LONG_DOUBLE,        MPI_MIN)
GOP_IMPL(gop_min,unsigned char,     MPI_UNSIGNED_CHAR,      MPI_MIN)
GOP_IMPL(gop_min,unsigned short,    MPI_UNSIGNED_SHORT,     MPI_MIN)
GOP_IMPL(gop_min,unsigned int,      MPI_UNSIGNED,           MPI_MIN)
GOP_IMPL(gop_min,unsigned long,     MPI_UNSIGNED_LONG,      MPI_MIN)
//GOP_IMPL(gop_min,unsigned long long,MPI_UNSIGNED_LONG_LONG, MPI_MIN)
GOP_IMPL(gop_max,char,              MPI_CHAR,               MPI_MAX)
GOP_IMPL(gop_max,short,             MPI_SHORT,              MPI_MAX)
GOP_IMPL(gop_max,int,               MPI_INT,                MPI_MAX)
GOP_IMPL(gop_max,long,              MPI_LONG,               MPI_MAX)
GOP_IMPL(gop_max,long long,         MPI_LONG_LONG,          MPI_MAX)
GOP_IMPL(gop_max,float,             MPI_FLOAT,              MPI_MAX)
GOP_IMPL(gop_max,double,            MPI_DOUBLE,             MPI_MAX)
GOP_IMPL(gop_max,long double,       MPI_LONG_DOUBLE,        MPI_MAX)
GOP_IMPL(gop_max,unsigned char,     MPI_UNSIGNED_CHAR,      MPI_MAX)
GOP_IMPL(gop_max,unsigned short,    MPI_UNSIGNED_SHORT,     MPI_MAX)
GOP_IMPL(gop_max,unsigned int,      MPI_UNSIGNED,           MPI_MAX)
GOP_IMPL(gop_max,unsigned long,     MPI_UNSIGNED_LONG,      MPI_MAX)
//GOP_IMPL(gop_max,unsigned long long,MPI_UNSIGNED_LONG_LONG, MPI_MAX)
GOP_IMPL(gop_sum,char,              MPI_CHAR,               MPI_SUM)
GOP_IMPL(gop_sum,short,             MPI_SHORT,              MPI_SUM)
GOP_IMPL(gop_sum,int,               MPI_INT,                MPI_SUM)
GOP_IMPL(gop_sum,long,              MPI_LONG,               MPI_SUM)
GOP_IMPL(gop_sum,long long,         MPI_LONG_LONG,          MPI_SUM)
GOP_IMPL(gop_sum,float,             MPI_FLOAT,              MPI_SUM)
GOP_IMPL(gop_sum,double,            MPI_DOUBLE,             MPI_SUM)
GOP_IMPL(gop_sum,long double,       MPI_LONG_DOUBLE,        MPI_SUM)
GOP_IMPL(gop_sum,unsigned char,     MPI_UNSIGNED_CHAR,      MPI_SUM)
GOP_IMPL(gop_sum,unsigned short,    MPI_UNSIGNED_SHORT,     MPI_SUM)
GOP_IMPL(gop_sum,unsigned int,      MPI_UNSIGNED,           MPI_SUM)
GOP_IMPL(gop_sum,unsigned long,     MPI_UNSIGNED_LONG,      MPI_SUM)
//GOP_IMPL(gop_sum,unsigned long long,MPI_UNSIGNED_LONG_LONG, MPI_SUM)
#undef GOP_IMPL

#if NEED_VECTOR_INT64_T_GOP
/**
 * Returns the sum of all values from all processes.
 *
 * @param[in,out] values the values to take the sums of
 */
void pagoda::gop_sum(int64_t &value)
{
    int64_t value_copy(value);
#if HAVE_MPI
#   if SIZEOF_INT64_T == SIZEOF_LONG_LONG
    MPI_Allreduce(&value_copy, &value, 1, MPI_LONG_LONG, MPI_SUM, ProcessGroup::get_default().get_comm());
#   elif SIZEOF_INT64_T == SIZEOF_LONG
    MPI_Allreduce(&value_copy, &value, 1, MPI_LONG, MPI_SUM, ProcessGroup::get_default().get_comm());
#   elif SIZEOF_INT64_T == SIZEOF_INT
    MPI_Allreduce(&value_copy, &value, 1, MPI_INT, MPI_SUM, ProcessGroup::get_default().get_comm());
#   else
#       error
#   endif
#else
#   error
#endif
}
#endif /* NEED_VECTOR_INT64_T_GOP */


#if HAVE_MPI
#   define broadcast_impl(T)                                         \
void pagoda::broadcast(vector<T> &values, int root)                  \
{                                                                    \
    MPI_Bcast(&values[0], values.size()*sizeof(T),                   \
            MPI_CHAR, root, ProcessGroup::get_default().get_comm()); \
}
#else
#   error
#endif
broadcast_impl(int)
broadcast_impl(long)
broadcast_impl(long long)
broadcast_impl(float)
broadcast_impl(double)
broadcast_impl(long double)
#undef broadcast_impl


#if HAVE_MPI
#   define broadcast_impl(T)                                         \
void pagoda::broadcast(T &value, int root)                           \
{                                                                    \
    MPI_Bcast(&value, sizeof(T),                                     \
            MPI_CHAR, root, ProcessGroup::get_default().get_comm()); \
}
#else
#   error
#endif
broadcast_impl(int)
broadcast_impl(long)
broadcast_impl(long long)
broadcast_impl(float)
broadcast_impl(double)
broadcast_impl(long double)
#undef broadcast_impl


bool pagoda::file_exists(const string &filename)
{
    vector<long> values(1, 0); // vector size 1 initialized to 0

    // we don't want all processors hammering the file system simply to test
    // if the file exists, so we let process 0 do it
    if (pagoda::nodeid() == 0) {
        // this statement should cast the file object to a boolean which is
        // true if the file exists; the file is automatically closed at the
        // end of its scope
        if ((ifstream(filename.c_str()))) {
            values[0] = 1;
        }
    }
    pagoda::broadcast(values, 0);

    return values.front() == 1;
}

