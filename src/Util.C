#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <numeric>
#include <string>
#include <vector>

#if HAVE_MPI
#   include <mpi.h>
#endif
#if HAVE_GA
#   include <ga.h>
#   include <macdecls.h>
#endif

#include "DataType.H"
#include "Print.H"
#include "ProcessGroup.H"
#include "Util.H"
#include "Variable.H"

using std::accumulate;
using std::back_inserter;
using std::copy;
using std::ifstream;
using std::istream_iterator;
using std::istringstream;
using std::multiplies;
using std::printf;
using std::string;
using std::vector;

static char SUM[] = "+";
static char MIN[] = "min";


/**
 * Returns the number of nodes in this calculation.
 *
 * This abstracts away the chosen messaging library.
 *
 * @return the number of nodes in this calculation
 */
int64_t pagoda::num_nodes()
{
    return ProcessGroup::get_default().get_size();
}


/**
 * Returns the ID of this node in this calculation.
 *
 * This abstracts away the chosen messaging library.
 *
 * @return the ID of this node in this calculation.
 */
int64_t pagoda::nodeid()
{
    return ProcessGroup::get_default().get_rank();
}


/**
 * Parallel barrier.
 */
void pagoda::barrier()
{
    ProcessGroup::get_default().barrier();
}


/**
 * Abort the parallel application.
 *
 * @param[in] message message to print before aborting
 */
void pagoda::abort(const string &message)
{
#if HAVE_GA
    GA_Error(const_cast<char*>(message.c_str()), 1);
#elif HAVE_MPI
    cerr << "[" << nodeid() << "] " << message << endl;
    MPI_Abort(MPI_COMM_WORLD, 1);
#else
#   error
#endif
}


/**
 * Abort the parallel application.
 *
 * @param[in] message message to print before aborting
 * @param[in] errorcode
 */
void pagoda::abort(const string &message, int errorcode)
{
#if HAVE_GA
    GA_Error(const_cast<char*>(message.c_str()), errorcode);
#elif HAVE_MPI
    cerr << "[" << nodeid() << "] " << message << " :: " << errorcode << endl;
    MPI_Abort(MPI_COMM_WORLD, errorcode);
#else
#   error
#endif
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


static int64_t op_lohi_diff(int64_t lo, int64_t hi)
{
    return hi-lo+1;
}


/**
 * Returns the shape of the given region.
 *
 * @param[in] lo
 * @param[in] hi
 * @return the shape
 */
vector<int64_t> pagoda::get_shape(const vector<int64_t> &lo,
                                  const vector<int64_t> &hi)
{
    vector<int64_t> ret(lo.size());


#if 0
    for (size_t i=0,limit=lo.size(); i<limit; ++i) {
        ret[i] = hi[i]-lo[i]+1;
    }
#else
    std::transform(lo.begin(), lo.end(), hi.begin(), ret.begin(), op_lohi_diff);
#endif

    return ret;
}


/**
 * Returns the total number of elements in the given array shape.
 *
 * @param[in] shape the shape of an Array
 * @return the total number of elements in the given array shape.
 */
int64_t pagoda::shape_to_size(const vector<int64_t> &shape)
{
    return accumulate(shape.begin(),shape.end(),1,multiplies<int64_t>());
}


/**
 * Returns true if string str starts with the string start.
 *
 * @param[in] fullString the string being queried
 * @param[in] prefix the string to test with
 * @return true if the given string starts with the given prefix
 */
bool pagoda::starts_with(const string &fullString, const string &prefix)
{
    size_t len_string = fullString.length();
    size_t len_prefix = prefix.length();
    size_t arg1 = 0;

    if (len_string > len_prefix) {
        return (0 == fullString.compare(arg1, len_prefix, prefix));
    }
    else {
        return false;
    }
}


/**
 * Returns true if string str ends with the string end.
 *
 * @param[in] fullString the string being queried
 * @param[in] ending the ending to test with
 * @return true if the given string ends with the given ending
 */
bool pagoda::ends_with(const string &fullString, const string &ending)
{
    size_t len_string = fullString.length();
    size_t len_ending = ending.length();
    size_t arg1 = len_string - len_ending;

    if (len_string > len_ending) {
        return (0 == fullString.compare(arg1, len_ending, ending));
    }
    else {
        return false;
    }
}


/**
 * Split the given string on all whitespace.
 *
 * @param[in] s the string to split
 * @return the split parts
 */
vector<string> pagoda::split(const string &s)
{
    vector<string> tokens;
    istringstream iss(s);
    copy(istream_iterator<string>(iss),
         istream_iterator<string>(),
         back_inserter<vector<string> >(tokens));
    return tokens;
}


/**
 * Split the given string based on the given delimiter.
 *
 * @param[in] s the string to split
 * @param[in] delimiter the delimiter to use
 * @return the split parts
 */
vector<string> pagoda::split(const string &s, char delimiter)
{
    string token;
    vector<string> tokens;
    istringstream iss(s);
    getline(iss, token, delimiter);
    while (iss) {
        if (!token.empty()) {
            tokens.push_back(token);
        }
        getline(iss, token, delimiter);
    }
    return tokens;
}


void pagoda::trim(string& str)
{
    string::size_type pos = str.find_last_not_of(' ');
    if (pos != string::npos) {
        str.erase(pos + 1);
        pos = str.find_first_not_of(' ');
        if (pos != string::npos) {
            str.erase(0, pos);
        }
    }
    else {
        str.erase(str.begin(), str.end());
    }
}


/**
 * Set a reasonable default amount of memory to use.
 *
 * This is a hack because we want to hide the need for GA's MA library, but
 * it's hard to do.
 */
void pagoda::calculate_required_memory()
{
    calculate_required_memory(vector<Variable*>());
}


/**
 * Attempt to calculate the largest amount of memory needed per-process.
 * This has to do with our use of GA_Pack.  Each process needs roughly
 * at least as much memory as the largest variable in our input netcdf
 * files times 4%.
 *
 * @param[in] vars the Variables used in this calculation
 */
void pagoda::calculate_required_memory(const vector<Variable*> &vars)
{
#if HAVE_GA
    int64_t max_size = 0;
    //double gigabytes = 0;
    string max_name;
    vector<Variable*>::const_iterator var;


    for (var=vars.begin(); var!=vars.end(); ++var) {
        int64_t var_size = shape_to_size((*var)->get_shape());
        if (var_size > max_size) {
            max_size = var_size;
            max_name = (*var)->get_name();
        }
    }
    if (max_size < 4000) {
        max_size = 4000;
        max_name = "NO VARIALBES";
    }
    //gigabytes = 1.0 / 1073741824.0 * max_size * 8;

    //max_size = int64_t(max_size * 0.04);
    max_size = int64_t(max_size / pagoda::num_nodes());
    //gigabytes = 1.0 / 1073741824.0 * max_size * 8;

    if (MA_init(C_DBL, max_size, max_size) == MA_FALSE) {
        char msg[] = "MA_init failed";
        GA_Error(msg, 0);
    }
#endif
}


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


#if ENABLE_BACKTRACE
#   include <execinfo.h>
#   include <stdio.h>
#   include <stdlib.h>
#   if CPP_DEMANGLE
#       include <cxxabi.h>
#   endif
#endif
void pagoda::print_backtrace()
{
#if ENABLE_BACKTRACE
    void *array[10];
    size_t size;
    char **strings;
    size_t i;

    size = backtrace(array, 10);
    strings = backtrace_symbols(array, size);

    printf("Obtained %zd stack frames.\n", size);

    for (i = 0; i < size; i++) {
#   if CPP_DEMANGLE
        string s = strings[i];
        size_t open_paren_loc = s.find_first_of('(');
        size_t plus_loc = s.find_first_of('+');
        if (open_paren_loc != string::npos && plus_loc != string::npos) {
            char *tmp;
            int status;
            string pre  = s.substr(0,open_paren_loc+1);
            string name = s.substr(open_paren_loc+1,plus_loc-open_paren_loc-1);
            string post = s.substr(plus_loc);
            tmp = abi::__cxa_demangle(name.c_str(), NULL, 0, &status);
            printf("%s%s%s\n", pre.c_str(), tmp, post.c_str());
            free(tmp);
        }
        else {
            printf("%s\n", strings[i]);
        }
#   else
        printf("%s\n", strings[i]);
#   endif
    }

    free(strings);
#endif
}
