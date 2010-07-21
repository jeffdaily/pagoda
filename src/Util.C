#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#define TRACE

#include <algorithm>
#include <numeric>
#include <string>
#include <vector>

#if HAVE_GA
#   include <ga.h>
#   include <macdecls.h>
#elif HAVE_MPI
#   include <mpi.h>
#endif

#include "Debug.H"
#include "Timing.H"
#include "Util.H"
#include "Variable.H"

using std::accumulate;
using std::back_inserter;
using std::copy;
using std::istream_iterator;
using std::istringstream;
using std::multiplies;
using std::string;
using std::vector;


/**
 * Returns the number of nodes in this calculation.
 *
 * This abstracts away the chosen messaging library.
 *
 * @return the number of nodes in this calculation
 */
int64_t pagoda::num_nodes()
{
#if HAVE_GA
    return GA_Nnodes();
#elif HAVE_MPI
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    return rank;
#else
#   error
#endif
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
#if HAVE_GA
    return GA_Nodeid();
#elif HAVE_MPI
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    return size;
#else
#   error
#endif
}


/**
 * Parallel barrier.
 */
void pagoda::barrier()
{
#if HAVE_GA
    GA_Sync();
#elif HAVE_MPI
    MPI_Barrier();
#else
#   error
#endif
}


/**
 * Abort the parallel application.
 *
 * @param[in] message message to print before aborting
 */
void pagoda::abort(const char *message)
{
#if HAVE_GA
    GA_Error(const_cast<char*>(message), 0);
#elif HAVE_MPI
    cerr << "[" << nodeid() << "] " << message << endl;
    MPI_Abort(MPI_COMM_WORLD, 0);
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
void pagoda::abort(const char *message, int errorcode)
{
#if HAVE_GA
    GA_Error(const_cast<char*>(message), errorcode);
#elif HAVE_MPI
    cerr << "[" << nodeid() << "] " << message << " :: " << errorcode << endl;
    MPI_Abort(MPI_COMM_WORLD, errorcode);
#else
#   error
#endif
}


/**
 * Returns the minimum of all values from all processes.
 *
 * @param[in,out] values the values to take the minimums of
 */ 
void pagoda::gop_min(vector<long> &values)
{
#if HAVE_GA
    GA_Lgop(&values[0], values.size(), "min");
#elif HAVE_MPI
    MPI_Allreduce(&values[0], &values[0], values.size(),
            MPI_LONG, MPI_MIN, MPI_COMM_WORLD);
#else
#   error
#endif
}


/**
 * Returns the sum of all values from all processes.
 *
 * @param[in,out] values the values to take the sums of
 */ 
void pagoda::gop_sum(vector<int> &values)
{
#if HAVE_GA
    GA_Igop(&values[0], values.size(), "+");
#elif HAVE_MPI
    MPI_Allreduce(&values[0], &values[0], values.size(),
            MPI_INT, MPI_SUM, MPI_COMM_WORLD);
#else
#   error
#endif
}


/**
 * Returns the sum of all values from all processes.
 *
 * @param[in,out] values the values to take the sums of
 */ 
void pagoda::gop_sum(vector<long> &values)
{
#if HAVE_GA
    GA_Lgop(&values[0], values.size(), "+");
#elif HAVE_MPI
    MPI_Allreduce(&values[0], &values[0], values.size(),
            MPI_LONG, MPI_SUM, MPI_COMM_WORLD);
#else
#   error
#endif
}


/**
 * Returns the sum of all values from all processes.
 *
 * @param[in,out] values the values to take the sums of
 */ 
void pagoda::gop_sum(vector<long long> &values)
{
#if HAVE_GA && HAVE_GA_LLGOP
    GA_Llgop(&values[0], values.size(), "+");
#elif HAVE_MPI
    MPI_Allreduce(&values[0], &values[0], values.size(),
            MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
#else
#   error
#endif
}


#if NEED_VECTOR_INT64_T_GOP
/**
 * Returns the sum of all values from all processes.
 *
 * @param[in,out] values the values to take the sums of
 */ 
void pagoda::gop_sum(vector<int64_t> &values)
{
#   if HAVE_GA && SIZEOF_INT64_T == SIZEOF_LONG_LONG && HAVE_GA_LLGOP
    GA_Llgop(&values[0], values.size(), "+");
#   elif HAVE_GA && SIZEOF_INT64_T == SIZEOF_LONG
    GA_Lgop(&values[0], values.size(), "+");
#   elif HAVE_GA && SIZEOF_INT64_T == SIZEOF_INT
    GA_Igop(&values[0], values.size(), "+");
#   elif HAVE_MPI && SIZEOF_INT64_T == SIZEOF_LONG_LONG
    MPI_Allreduce(&values[0], &values[0], values.size(), MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
#   elif HAVE_MPI && SIZEOF_INT64_T == SIZEOF_LONG
    MPI_Allreduce(&values[0], &values[0], values.size(), MPI_LONG, MPI_SUM, MPI_COMM_WORLD);
#   elif HAVE_MPI && SIZEOF_INT64_T == SIZEOF_INT
    MPI_Allreduce(&values[0], &values[0], values.size(), MPI_INT, MPI_SUM, MPI_COMM_WORLD);
#   else
#       error
#   endif
}
#endif /* NEED_VECTOR_INT64_T_GOP */


/**
 * Returns the sum of all values from all processes.
 *
 * @param[in,out] values the values to take the sums of
 */ 
void pagoda::gop_sum(vector<float> &values)
{
#if HAVE_GA
    GA_Fgop(&values[0], values.size(), "+");
#elif HAVE_MPI
    MPI_Allreduce(&values[0], &values[0], values.size(),
            MPI_FLOAT, MPI_SUM, MPI_COMM_WORLD);
#else
#   error
#endif
}


/**
 * Returns the sum of all values from all processes.
 *
 * @param[in,out] values the values to take the sums of
 */ 
void pagoda::gop_sum(vector<double> &values)
{
#if HAVE_GA
    GA_Dgop(&values[0], values.size(), "+");
#elif HAVE_MPI
    MPI_Allreduce(&values[0], &values[0], values.size(),
            MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
#else
#   error
#endif
}


/**
 * Returns the sum of all values from all processes.
 *
 * @param[in,out] values the values to take the sums of
 */ 
void pagoda::gop_sum(vector<long double> &values)
{
#if HAVE_GA && HAVE_GA_LDGOP
    GA_Ldgop(&values[0], values.size(), "+");
#elif HAVE_MPI
    MPI_Allreduce(&values[0], &values[0], values.size(),
            MPI_LONG_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
#else
#   error
#endif
}


/**
 * Returns the shape of the given region.
 *
 * @param[in] lo
 * @param[in] hi
 * @return the shape
 */
vector<int64_t> pagoda::get_shape(const vector<int64_t> &lo, const vector<int64_t> &hi)
{
    vector<int64_t> ret(lo.size());

    TIMING("pagoda::get_shape(vector<int64_t>,vector<int64_t>)");

    for (size_t i=0,limit=lo.size(); i<limit; ++i) {
        ret[i] = hi[i]-lo[i]+1;
    }

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
 * Returns true if string str ends with the string end.
 *
 * @param[in] fullString the string being queried
 * @param[in] ending the ending to test with
 * @return true if the given string ends with the given ending
 */
bool pagoda::ends_with(const string &fullString, const string &ending)
{
    TIMING("pagoda::ends_with(string,string)");
    size_t len_string = fullString.length();
    size_t len_ending = ending.length();
    size_t arg1 = len_string - len_ending;

    if (len_string > len_ending) {
        return (0 == fullString.compare(arg1, len_ending, ending));
    } else {
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
    double gigabytes = 0;
    string max_name;
    vector<Variable*>::const_iterator var;

    TIMING("pagoda::calculate_required_memory(vector<Variable*>)");
    TRACER("pagoda::calculate_required_memory BEGIN\n");

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
    gigabytes = 1.0 / 1073741824.0 * max_size * 8;
    TRACER("MA max variable '%s' is %ld bytes (%f gigabytes)\n",
            max_name.c_str(), max_size*8, gigabytes);

    //max_size = int64_t(max_size * 0.04);
    max_size = int64_t(max_size / pagoda::num_nodes());
    gigabytes = 1.0 / 1073741824.0 * max_size * 8;
    TRACER("MA max memory %ld bytes (%f gigabytes)\n", max_size*8, gigabytes);

    if (MA_init(MT_DBL, max_size, max_size) == MA_FALSE) {
        char msg[] = "MA_init failed";
        GA_Error(msg, 0);
    }
    TRACER("pagoda::calculate_required_memory END\n");
#endif
}
