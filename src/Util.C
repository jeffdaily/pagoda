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

#if HAVE_GA
#   include <ga.h>
#   include <macdecls.h>
#endif

#include "Bootstrap.H"
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
