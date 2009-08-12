#ifdef HAVE_CONFIG_H
#include "config.h"
#endif // HAVE_CONFIG_H

#include <functional>
    using std::bind2nd;
    using std::ptr_fun;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include <ga.h>
#include <macdecls.h>

#include "Attribute.H"
#include "Dimension.H"
#include "StringComparator.H"
#include "Util.H"
#include "Variable.H"


#ifdef F77_DUMMY_MAIN
#  ifdef __cplusplus
     extern "C"
#  endif
   int F77_DUMMY_MAIN() { return 1; }
#endif



/**
 * Returns true if string str ends with the string end.
 */
bool Util::ends_with(string const &fullString, string const &ending)
{
    if (fullString.length() > ending.length()) {
        return (0 == fullString.compare(fullString.length() - ending.length(), ending.length(), ending));
    } else {
        return false;
    }
}


/**
 * Attempt to calculate the largest amount of memory needed per-process.
 * This has to do with our use of GA_Pack.  Each process needs roughly
 * at least as much memory as the largest variable in our input netcdf
 * files divided by the total number of processes.
 */
void Util::calculate_required_memory(const vector<Variable*> &vars)
{
    int64_t max_size = 0;
    string max_name;
    vector<Variable*>::const_iterator var;

    for (var=vars.begin(); var!=vars.end(); ++var) {
        int64_t var_size = (*var)->get_size();
        if (var_size > max_size) {
            max_size = var_size;
            max_name = (*var)->get_name();
        }
    }

    max_size /= GA_Nnodes();
    DEBUG_PRINT_ME1("MA max memory %llu bytes\n", max_size*8);
    DEBUG_PRINT_ME1("MA max variable %s\n", max_name.c_str());
    if (MA_init(MT_DBL, max_size, max_size) == MA_FALSE) {
        char msg[] = "MA_init failed";
        GA_Error(msg, 0);
    }
}


