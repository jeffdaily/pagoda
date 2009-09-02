#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <string>
#include <vector>

#include <ga.h>
#include <macdecls.h>

#include "Debug.H"
#include "Util.H"
#include "Variable.H"

using std::string;
using std::vector;


/**
 * Returns true if string str ends with the string end.
 */
bool Util::ends_with(string const &fullString, string const &ending)
{
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
 * Attempt to calculate the largest amount of memory needed per-process.
 * This has to do with our use of GA_Pack.  Each process needs roughly
 * at least as much memory as the largest variable in our input netcdf
 * files times 4%.
 */
void Util::calculate_required_memory(const vector<Variable*> &vars)
{
    TRACER("Util::calculate_required_memory BEGIN\n");
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

    int64_t max_size8 = max_size*8;
    double gigabytes = 1.0 / 1073741824.0 * max_size8;

    TRACER3("MA max variable '%s' is %ld bytes (%f gigabytes)\n",
            max_name.c_str(), max_size8, gigabytes);
    max_size *= 0.04;
    max_size8 = max_size*8;
    gigabytes = 1.0 / 1073741824.0 * max_size8;
    TRACER2("MA max memory %ld bytes (%f gigabytes)\n", max_size8, gigabytes);

    if (MA_init(MT_DBL, max_size, max_size) == MA_FALSE) {
        char msg[] = "MA_init failed";
        GA_Error(msg, 0);
    }
    TRACER("Util::calculate_required_memory END\n");
}
