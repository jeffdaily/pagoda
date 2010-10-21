#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#ifdef HAVE_GETTIMEOFDAY
#   ifdef HAVE_SYS_TIME_H
#       include <sys/time.h>
#   endif
#   ifdef HAVE_UNISTD_H
#       include <unistd.h>
#   endif
#endif

#include <cmath>
#include <ctime>
#include <iostream>
#include <iomanip>
#include <map>
#include <sstream>
#include <string>
#include <utility>

#include "Common.H"
#include "Timing.H"

using std::ceil;
using std::difftime;
using std::endl;
using std::fixed;
using std::left;
using std::log10;
using std::map;
using std::make_pair;
using std::multimap;
using std::ostringstream;
using std::pair;
using std::right;
using std::setprecision;
using std::setw;
using std::string;
using std::time;


static int g_name_width = 14;
static int g_counts_width = 6;
static int g_times_width = 7;
static int g_percent_width = 7;
static int g_log10_adjustment = 3;


static inline void header(ostringstream &out)
{
    out << left
        << setw(g_name_width) << "Function Name"
        << right
        << setw(g_counts_width) << "Calls"
        << setw(g_percent_width) << "Prcnt"
        << setw(g_times_width) << "Times"
        << setw(g_percent_width) << "Prcnt"
        << setw(g_times_width) << "T/call"
        << setw(g_percent_width) << "Prcnt"
        << endl;
}


static inline void line(ostringstream &out,
        string name, int counts, int counts_total,
        int64_t times, int64_t times_total, int64_t times_global)
{
    out << left
        << setw(g_name_width) << name
        << right
        << setw(g_counts_width) << counts
        << setw(g_percent_width) << ((double)(100.0*counts/counts_total))
        << setw(g_times_width) << times
        << setw(g_percent_width) << ((double)(100.0*times/times_total))
        << setw(g_times_width) << ((double)(1.0*times/counts))
        << setw(g_percent_width) << ((double)(100.0*times/times_global))
        << endl;
}


int64_t Timing::start_global;
int64_t Timing::end_global;
TimesMap Timing::times;
CountsMap Timing::counts;


Timing::Timing(const string &name)
    :   name(name)
    ,   start(get_time())
{
    ++counts[name];
}


Timing::~Timing()
{
    int64_t end = get_time();
    int64_t diff = end-start;
    if (diff > 0) {
        int64_t new_sum = times[name] + diff;
        if (new_sum > 0) {
            times[name] = new_sum;
        } else {
            std::cerr << "WARNING: times integer overflow: " << name << endl;
        }
    } else if (diff < 0) {
        std::cerr << "WARNING: diff < 0: " << name << endl;
        std::cerr << "\t" << end << "-" << start << "=" << diff << endl;
    }
}


int64_t Timing::get_time()
{
    int64_t value;

#undef HAVE_GETTIMEOFDAY
#define HAVE_CLOCK_T
#ifdef HAVE_GETTIMEOFDAY
    struct timeval the_time;
    gettimeofday(&the_time, NULL);
    value = the_time.tv_sec * 1000 + the_time.tv_usec;
#elif defined(HAVE_CLOCK_T)
    value = clock();
#else
    value = time(NULL);
#endif

    return value;
}


string Timing::get_stats_calls(bool descending)
{
    ostringstream out;
    size_t name_width = 0;
    CountsMapReverse counts_reverse;
    CountsMap::iterator counts_it = counts.begin();
    CountsMap::iterator counts_end = counts.end();
    unsigned long counts_max = 0;
    unsigned long counts_total = 0;
    int counts_width = 0;
    TimesMap::iterator times_it = times.begin();
    TimesMap::iterator times_end = times.end();
    int64_t times_max = 0;
    int64_t times_total = 0;
    int times_width = 0;

    out << fixed << setprecision(1);

    // create reverse mapping of calls, tally sizes of output columns
    for (; counts_it!=counts_end; ++counts_it) {
        counts_reverse.insert(make_pair(counts_it->second,counts_it->first));
        if (counts_it->first.size() > name_width) {
            name_width = counts_it->first.size();
        }
        if (counts_it->second > counts_max) {
            counts_max = counts_it->second;
        }
        counts_total += counts_it->second;
    }
    if (static_cast<int>(name_width) > g_name_width) {
        g_name_width = name_width;
    }
    counts_width = (int)(ceil(log10((double)counts_max))) + g_log10_adjustment;
    if (counts_width > g_counts_width) {
        g_counts_width = counts_width;
    }
    // determine total time
    for (; times_it!=times_end; ++times_it) {
        if (times_it->second > times_max) {
            times_max = times_it->second;
        }
        times_total += times_it->second;
    }
    times_width = (int)(ceil(log10((double)times_max))) + g_log10_adjustment;
    if (times_width > g_times_width) {
        g_times_width = times_width;
    }

    header(out);

    if (descending) {
        CountsMapReverse::reverse_iterator rit;
        CountsMapReverse::reverse_iterator rend;

        rit = counts_reverse.rbegin();
        rend = counts_reverse.rend();
        for (; rit!=rend; ++rit) {
            line(out, rit->second, rit->first, counts_total,
                    times.find(rit->second)->second, times_total,
                    end_global-start_global);
        }
    } else {
        CountsMapReverse::iterator rit;
        CountsMapReverse::iterator rend;

        rit = counts_reverse.begin();
        rend = counts_reverse.end();
        for (; rit!=rend; ++rit) {
            line(out, rit->second, rit->first, counts_total,
                    times.find(rit->second)->second, times_total,
                    end_global-start_global);
        }
    }

    return out.str();
}


string Timing::get_stats_total_time(bool descending)
{
    ostringstream out;
    size_t name_width = 0;
    TimesMapReverse times_reverse;
    TimesMap::iterator times_it = times.begin();
    TimesMap::iterator times_end = times.end();
    unsigned long times_max = 0;
    unsigned long times_total = 0;
    int times_width = 0;
    CountsMap::iterator counts_it = counts.begin();
    CountsMap::iterator counts_end = counts.end();
    unsigned long counts_max = 0;
    int64_t counts_total = 0;
    int counts_width = 0;

    out << fixed << setprecision(1);

    // create reverse mapping of times, tally sizes of output columns
    for (; times_it!=times_end; ++times_it) {
        times_reverse.insert(make_pair(times_it->second,times_it->first));
        if (times_it->first.size() > name_width) {
            name_width = times_it->first.size();
        }
        if (times_it->second > static_cast<int64_t>(times_max)) {
            times_max = times_it->second;
        }
        times_total += times_it->second;
    }
    times_width = (int)(ceil(log10((double)times_max))) + g_log10_adjustment;
    if (times_width > g_times_width) {
        g_times_width = times_width;
    }
    // determine total time
    for (; counts_it!=counts_end; ++counts_it) {
        if (counts_it->second > counts_max) {
            counts_max = counts_it->second;
        }
        counts_total += counts_it->second;
    }
    counts_width = (int)(ceil(log10((double)counts_max))) + g_log10_adjustment;
    if (counts_width > g_counts_width) {
        g_counts_width = counts_width;
    }

    out << endl;
    out << "TOTAL TIME=" << times_total << endl;
    out << "TOTAL CALLS=" << counts_total << endl;
    out << endl;

    header(out);

    if (descending) {
        TimesMapReverse::reverse_iterator rit;
        TimesMapReverse::reverse_iterator rend;

        rit = times_reverse.rbegin();
        rend = times_reverse.rend();
        for (; rit!=rend; ++rit) {
            line(out, rit->second, counts.find(rit->second)->second,
                    counts_total, rit->first, times_total,
                    end_global-start_global);
        }
    } else {
        TimesMapReverse::iterator rit;
        TimesMapReverse::iterator rend;

        rit = times_reverse.begin();
        rend = times_reverse.end();
        for (; rit!=rend; ++rit) {
            line(out, rit->second, counts.find(rit->second)->second,
                    counts_total, rit->first, times_total,
                    end_global-start_global);
        }
    }

    return out.str();
}
