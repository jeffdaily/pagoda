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


static inline void line(ostringstream &out,
        int name_width, string name,
        int counts_width, int counts, int counts_total,
        int times_width, int64_t times, int64_t times_total,
        int percent_width, int times_per_call_width)
{
    out << left
        << setw(name_width) << name
        << right
        << setw(counts_width) << counts
        << setw(percent_width) << ((double)(100.0*counts/counts_total)) << '%'
        << setw(times_width) << times
        << setw(percent_width) << ((double)(100.0*times/times_total)) << '%'
        << setw(times_per_call_width) << ((double)(1.0*times/counts))
        << endl;
}


TimesMap Timing::times;
CountsMap Timing::counts;


Timing::Timing(const string &name)
    :   name(name)
    ,   start()
{
    ++counts[name];
#ifdef HAVE_GETTIMEOFDAY
    struct timeval _start;
    gettimeofday(&_start, NULL);
    start = _start.tv_sec * 1000 + _start.tv_usec;
#elif defined(HAVE_CLOCK_T)
    start = clock();
#else
    start = time(NULL);
#endif
}


Timing::~Timing()
{
    int64_t end;
#ifdef HAVE_GETTIMEOFDAY
    struct timeval _end;
    gettimeofday(&_end, NULL);
    end = _end.tv_sec * 1000 + _end.tv_usec;
    /*
    if ((end-start) < 0 && 0 == ME) {
        std::cout << "WARNING: end-start < 0 in " << name << endl;
        std::cout << "end=" << end << endl;
        std::cout << "start=" << start << endl;
        std::cout << _end.tv_sec
            << " " << _end.tv_usec
            << " " << (_end.tv_sec*1000)
            << " " << (_end.tv_sec*1000 +_end.tv_usec)
            << endl;
    }
    */
#elif defined(HAVE_CLOCK_T)
    end = clock();
#else
    end = time(NULL);
#endif
    /*
    if (0 == ME) {
        std::cout << name
            << " " << start
            << " " << end
            << " " << (end-start)<< endl;
    }
    */
    times[name] += end-start;
}


string Timing::get_stats_calls(bool descending)
{
    ostringstream out;
    size_t name_width = 0;
    int percent_width = 5;
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
    counts_width = (int)(ceil(log10(counts_max))) + 1;
    if (counts_width < 6) {
        counts_width = 6;
    }
    // determine total time
    for (; times_it!=times_end; ++times_it) {
        if (times_it->second > times_max) {
            times_max = times_it->second;
        }
        times_total += times_it->second;
        /*
        if (0 == ME) {
            std::cout << times_it->first << " " << times_it->second << endl;
        }
        */
    }
    times_width = (int)(ceil(log10(times_max))) + 4;
    if (times_width < 6) {
        times_width = 6;
    }

    // header
    out << setw(name_width) << left << " Function Name"
        << setw(counts_width) << " Calls"
        << setw(percent_width) << " Prcnt"
        << setw(times_width) << " Times"
        << setw(percent_width) << " Prcnt"
        << setw(times_width) << " T/call"
        << endl;

    if (descending) {
        CountsMapReverse::reverse_iterator rit;
        CountsMapReverse::reverse_iterator rend;

        rit = counts_reverse.rbegin();
        rend = counts_reverse.rend();
        for (; rit!=rend; ++rit) {
            line(out, name_width, rit->second,
                    counts_width, rit->first, counts_total,
                    times_width, times.find(rit->second)->second, times_total,
                    percent_width, times_width);
        }
    } else {
        CountsMapReverse::iterator rit;
        CountsMapReverse::iterator rend;

        rit = counts_reverse.begin();
        rend = counts_reverse.end();
        for (; rit!=rend; ++rit) {
            line(out, name_width, rit->second,
                    counts_width, rit->first, counts_total,
                    times_width, times.find(rit->second)->second, times_total,
                    percent_width, times_width);
        }
    }

    return out.str();
}


string Timing::get_stats_total_time(bool descending)
{
    ostringstream out;
    size_t name_width = 0;
    int percent_width = 5;
    TimesMapReverse times_reverse;
    TimesMap::iterator times_it = times.begin();
    TimesMap::iterator times_end = times.end();
    unsigned long times_max = 0;
    unsigned long times_total = 0;
    int times_width = 0;
    CountsMap::iterator counts_it = counts.begin();
    CountsMap::iterator counts_end = counts.end();
    int64_t counts_max = 0;
    int64_t counts_total = 0;
    int counts_width = 0;

    out << fixed << setprecision(1);

    // create reverse mapping of times, tally sizes of output columns
    for (; times_it!=times_end; ++times_it) {
        times_reverse.insert(make_pair(times_it->second,times_it->first));
        if (times_it->first.size() > name_width) {
            name_width = times_it->first.size();
        }
        if (times_it->second > times_max) {
            times_max = times_it->second;
        }
        times_total += times_it->second;
    }
    times_width = (int)(ceil(log10(times_max))) + 4;
    if (times_width < 6) {
        times_width = 6;
    }
    // determine total time
    for (; counts_it!=counts_end; ++counts_it) {
        if (counts_it->second > counts_max) {
            counts_max = counts_it->second;
        }
        counts_total += counts_it->second;
        /*
        if (0 == ME) {
            std::cout << counts_it->first << " " << counts_it->second << endl;
        }
        */
    }
    counts_width = (int)(ceil(log10(counts_max))) + 1;
    if (counts_width < 6) {
        counts_width = 6;
    }

    out << endl;
    out << "TOTAL TIME=" << times_total << endl;
    out << "TOTAL CALLS=" << counts_total << endl;
    out << endl;

    // header
    out << setw(name_width) << left << "Function Name"
        << setw(counts_width) << " Calls"
        << setw(percent_width) << " Prcnt"
        << setw(times_width) << " Times"
        << setw(percent_width) << " Prcnt"
        << setw(times_width) << " T/call"
        << endl;

    if (descending) {
        TimesMapReverse::reverse_iterator rit;
        TimesMapReverse::reverse_iterator rend;

        rit = times_reverse.rbegin();
        rend = times_reverse.rend();
        for (; rit!=rend; ++rit) {
            line(out, name_width, rit->second,
                    counts_width, counts.find(rit->second)->second,
                    counts_total, times_width, rit->first, times_total,
                    percent_width, times_width);
        }
    } else {
        TimesMapReverse::iterator rit;
        TimesMapReverse::iterator rend;

        rit = times_reverse.begin();
        rend = times_reverse.end();
        for (; rit!=rend; ++rit) {
            line(out, name_width, rit->second,
                    counts_width, counts.find(rit->second)->second,
                    counts_total, times_width, rit->first, times_total,
                    percent_width, times_width);
        }
    }

    return out.str();
}
