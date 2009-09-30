#ifndef TIMING_H
#define TIMING_H

#include <ctime>
#include <map>
#include <string>
#include <utility>

#if defined(GATHER_TIMING)
#   define TIMING(name) Timing __functime(name)
#else
#   define TIMING(name)
#endif

using std::map;
using std::multimap;
using std::pair;
using std::string;


typedef map<string,int64_t> TimesMap;
typedef map<int64_t,string> TimesMapReverse;
typedef pair<string,int64_t> TimesPair;
typedef pair<int64_t,string> TimesPairReverse;
typedef map<string,unsigned long> CountsMap;
typedef multimap<unsigned long,string> CountsMapReverse;
typedef pair<string,unsigned long> CountsPair;
typedef pair<unsigned long,string> CountsPairReverse;


class Timing
{
    public:
        Timing(const string &name);
        ~Timing();

        static string get_stats_calls(bool descending=true);
        static string get_stats_total_time(bool descending=true);
        static string get_stats_time_per_call(bool descending=true);

    protected:
        string name;
        int64_t start;

        static TimesMap times;
        static CountsMap counts;
};

#endif // TIMING_H
