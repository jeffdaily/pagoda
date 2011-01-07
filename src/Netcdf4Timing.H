#ifndef NETCDF4TIMING_H
#define NETCDF4TIMING_H

#ifdef HAVE_STDINT_H
#   include <stdint.h>
#endif
#ifdef HAVE_INTTYPES_H
#   include <inttypes.h>
#endif

#include <map>
#include <string>

#include <mpi.h>
#include <netcdf.h>

#ifdef GATHER_NETCDF4_TIMING
#   define NETCDF4_TIMING1(A1)       Netcdf4Timing __timing_var(A1)
#   define NETCDF4_TIMING3(A1,A2,A3) Netcdf4Timing __timing_var(A1,A2,A3)
#else
#   define NETCDF4_TIMING1(A1)
#   define NETCDF4_TIMING3(A1,A2,A3)
//#   warning netcdf timing is turned off
#endif

using std::map;
using std::multimap;
using std::string;


typedef map<string,uint64_t> Netcdf4IOMap;
typedef multimap<uint64_t,string> Netcdf4IOMapR;


/**
 * Gather timing data for netcdf calls.
 */
class Netcdf4Timing
{
    public:
        Netcdf4Timing(const string &name);
        Netcdf4Timing(const string &name, size_t count, nc_type type);
        Netcdf4Timing(const string &name, const vector<size_t> &count,
                      nc_type type);
        ~Netcdf4Timing();

        static uint64_t get_time();
        static string get_stats_calls(bool descending=true);
        static string get_stats_aggregate();

        static uint64_t start_global;
        static uint64_t end_global;

    protected:
        void calc_bytes(const vector<size_t> &count, nc_type type);

        string name;
        uint64_t start;

        static Netcdf4IOMap times;
        static Netcdf4IOMap bytes;
        static Netcdf4IOMap calls;
};

#endif // NETCDF4TIMING_H
