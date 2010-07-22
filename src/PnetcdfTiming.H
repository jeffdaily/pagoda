#ifndef PNETCDFTIMING_H
#define PNETCDFTIMING_H

#ifdef HAVE_STDINT_H
#   include <stdint.h>
#endif
#ifdef HAVE_INTTYPES_H
#   include <inttypes.h>
#endif

#include <map>
#include <string>

#include <mpi.h>
#include <pnetcdf.h>

#ifdef GATHER_PNETCDF_TIMING
#   define PNETCDF_TIMING1(A1)       PnetcdfTiming __timing_var(A1)
#   define PNETCDF_TIMING3(A1,A2,A3) PnetcdfTiming __timing_var(A1,A2,A3)
#else
#   define PNETCDF_TIMING1(A1)
#   define PNETCDF_TIMING3(A1,A2,A3)
//#   warning pnetcdf timing is turned off
#endif

using std::map;
using std::multimap;
using std::string;


typedef map<string,uint64_t> PnetcdfIOMap;
typedef multimap<uint64_t,string> PnetcdfIOMapR;


/**
 * Gather timing data for pnetcdf calls.
 */
class PnetcdfTiming
{
    public:
        PnetcdfTiming(const string &name);
        PnetcdfTiming(const string &name, MPI_Offset count, nc_type type);
        PnetcdfTiming(const string &name, const vector<MPI_Offset> &count,
                nc_type type);
        ~PnetcdfTiming();

        static uint64_t get_time();
        static string get_stats_calls(bool descending=true);
        static string get_stats_aggregate();

        static uint64_t start_global;
        static uint64_t end_global;

    protected:
        void calc_bytes(const vector<MPI_Offset> &count, nc_type type);

        string name;
        uint64_t start;

        static PnetcdfIOMap times;
        static PnetcdfIOMap bytes;
        static PnetcdfIOMap calls;
};

#endif // PNETCDFTIMING_H
