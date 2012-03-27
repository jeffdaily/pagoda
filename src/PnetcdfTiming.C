#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#ifdef HAVE_TIME_H
#   include <time.h>
#endif
#ifdef HAVE_SYS_TIME_H
#   include <sys/time.h>
#endif
#ifdef HAVE_UNISTD_H
#   include <unistd.h>
#endif

#define USE_BARRIERS
#ifdef USE_BARRIERS
#   define BARRIER() pagoda::barrier()
#else
#   define BARRIER()
#endif

#include <cmath>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <mpi.h>
#include <pnetcdf.h>

#include "Bootstrap.H"
#include "PnetcdfError.H"
#include "PnetcdfTiming.H"
#include "Print.H"

using std::cerr;
using std::endl;
using std::fixed;
using std::left;
using std::ostringstream;
using std::right;
using std::setprecision;
using std::setw;


uint64_t PnetcdfTiming::start_global;
uint64_t PnetcdfTiming::end_global;
PnetcdfIOMap PnetcdfTiming::times;
PnetcdfIOMap PnetcdfTiming::bytes;
PnetcdfIOMap PnetcdfTiming::calls;
bool PnetcdfTiming::time_wait_is_reading(false);
bool PnetcdfTiming::time_wait_is_writing(false);
uint64_t PnetcdfTiming::time_wait_read(0);
uint64_t PnetcdfTiming::time_wait_write(0);


static int g_name_width = 14;
static int g_calls_width = 6;
static int g_times_width = 7;
static int g_percent_width = 7;
static int g_bytes_width = 7;
static int g_log10_adjustment = 3;
#if HAVE_CLOCK_GETTIME
// converts bytes/nanosecond to gigabytes/second
static double g_io_multiplier = 1000000000.0/1073741824.0; // 0.931322575
// converts nanoseconds to seconds
static double g_time_multiplier = 1.0/1000000000.0;
static string UNIT_SIZE("bytes");
static string UNIT_TIME("nanoseconds");
#elif HAVE_GETTIMEOFDAY
// converts bytes/microsecond to gigabytes/second
static double g_io_multiplier = 1000000.0/1073741824.0; // 0.000931322575
static string UNIT_SIZE("bytes");
static string UNIT_TIME("microseconds");
#else
// converts bytes/second to gigabytes/second
static double g_io_multiplier = 1.0/1073741824.0;
static string UNIT_SIZE("bytes");
static string UNIT_TIME("seconds");
#endif


static inline void header(ostringstream &out)
{
    out << left
        << setw(g_name_width) << "Function Name"
        << right
        << setw(g_calls_width) << "Calls"
        << setw(g_percent_width) << "Prcnt"
        << setw(g_times_width) << "Times"
        << setw(g_percent_width) << "Prcnt"
        << setw(g_times_width) << "T/Call"
        << setw(g_percent_width) << "Prcnt"
        << setw(g_bytes_width) << "Bytes"
        << setw(g_bytes_width) << "B/Time"
        << endl;
}


static inline void line(ostringstream &out, string name,
                        int calls, int calls_total,
                        uint64_t times, uint64_t times_total, uint64_t times_global,
                        uint64_t bytes)
{
    out << left
        << setw(g_name_width) << name
        << right
        << setw(g_calls_width) << calls
        << setw(g_percent_width) << (100.0*calls/calls_total)
        << setw(g_times_width) << times
        << setw(g_percent_width) << (100.0*times/times_total)
        << setw(g_times_width) << (1.0*times/calls)
        << setw(g_percent_width) << (100.0*times/times_global)
        << setw(g_bytes_width) << bytes
        << setw(g_bytes_width) << (1.0*bytes/times)
        << endl;
}


// create reverse mapping; calculate sizes of output columns
static inline void reverse_info(
    const PnetcdfIOMap &iomap,
    PnetcdfIOMapR &iomap_reverse,
    uint64_t &total, int &width, int &name_width)
{
    uint64_t max = 0;

    total = 0;
    width = 0;
    name_width = 0;
    for (PnetcdfIOMap::const_iterator it=iomap.begin(),end=iomap.end();
            it != end; ++it) {
        string name = it->first;
        uint64_t data = it->second;

        iomap_reverse.insert(make_pair(data,name));
        if (static_cast<int>(name.size()) > name_width) {
            name_width = name.size();
        }
        if (data > max) {
            max = data;
        }
        total += data;
    }
    if (name_width > g_name_width) {
        g_name_width = name_width;
    }
    width = (int)(ceil(log10((double)max))) + g_log10_adjustment;
}


static inline void calc_info(const PnetcdfIOMap &iomap,
                             uint64_t &total, int &width)
{
    uint64_t max = 0;

    total = 0;
    width = 0;
    for (PnetcdfIOMap::const_iterator it=iomap.begin(), end=iomap.end();
            it != end; ++it) {
        uint64_t data = it->second;
        if (data > max) {
            max = data;
        }
        total += data;
    }
    width = (int)(ceil(log10((double)max))) + g_log10_adjustment;
}


PnetcdfTiming::PnetcdfTiming(const string &name)
    :   name(name)
    ,   start(0)
{
    ++calls[name];

    if (name.find("iget_") != string::npos) {
        time_wait_is_reading = true;
    }
    else if (name.find("iput_") != string::npos) {
        time_wait_is_writing = true;
    }

    BARRIER();
    start = get_time();
}


PnetcdfTiming::PnetcdfTiming(
    const string &name,
    MPI_Offset count,
    nc_type type)
    :   name(name)
    ,   start(0)
{
    // calls
    ++calls[name];

    // bytes
    calc_bytes(vector<MPI_Offset>(1,count), type);

    if (name.find("iget_") != string::npos) {
        time_wait_is_reading = true;
    }
    else if (name.find("iput_") != string::npos) {
        time_wait_is_writing = true;
    }

    BARRIER();
    start = get_time();
}


PnetcdfTiming::PnetcdfTiming(
    const string &name,
    const vector<MPI_Offset> &count,
    nc_type type)
    :   name(name)
    ,   start(0)
{
    // calls
    ++calls[name];

    // bytes
    calc_bytes(count, type);

    if (name.find("iget_") != string::npos) {
        time_wait_is_reading = true;
    }
    else if (name.find("iput_") != string::npos) {
        time_wait_is_writing = true;
    }

    BARRIER();
    start = get_time();
}


void PnetcdfTiming::calc_bytes(const vector<MPI_Offset> &count, nc_type type)
{
    MPI_Offset size = 1;
    uint64_t new_size = 0;

    for (int i=0,limit=count.size(); i<limit; ++i) {
        size *= count[i];
    }
    switch (type) {
        case NC_BYTE:
            size *= 1;
            break;
        case NC_CHAR:
            size *= 1;
            break;
        case NC_SHORT:
            size *= 2;
            break;
        case NC_INT:
            size *= 4;
            break;
        case NC_FLOAT:
            size *= 4;
            break;
        case NC_DOUBLE:
            size *= 8;
            break;
        default:
            ERR("type not recognized");
    }
    new_size = bytes[name] + size;
    if (new_size < bytes[name]) {
        cerr << pagoda::me << " WARNING: bytes overrun" << endl;
    }
    else {
        bytes[name] = new_size;
    }
}


PnetcdfTiming::~PnetcdfTiming()
{
    uint64_t end = get_time();
    //BARRIER();
    //uint64_t end_observed = get_time();

    if (end > start) {
        uint64_t diff = end-start;
        uint64_t new_sum = times[name] + diff;

        if (new_sum > times[name]) {
            times[name] = new_sum;
        }
        else {
            cerr << "WARNING: times overrun: " << name << endl;
        }
        if (name.find("wait") != string::npos) {
            if (time_wait_is_reading) {
                time_wait_is_reading = false;
                uint64_t time_wait_read_new_sum = time_wait_read + diff;
                if (time_wait_read_new_sum > time_wait_read) {
                    time_wait_read = time_wait_read_new_sum;
                } else {
                    cerr << "WARNING: times overrun: time_wait_read" << endl;
                }
            } else if (time_wait_is_writing) {
                time_wait_is_writing = false;
                uint64_t time_wait_write_new_sum = time_wait_write + diff;
                if (time_wait_write_new_sum > time_wait_write) {
                    time_wait_write = time_wait_write_new_sum;
                } else {
                    cerr << "WARNING: times overrun: time_wait_write" << endl;
                }
            }
        }
    }
}


uint64_t PnetcdfTiming::get_time()
{
    uint64_t value;

#if HAVE_CLOCK_GETTIME
    struct timespec tp;
    (void)clock_gettime(CLOCK_MONOTONIC, &tp);
    //(void)clock_gettime(CLOCK_REALTIME, &tp);
    value = tp.tv_sec;
    value *= 1000000000;
    value += tp.tv_nsec;
#elif HAVE_GETTIMEOFDAY
    struct timeval tv;
    gettimeofday(&tv, NULL);
    value = tv.tv_sec;
    value *= 1000000;
    value += tv.tv_usec;
#elif HAVE_CLOCK
    value = (uint64_t)(1.0*clock()/CLOCKS_PER_SEC);
#else
    value = time(NULL);
#endif

    return value;
}


string PnetcdfTiming::get_stats_calls(bool descending)
{
    ostringstream out;
    PnetcdfIOMapR calls_reverse;
    int name_width = 0;
    int calls_width = 0;
    int times_width = 0;
    int bytes_width = 0;
    uint64_t calls_total = 0;
    uint64_t times_total = 0;
    uint64_t bytes_total = 0;

    out << fixed << setprecision(1);

    // create reverse mapping of calls; calculate sizes of output columns
    reverse_info(calls, calls_reverse, calls_total, calls_width, name_width);
    if (name_width > g_name_width) {
        g_name_width = name_width;
    }
    if (calls_width > g_calls_width) {
        g_calls_width = calls_width;
    }

    // calculate total times, max times, size of times columns
    calc_info(times, times_total, times_width);
    if (times_width > g_times_width) {
        g_times_width = times_width;
    }

    // calculate total bytes, max bytes, size of bytes columns
    calc_info(bytes, bytes_total, bytes_width);
    if (bytes_width > g_bytes_width) {
        g_bytes_width = bytes_width;
    }

    header(out);

    if (descending) {
        for (PnetcdfIOMapR::reverse_iterator it=calls_reverse.rbegin(),
                end=calls_reverse.rend(); it!=end; ++it) {
            string name = it->second;
            PnetcdfIOMap::iterator times_it = times.find(name);
            PnetcdfIOMap::iterator bytes_it = bytes.find(name);
            uint64_t _times = (times_it == times.end()) ? 0 : times_it->second;
            uint64_t _bytes = (bytes_it == bytes.end()) ? 0 : bytes_it->second;

            line(out, name, it->first, calls_total,
                 _times, times_total, end_global-start_global,
                 _bytes);
        }
    }
    else {
        for (PnetcdfIOMapR::iterator it=calls_reverse.begin(),
                end=calls_reverse.end(); it!=end; ++it) {
        }
    }

    out << endl;

    return out.str();
}


string PnetcdfTiming::get_stats_aggregate()
{
    ostringstream out;
    //uint64_t times_total=0;
    uint64_t times_read=0;
    //uint64_t times_read_agg=0;
    uint64_t times_write=0;
    //uint64_t times_write_agg=0;
    //uint64_t bytes_total=0;
    uint64_t bytes_read=0;
    //uint64_t bytes_read_agg=0;
    uint64_t bytes_write=0;
    //uint64_t bytes_write_agg=0;
    double bw_read=0;
    double bw_read_agg=0;
    double bw_write=0;
    double bw_write_agg=0;
    double times_total=0;
    double times_total_agg=0;
#if   SIZEOF_UINT64_T == SIZEOF_UNSIGNED_INT
    MPI_Datatype type = MPI_UNSIGNED;
#elif SIZEOF_UINT64_T == SIZEOF_UNSIGNED_LONG
    MPI_Datatype type = MPI_UNSIGNED_LONG;
#elif SIZEOF_UINT64_T == SIZEOF_UNSIGNED_LONG_LONG
    // NOTE: There is no MPI_UNSIGNED_LONG_LONG_INT type... :-(
    MPI_Datatype type = MPI_LONG_LONG_INT;
#else
#   error "Can't determine MPI_Datatype for uint64_t"
#endif

    // calculate total times
    for (PnetcdfIOMap::const_iterator it=times.begin(), end=times.end();
            it != end; ++it) {
        string name = it->first;
        uint64_t data = it->second;
        if        (name.find("get_var") != string::npos) {
            times_read += data;
        } else if (name.find("put_var") != string::npos) {
            times_write += data;
        }
    }
    times_read += time_wait_read;
    //pagoda::println_sync("time_wait_read " + pagoda::to_string(time_wait_read));
    times_write += time_wait_write;
    //pagoda::println_sync("time_wait_write " + pagoda::to_string(time_wait_write));

    // calculate total bytes
    for (PnetcdfIOMap::const_iterator it=bytes.begin(), end=bytes.end();
            it != end; ++it) {
        string name = it->first;
        uint64_t data = it->second;
        if        (name.find("get_var") != string::npos) {
            bytes_read += data;
        } else if (name.find("put_var") != string::npos) {
            bytes_write += data;
        }
    }

    bw_read = static_cast<double>(bytes_read)/static_cast<double>(times_read);
    bw_write = static_cast<double>(bytes_write)/static_cast<double>(times_write);
    times_total = (times_read + times_write) * g_time_multiplier;
    type = MPI_DOUBLE;
    MPI_Allreduce(&bw_read,&bw_read_agg,1,type,MPI_SUM,pagoda::COMM_WORLD);
    MPI_Allreduce(&bw_write,&bw_write_agg,1,type,MPI_SUM,pagoda::COMM_WORLD);
    MPI_Allreduce(&times_total,&times_total_agg,1,type,MPI_SUM,pagoda::COMM_WORLD);

    double io_read  = g_io_multiplier * bw_read_agg;
    double io_write = g_io_multiplier * bw_write_agg;

#if 0
    pagoda::print_sync("Before allreduce bytes_read:      %lu\n", bytes_read);
    MPI_Allreduce(&bytes_read,&bytes_read_agg,1,type,MPI_SUM,pagoda::COMM_WORLD);
    pagoda::print_sync(" after allreduce bytes_read_agg:  %lu\n", bytes_read_agg);
    if (bytes_read_agg < bytes_read) {
        pagoda::print_sync("WARNING: bytes_read_agg<bytes_read\n");
    } else {
        pagoda::print_sync("bytes_read_agg OKAY\n");
    }

    pagoda::print_sync("Before allreduce bytes_write:     %lu\n", bytes_write);
    MPI_Allreduce(&bytes_write,&bytes_write_agg,1,type,MPI_SUM,pagoda::COMM_WORLD);
    pagoda::print_sync(" after allreduce bytes_write_agg: %lu\n", bytes_write_agg);
    if (bytes_write_agg < bytes_write) {
        pagoda::print_sync("WARNING: bytes_write_agg<bytes_write\n");
    } else {
        pagoda::print_sync("bytes_write_agg OKAY\n");
    }

    pagoda::print_sync("Before allreduce times_read:      %lu\n", times_read);
    //MPI_Allreduce(&times_read,&times_read_agg,1,type,MPI_SUM,pagoda::COMM_WORLD);
    MPI_Allreduce(&times_read,&times_read_agg,1,type,MPI_MAX,pagoda::COMM_WORLD);
    pagoda::print_sync(" after allreduce times_read_agg:  %lu\n", times_read_agg);
    if (times_read_agg < times_read) {
        pagoda::print_sync("WARNING: times_read_agg<times_read\n");
    } else {
        pagoda::print_sync("times_read_agg OKAY\n");
    }

    pagoda::print_sync("Before allreduce times_write:     %lu\n", times_write);
    //MPI_Allreduce(&times_write,&times_write_agg,1,type,MPI_SUM,pagoda::COMM_WORLD);
    MPI_Allreduce(&times_write,&times_write_agg,1,type,MPI_MAX,pagoda::COMM_WORLD);
    pagoda::print_sync(" after allreduce times_write_agg: %lu\n", times_write_agg);
    if (times_write_agg < times_write) {
        pagoda::print_sync("WARNING: times_write_agg<times_write\n");
    } else {
        pagoda::print_sync("times_write_agg OKAY\n");
    }

    bytes_total = bytes_read_agg + bytes_write_agg;
    times_total = times_read_agg + times_write_agg;
    double io_read  = g_io_multiplier * bytes_read_agg  / times_read_agg;
    double io_write = g_io_multiplier * bytes_write_agg / times_write_agg;
    double io_total = g_io_multiplier * bytes_total     / times_total;
#endif

    out << endl
        << "Aggregate Bandwidth (Total Bytes/Total Times)"
        << endl
        << "Read "
#if 0
        << endl
        << bytes_read_agg  << " " << UNIT_SIZE << " / "
        << times_read_agg  << " " << UNIT_TIME << " = "
#endif
        <<    io_read      << " gigabytes/sec"
        << endl
        << "Write "
#if 0
        << endl
        << bytes_write_agg << " " << UNIT_SIZE << " / "
        << times_write_agg << " " << UNIT_TIME << " = "
#endif
        <<    io_write     << " gigabytes/sec"
        << endl
#if 0
        << "Total"
        << endl
        << bytes_total << " " << UNIT_SIZE << " / "
        << times_total << " " << UNIT_TIME << " = "
        <<    io_total << " gigabytes/sec"
        << endl
#endif
        << "Total Time Spent in IO" << endl
        << times_total_agg << " seconds" << " / "
        << pagoda::npe << " PEs = " << times_total_agg / pagoda::npe << endl
        << endl;

    return out.str();
}
