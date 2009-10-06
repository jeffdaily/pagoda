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
#include <iomanip>
#include <iostream>
#include <sstream>

#include <mpi.h>
#include <pnetcdf.h>

#include "NetcdfError.H"
#include "PnetcdfTiming.H"

using std::cerr;
using std::endl;
using std::fixed;
using std::left;
using std::ostringstream;
using std::right;
using std::setprecision;
using std::setw;


int64_t PnetcdfTiming::start_global;
int64_t PnetcdfTiming::end_global;
PnetcdfIOMap PnetcdfTiming::times;
PnetcdfIOMap PnetcdfTiming::bytes;
PnetcdfIOMap PnetcdfTiming::calls;


static int g_name_width = 14;
static int g_calls_width = 6;
static int g_times_width = 7;
static int g_percent_width = 7;
static int g_bytes_width = 7;
static int g_log10_adjustment = 3;


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


static inline void footer(ostringstream &out, int64_t bytes, int64_t times)
{
    out << endl
        << "Aggregate Bandwidth (Total Bytes/Total Times)"
        << endl
        << bytes << " / " << times << " = " << (1.0*bytes/times)
        << endl
        << endl;
}


static inline void line(ostringstream &out, string name,
        int calls, int calls_total,
        int64_t times, int64_t times_total, int64_t times_global,
        int64_t bytes)
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
        int64_t &total, int &width, int &name_width)
{
    int64_t max = 0;

    total = 0;
    width = 0;
    name_width = 0;
    for (PnetcdfIOMap::const_iterator it=iomap.begin(),end=iomap.end();
            it != end; ++it) {
        string name = it->first;
        int64_t data = it->second;

        iomap_reverse.insert(make_pair(data,name));
        if (name.size() > name_width) {
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
    width = (int)(ceil(log10(max))) + g_log10_adjustment;
}


static inline void calc_info(const PnetcdfIOMap &iomap,
        int64_t &total, int &width)
{
    int64_t max = 0;

    total = 0;
    width = 0;
    for (PnetcdfIOMap::const_iterator it=iomap.begin(), end=iomap.end();
            it != end; ++it) {
        int64_t data = it->second;
        if (data > max) {
            max = data;
        }
        total += data;
    }
    width = (int)(ceil(log10(max))) + g_log10_adjustment;
}


PnetcdfTiming::PnetcdfTiming(const string &name)
    :   name(name)
    ,   start(0)
{
    ++calls[name];
    start = get_time();
}


PnetcdfTiming::PnetcdfTiming(
        const string &name,
        int ndim,
        const MPI_Offset *count,
        nc_type type)
    :   name(name)
    ,   start(0)
{
    MPI_Offset size = 1;
    for (int i=0; i<ndim; ++i) {
        size *= count[i];
    }
    switch (type) {
        case NC_BYTE:   size *= 1; break;
        case NC_CHAR:   size *= 1; break;
        case NC_SHORT:  size *= 2; break;
        case NC_INT:    size *= 4; break;
        case NC_FLOAT:  size *= 4; break;
        case NC_DOUBLE: size *= 8; break;
    }
    ++calls[name];
    bytes[name] += size;
    start = get_time();
}


PnetcdfTiming::~PnetcdfTiming()
{
    int64_t end = get_time();
    int64_t diff = end-start;
    if (diff > 0) {
        int64_t new_sum = times[name] + diff;
        if (new_sum > 0) {
            times[name] = new_sum;
        } else {
            cerr << "WARNING: times integer overflow: " << name << endl;
        }
    } else if (diff < 0) {
        cerr << "WARNING: diff < 0: " << name << endl;
        cerr << "\t" << end << "-" << start << "=" << diff << endl;
    }
}


int64_t PnetcdfTiming::get_time()
{
    int64_t value;

#if defined(HAVE_CLOCK_GETTIME)
    struct timespec tp;
    int stat = clock_gettime(CLOCK_MONOTONIC, &tp);
    value = tp.tv_sec * 1000000000 + tp.tv_nsec;
#elif defined(HAVE_GETTIMEOFDAY)
    struct timeval tv;
    gettimeofday(&tv, NULL);
    value = tv.tv_sec * 1000 + tv.tv_usec;
#elif defined(HAVE_CLOCK)
    value = (int64_t)(1.0*clock()/CLOCKS_PER_SEC);
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
    int64_t calls_total = 0;
    int64_t times_total = 0;
    int64_t bytes_total = 0;

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
            int64_t _times = (times_it == times.end()) ? 0 : times_it->second;
            int64_t _bytes = (bytes_it == bytes.end()) ? 0 : bytes_it->second;

            line(out, name, it->first, calls_total,
                    _times, times_total, end_global-start_global,
                    _bytes);
        }
    } else {
        for (PnetcdfIOMapR::iterator it=calls_reverse.begin(),
                end=calls_reverse.end(); it!=end; ++it) {
        }
    }

    out << endl;
    //footer(out, bytes_total, times_total);

    return out.str();
}


string PnetcdfTiming::get_stats_aggregate()
{
    ostringstream out;
    int64_t times_total;
    int64_t times_read;
    int64_t times_read_agg;
    int64_t times_write;
    int64_t times_write_agg;
    int64_t bytes_total;
    int64_t bytes_read;
    int64_t bytes_read_agg;
    int64_t bytes_write;
    int64_t bytes_write_agg;
#if   SIZEOF_INT64_T == SIZEOF_INT
    MPI_Datatype type = MPI_INT;
#elif SIZEOF_INT64_T == SIZEOF_LONG
    MPI_Datatype type = MPI_LONG;
#elif SIZEOF_INT64_T == SIZEOF_LONG_LONG
    MPI_Datatype type = MPI_LONG_LONG_INT;
#else
#   error Can't determine MPI_Datatype for int64_t
#endif

    // calculate total times
    for (PnetcdfIOMap::const_iterator it=times.begin(), end=times.end();
            it != end; ++it) {
        string name = it->first;
        int64_t data = it->second;
        if (name.find("get_var") != string::npos) {
            times_read += data;
        } else if (name.find("put_var") != string::npos) {
            times_write += data;
        }
    }
    // calculate total bytes
    for (PnetcdfIOMap::const_iterator it=bytes.begin(), end=bytes.end();
            it != end; ++it) {
        string name = it->first;
        int64_t data = it->second;
        if (name.find("get_var") != string::npos) {
            bytes_read += data;
        } else if (name.find("put_var") != string::npos) {
            bytes_write += data;
        }
    }
    
    MPI_Allreduce(&times_read, &times_read_agg, 1, type, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&times_write, &times_write_agg, 1, type, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&bytes_read, &bytes_read_agg, 1, type, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&bytes_write, &bytes_write_agg, 1, type, MPI_SUM, MPI_COMM_WORLD);

    bytes_total = bytes_read_agg + bytes_write_agg;
    times_total = times_read_agg + times_write_agg;

    out << endl
        << "Aggregate Bandwidth (Total Bytes/Total Times)" << endl
        << "Read" << endl
        << bytes_read_agg << " / " << times_read_agg << " = " << (1.0*bytes_read_agg/times_read_agg) << endl
        << "Write" << endl
        << bytes_write_agg << " / " << times_write_agg << " = " << (1.0*bytes_write_agg/times_write_agg) << endl
        << "Total" << endl
        << bytes_total << " / " << times_total << " = " << (1.0*bytes_total/times_total) << endl
        << endl;

    return out.str();
}


void ncmpi::create(MPI_Comm comm, const char *path, int cmode, MPI_Info info, int *ncidp)
{
    PNETCDF_TIMING1("ncmpi_create");
    ERRNO_CHECK(ncmpi_create(comm, path, cmode, info, ncidp));
}


void ncmpi::open(MPI_Comm comm, const char *path, int omode, MPI_Info info, int *ncidp)
{
    PNETCDF_TIMING1("ncmpi_open");
    ERRNO_CHECK(ncmpi_open(comm, path, omode, info, ncidp));
}


void ncmpi::enddef(int ncid)
{
    PNETCDF_TIMING1("ncmpi_enddef");
    ERRNO_CHECK(ncmpi_enddef(ncid));
}


void ncmpi::redef(int ncid)
{
    PNETCDF_TIMING1("ncmpi_redef");
    ERRNO_CHECK(ncmpi_redef(ncid));
}


void ncmpi::get_file_info(int ncid, MPI_Info *info_used)
{
    PNETCDF_TIMING1("ncmpi_get_file_info");
    ERRNO_CHECK(ncmpi_get_file_info(ncid, info_used));
}


void ncmpi::sync(int ncid)
{
    PNETCDF_TIMING1("ncmpi_sync");
    ERRNO_CHECK(ncmpi_sync(ncid));
}


void ncmpi::abort(int ncid)
{
    PNETCDF_TIMING1("ncmpi_abort");
    ERRNO_CHECK(ncmpi_abort(ncid));
}


void ncmpi::begin_indep_data(int ncid)
{
    PNETCDF_TIMING1("ncmpi_begin_indep_data");
    ERRNO_CHECK(ncmpi_begin_indep_data(ncid));
}


void ncmpi::end_indep_data(int ncid)
{
    PNETCDF_TIMING1("ncmpi_end_indep_data");
    ERRNO_CHECK(ncmpi_end_indep_data(ncid));
}


void ncmpi::close(int ncid)
{
    PNETCDF_TIMING1("ncmpi_close");
    ERRNO_CHECK(ncmpi_close(ncid));
}


void ncmpi::inq(int ncid, int *ndimsp, int *nvarsp, int *ngattsp, int *unlimdimidp)
{
    PNETCDF_TIMING1("ncmpi_inq");
    ERRNO_CHECK(ncmpi_inq(ncid, ndimsp, nvarsp, ngattsp, unlimdimidp));
}


void ncmpi::inq_ndims(int ncid, int *ndimsp)
{
    PNETCDF_TIMING1("ncmpi_inq_ndims");
    ERRNO_CHECK(ncmpi_inq_ndims(ncid, ndimsp));
}


void ncmpi::inq_nvars(int ncid, int *nvarsp)
{
    PNETCDF_TIMING1("ncmpi_inq_nvars");
    ERRNO_CHECK(ncmpi_inq_nvars(ncid, nvarsp));
}


void ncmpi::inq_natts(int ncid, int *ngattsp)
{
    PNETCDF_TIMING1("ncmpi_inq_natts");
    ERRNO_CHECK(ncmpi_inq_natts(ncid, ngattsp));
}


void ncmpi::inq_unlimdim(int ncid, int *unlimdimidp)
{
    PNETCDF_TIMING1("ncmpi_inq_unlimdim");
    ERRNO_CHECK(ncmpi_inq_unlimdim(ncid, unlimdimidp));
}


void ncmpi::inq_dimid(int ncid, const char *name, int *idp)
{
    PNETCDF_TIMING1("ncmpi_inq_dimid");
    ERRNO_CHECK(ncmpi_inq_dimid(ncid, name, idp));
}


void ncmpi::inq_dim(int ncid, int dimid, char *name, MPI_Offset *lenp)
{
    PNETCDF_TIMING1("ncmpi_inq_dim");
    ERRNO_CHECK(ncmpi_inq_dim(ncid, dimid, name, lenp));
}


void ncmpi::inq_dimname(int ncid, int dimid, char *name)
{
    PNETCDF_TIMING1("ncmpi_inq_dimname");
    ERRNO_CHECK(ncmpi_inq_dimname(ncid, dimid, name));
}


void ncmpi::inq_dimlen(int ncid, int dimid, MPI_Offset *lenp)
{
    PNETCDF_TIMING1("ncmpi_inq_dimlen");
    ERRNO_CHECK(ncmpi_inq_dimlen(ncid, dimid, lenp));
}


void ncmpi::inq_var(int ncid, int varid, char *name, nc_type *xtypep, int *ndimsp, int *dimidsp, int *nattsp)
{
    PNETCDF_TIMING1("ncmpi_inq_var");
    ERRNO_CHECK(ncmpi_inq_var(ncid, varid, name, xtypep, ndimsp, dimidsp, nattsp));
}


void ncmpi::inq_varid(int ncid, const char *name, int *varidp)
{
    PNETCDF_TIMING1("ncmpi_inq_varid");
    ERRNO_CHECK(ncmpi_inq_varid(ncid, name, varidp));
}


void ncmpi::inq_varname(int ncid, int varid, char *name)
{
    PNETCDF_TIMING1("ncmpi_inq_varname");
    ERRNO_CHECK(ncmpi_inq_varname(ncid, varid, name));
}


void ncmpi::inq_vartype(int ncid, int varid, nc_type *xtypep)
{
    PNETCDF_TIMING1("ncmpi_inq_vartype");
    ERRNO_CHECK(ncmpi_inq_vartype(ncid, varid, xtypep));
}


void ncmpi::inq_varndims(int ncid, int varid, int *ndimsp)
{
    PNETCDF_TIMING1("ncmpi_inq_varndims");
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, ndimsp));
}


void ncmpi::inq_vardimid(int ncid, int varid, int *dimidsp)
{
    PNETCDF_TIMING1("ncmpi_inq_vardimid");
    ERRNO_CHECK(ncmpi_inq_vardimid(ncid, varid, dimidsp));
}


void ncmpi::inq_varnatts(int ncid, int varid, int *nattsp)
{
    PNETCDF_TIMING1("ncmpi_inq_varnatts");
    ERRNO_CHECK(ncmpi_inq_varnatts(ncid, varid, nattsp));
}


void ncmpi::def_dim(int ncid, const char *name, MPI_Offset len, int *idp)
{
    PNETCDF_TIMING1("ncmpi_def_dim");
    ERRNO_CHECK(ncmpi_def_dim(ncid, name, len, idp));
}


void ncmpi::def_var(int ncid, const char *name, nc_type xtype, int ndims, const int *dimidsp, int *varidp)
{
    PNETCDF_TIMING1("ncmpi_def_var");
    ERRNO_CHECK(ncmpi_def_var(ncid, name, xtype, ndims, dimidsp, varidp));
}


void ncmpi::rename_dim(int ncid, int dimid, const char *name)
{
    PNETCDF_TIMING1("ncmpi_rename_dim");
    ERRNO_CHECK(ncmpi_rename_dim(ncid, dimid, name));
}


void ncmpi::rename_var(int ncid, int varid, const char *name)
{
    PNETCDF_TIMING1("ncmpi_rename_var");
    ERRNO_CHECK(ncmpi_rename_var(ncid, varid, name));
}


void ncmpi::inq_att(int ncid, int varid, const char *name, nc_type *xtypep, MPI_Offset *lenp)
{
    PNETCDF_TIMING1("ncmpi_inq_att");
    ERRNO_CHECK(ncmpi_inq_att(ncid, varid, name, xtypep, lenp));
}


void ncmpi::inq_attid(int ncid, int varid, const char *name, int *idp)
{
    PNETCDF_TIMING1("ncmpi_inq_attid");
    ERRNO_CHECK(ncmpi_inq_attid(ncid, varid, name, idp));
}


void ncmpi::inq_atttype(int ncid, int varid, const char *name, nc_type *xtypep)
{
    PNETCDF_TIMING1("ncmpi_inq_atttype");
    ERRNO_CHECK(ncmpi_inq_atttype(ncid, varid, name, xtypep));
}


void ncmpi::inq_attlen(int ncid, int varid, const char *name, MPI_Offset *lenp)
{
    PNETCDF_TIMING1("ncmpi_inq_attlen");
    ERRNO_CHECK(ncmpi_inq_attlen(ncid, varid, name, lenp));
}


void ncmpi::inq_attname(int ncid, int varid, int attnum, char *name)
{
    PNETCDF_TIMING1("ncmpi_inq_attname");
    ERRNO_CHECK(ncmpi_inq_attname(ncid, varid, attnum, name));
}


void ncmpi::copy_att(int ncid, int varid, const char *name, int ncid_out, int varid_out)
{
    PNETCDF_TIMING1("ncmpi_copy_att");
    ERRNO_CHECK(ncmpi_copy_att(ncid, varid, name, ncid_out, varid_out));
}


void ncmpi::rename_att(int ncid, int varid, const char *name, const char *newname)
{
    PNETCDF_TIMING1("ncmpi_rename_att");
    ERRNO_CHECK(ncmpi_rename_att(ncid, varid, name, newname));
}


void ncmpi::del_att(int ncid, int varid, const char *name)
{
    PNETCDF_TIMING1("ncmpi_del_att");
    ERRNO_CHECK(ncmpi_del_att(ncid, varid, name));
}


void ncmpi::put_att(int ncid, int varid, const char *name, MPI_Offset len, const char *op)
{
    PNETCDF_TIMING4("ncmpi_put_att_text", 1, &len, NC_CHAR);
    ERRNO_CHECK(ncmpi_put_att_text(ncid, varid, name, len, op));
}


// For completeness. xtype is ignored.
void ncmpi::put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const char *op)
{
    PNETCDF_TIMING4("ncmpi_put_att_text", 1, &len, NC_CHAR);
    ERRNO_CHECK(ncmpi_put_att_text(ncid, varid, name, len, op));
}


void ncmpi::put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const unsigned char *op)
{
    PNETCDF_TIMING4("ncmpi_put_att_uchar", 1, &len, NC_CHAR);
    ERRNO_CHECK(ncmpi_put_att_uchar(ncid, varid, name, xtype, len, op));
}


void ncmpi::put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const signed char *op)
{
    PNETCDF_TIMING4("ncmpi_put_att_schar", 1, &len, NC_BYTE);
    ERRNO_CHECK(ncmpi_put_att_schar(ncid, varid, name, xtype, len, op));
}


void ncmpi::put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const short *op)
{
    PNETCDF_TIMING4("ncmpi_put_att_short", 1, &len, NC_SHORT);
    ERRNO_CHECK(ncmpi_put_att_short(ncid, varid, name, xtype, len, op));
}


void ncmpi::put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const int *op)
{
    PNETCDF_TIMING4("ncmpi_put_att_int", 1, &len, NC_INT);
    ERRNO_CHECK(ncmpi_put_att_int(ncid, varid, name, xtype, len, op));
}


void ncmpi::put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const long *op)
{
    PNETCDF_TIMING4("ncmpi_put_att_long", 1, &len, NC_INT);
    ERRNO_CHECK(ncmpi_put_att_long(ncid, varid, name, xtype, len, op));
}


void ncmpi::put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const float *op)
{
    PNETCDF_TIMING4("ncmpi_put_att_float", 1, &len, NC_FLOAT);
    ERRNO_CHECK(ncmpi_put_att_float(ncid, varid, name, xtype, len, op));
}


void ncmpi::put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const double *op)
{
    PNETCDF_TIMING4("ncmpi_put_att_double", 1, &len, NC_DOUBLE);
    ERRNO_CHECK(ncmpi_put_att_double(ncid, varid, name, xtype, len, op));
}


void ncmpi::get_att(int ncid, int varid, const char *name, char *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    MPI_Offset len;
    ERRNO_CHECK(ncmpi_inq_attlen(ncid, varid, name, &len));
#endif
    PNETCDF_TIMING4("ncmpi_get_att_text", 1, &len, NC_CHAR);
    ERRNO_CHECK(ncmpi_get_att_text(ncid, varid, name, ip));
}


void ncmpi::get_att(int ncid, int varid, const char *name, unsigned char *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    MPI_Offset len;
    ERRNO_CHECK(ncmpi_inq_attlen(ncid, varid, name, &len));
#endif
    PNETCDF_TIMING4("ncmpi_get_att_uchar", 1, &len, NC_CHAR);
    ERRNO_CHECK(ncmpi_get_att_uchar(ncid, varid, name, ip));
}


void ncmpi::get_att(int ncid, int varid, const char *name, signed char *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    MPI_Offset len;
    ERRNO_CHECK(ncmpi_inq_attlen(ncid, varid, name, &len));
#endif
    PNETCDF_TIMING4("ncmpi_get_att_schar", 1, &len, NC_BYTE);
    ERRNO_CHECK(ncmpi_get_att_schar(ncid, varid, name, ip));
}


void ncmpi::get_att(int ncid, int varid, const char *name, short *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    MPI_Offset len;
    ERRNO_CHECK(ncmpi_inq_attlen(ncid, varid, name, &len));
#endif
    PNETCDF_TIMING4("ncmpi_get_att_short", 1, &len, NC_SHORT);
    ERRNO_CHECK(ncmpi_get_att_short(ncid, varid, name, ip));
}


void ncmpi::get_att(int ncid, int varid, const char *name, int *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    MPI_Offset len;
    ERRNO_CHECK(ncmpi_inq_attlen(ncid, varid, name, &len));
#endif
    PNETCDF_TIMING4("ncmpi_get_att_int", 1, &len, NC_INT);
    ERRNO_CHECK(ncmpi_get_att_int(ncid, varid, name, ip));
}


void ncmpi::get_att(int ncid, int varid, const char *name, long *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    MPI_Offset len;
    ERRNO_CHECK(ncmpi_inq_attlen(ncid, varid, name, &len));
#endif
    PNETCDF_TIMING4("ncmpi_get_att_long", 1, &len, NC_INT);
    ERRNO_CHECK(ncmpi_get_att_long(ncid, varid, name, ip));
}


void ncmpi::get_att(int ncid, int varid, const char *name, float *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    MPI_Offset len;
    ERRNO_CHECK(ncmpi_inq_attlen(ncid, varid, name, &len));
#endif
    PNETCDF_TIMING4("ncmpi_get_att_float", 1, &len, NC_FLOAT);
    ERRNO_CHECK(ncmpi_get_att_float(ncid, varid, name, ip));
}


void ncmpi::get_att(int ncid, int varid, const char *name, double *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    MPI_Offset len;
    ERRNO_CHECK(ncmpi_inq_attlen(ncid, varid, name, &len));
#endif
    PNETCDF_TIMING4("ncmpi_get_att_double", 1, &len, NC_DOUBLE);
    ERRNO_CHECK(ncmpi_get_att_double(ncid, varid, name, ip));
}


void ncmpi::put_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const short *op)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_put_vara_short_all", ndim, count, NC_SHORT);
    ERRNO_CHECK(ncmpi_put_vara_short_all(ncid, varid, start, count, op));
}


void ncmpi::put_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const int *op)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_put_vara_int_all", ndim, count, NC_INT);
    ERRNO_CHECK(ncmpi_put_vara_int_all(ncid, varid, start, count, op));
}


void ncmpi::put_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const float *op)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_put_vara_float_all", ndim, count, NC_FLOAT);
    ERRNO_CHECK(ncmpi_put_vara_float_all(ncid, varid, start, count, op));
}


void ncmpi::put_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const double *op)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_put_vara_double_all", ndim, count, NC_DOUBLE);
    ERRNO_CHECK(ncmpi_put_vara_double_all(ncid, varid, start, count, op));
}


void ncmpi::get_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], short *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_get_vara_short_all", ndim, count, NC_SHORT);
    ERRNO_CHECK(ncmpi_get_vara_short_all(ncid, varid, start, count, ip));
}


void ncmpi::get_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], int *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_get_vara_int_all", ndim, count, NC_INT);
    ERRNO_CHECK(ncmpi_get_vara_int_all(ncid, varid, start, count, ip));
}


void ncmpi::get_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], float *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_get_vara_float_all", ndim, count, NC_FLOAT);
    ERRNO_CHECK(ncmpi_get_vara_float_all(ncid, varid, start, count, ip));
}


void ncmpi::get_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], double *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_get_vara_double_all", ndim, count, NC_DOUBLE);
    ERRNO_CHECK(ncmpi_get_vara_double_all(ncid, varid, start, count, ip));
}


void ncmpi::put_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const short *op)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_put_vara_short", ndim, count, NC_SHORT);
    ERRNO_CHECK(ncmpi_put_vara_short(ncid, varid, start, count, op));
}


void ncmpi::put_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const int *op)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_put_vara_int", ndim, count, NC_INT);
    ERRNO_CHECK(ncmpi_put_vara_int(ncid, varid, start, count, op));
}


void ncmpi::put_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const float *op)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_put_vara_float", ndim, count, NC_FLOAT);
    ERRNO_CHECK(ncmpi_put_vara_float(ncid, varid, start, count, op));
}


void ncmpi::put_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const double *op)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_put_vara_double", ndim, count, NC_DOUBLE);
    ERRNO_CHECK(ncmpi_put_vara_double(ncid, varid, start, count, op));
}


void ncmpi::get_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], short *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_get_vara_short", ndim, count, NC_SHORT);
    ERRNO_CHECK(ncmpi_get_vara_short(ncid, varid, start, count, ip));
}


void ncmpi::get_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], int *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_get_vara_int", ndim, count, NC_INT);
    ERRNO_CHECK(ncmpi_get_vara_int(ncid, varid, start, count, ip));
}


void ncmpi::get_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], float *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_get_vara_float", ndim, count, NC_FLOAT);
    ERRNO_CHECK(ncmpi_get_vara_float(ncid, varid, start, count, ip));
}


void ncmpi::get_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], double *ip)
{
#ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#endif
    PNETCDF_TIMING4("ncmpi_get_vara_double", ndim, count, NC_DOUBLE);
    ERRNO_CHECK(ncmpi_get_vara_double(ncid, varid, start, count, ip));
}
