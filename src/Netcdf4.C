#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <netcdf.h>

#include <cassert>
#include <string>
#include <vector>

#include "Debug.H"
#include "Netcdf4Error.H"
#include "Netcdf4.H"
#include "Netcdf4Timing.H"

using std::string;
using std::vector;


int nc::create(const string &path, int cmode)
{
    int ncid;
    NETCDF4_TIMING1("nc_create");
    ERRNO_CHECK(nc_create(path.c_str(), cmode, &ncid));
    return ncid;
}


int nc::create(const string &path, int cmode, size_t initialsz, size_t &bufrsizehint)
{
    int ncid;
    NETCDF4_TIMING1("nc__create");
    ERRNO_CHECK(nc__create(path.c_str(), cmode, initialsz, &bufrsizehint, &ncid));
    return ncid;
}


int nc::create(const string &path, int cmode, MPI_Comm comm, MPI_Info info)
{
    int ncid;
    NETCDF4_TIMING1("nc_create_par");
    ERRNO_CHECK(nc_create_par(path.c_str(), cmode, comm, info, &ncid));
    return ncid;
}


int nc::open(const string &path, int omode)
{
    int ncid;
    NETCDF4_TIMING1("nc_open");
    ERRNO_CHECK(nc_open(path.c_str(), omode, &ncid));
    return ncid;
}


int nc::open(const string &path, int omode, size_t &bufrsizehint)
{
    int ncid;
    NETCDF4_TIMING1("nc__open");
    ERRNO_CHECK(nc__open(path.c_str(), omode, &bufrsizehint, &ncid));
    return ncid;
}


int nc::open(const string &path, int omode, MPI_Comm comm, MPI_Info info)
{
    int ncid;
    NETCDF4_TIMING1("nc_open_par");
    ERRNO_CHECK(nc_open_par(path.c_str(), omode, comm, info, &ncid));
    return ncid;
}


void nc::enddef(int ncid)
{
    NETCDF4_TIMING1("nc_enddef");
    ERRNO_CHECK(nc_enddef(ncid));
}


void nc::enddef(int ncid,
        size_t h_minfree, size_t v_align, size_t v_minfree, size_t r_align)
{
    NETCDF4_TIMING1("nc__enddef");
    ERRNO_CHECK(nc__enddef(ncid, h_minfree, v_align, v_minfree, r_align));
}


void nc::redef(int ncid)
{
    NETCDF4_TIMING1("nc_redef");
    ERRNO_CHECK(nc_redef(ncid));
}


void nc::sync(int ncid)
{
    NETCDF4_TIMING1("nc_sync");
    ERRNO_CHECK(nc_sync(ncid));
}


void nc::abort(int ncid)
{
    NETCDF4_TIMING1("nc_abort");
    ERRNO_CHECK(nc_abort(ncid));
}


void nc::close(int ncid)
{
    NETCDF4_TIMING1("nc_close");
    ERRNO_CHECK(nc_close(ncid));
}


int nc::inq_format(int ncid)
{
    int format;
    NETCDF4_TIMING1("nc_inq_format");
    ERRNO_CHECK(nc_inq_format(ncid, &format));
    return format;
}

void nc::inq(int ncid, int &ndims, int &nvars, int &ngatts, int &unlimdimid)
{
    NETCDF4_TIMING1("nc_inq");
    ERRNO_CHECK(nc_inq(ncid, &ndims, &nvars, &ngatts, &unlimdimid));
}


int nc::inq_ndims(int ncid)
{
    int ndims;
    NETCDF4_TIMING1("nc_inq_ndims");
    ERRNO_CHECK(nc_inq_ndims(ncid, &ndims));
    return ndims;
}


int nc::inq_nvars(int ncid)
{
    int nvars;
    NETCDF4_TIMING1("nc_inq_nvars");
    ERRNO_CHECK(nc_inq_nvars(ncid, &nvars));
    return nvars;
}


int nc::inq_natts(int ncid)
{
    int natts;
    NETCDF4_TIMING1("nc_inq_natts");
    ERRNO_CHECK(nc_inq_natts(ncid, &natts));
    return natts;
}


int nc::inq_unlimdim(int ncid)
{
    int dim;
    NETCDF4_TIMING1("nc_inq_unlimdim");
    ERRNO_CHECK(nc_inq_unlimdim(ncid, &dim));
    return dim;
}


vector<int> nc::inq_unlimdims(int ncid)
{
    vector<int> dims;
    int nunlimdims;
    NETCDF4_TIMING1("nc_inq_unlimdims");
    ERRNO_CHECK(nc_inq_unlimdims(ncid, &nunlimdims, NULL));
    dims.resize(nunlimdims);
    ERRNO_CHECK(nc_inq_unlimdims(ncid, &nunlimdims, &dims[0]));
    return dims;
}


int nc::set_fill(int ncid, int fillmode)
{
    int last_fillmode;
    NETCDF4_TIMING1("nc_set_fill");
    ERRNO_CHECK(nc_set_fill(ncid, fillmode, &last_fillmode));
    return last_fillmode;
}


int nc::set_default_format(int format)
{
    int last_format;
    NETCDF4_TIMING1("nc_set_default_format");
    ERRNO_CHECK(nc_set_default_format(format, &last_format));
    return last_format;
}


void nc::set_chunk_cache(size_t  size, size_t  nelems, float  preemption)
{
    NETCDF4_TIMING1("nc_set_chunk_cache");
    ERRNO_CHECK(nc_set_chunk_cache(size, nelems, preemption));
}


void nc::get_chunk_cache(size_t &size, size_t &nelems, float &preemption)
{
    NETCDF4_TIMING1("nc_get_chunk_cache");
    ERRNO_CHECK(nc_get_chunk_cache(&size, &nelems, &preemption));
}


int nc::def_dim(int ncid, const string &name, size_t len)
{
    int id;
    NETCDF4_TIMING1("nc_def_dim");
    ERRNO_CHECK(nc_def_dim(ncid, name.c_str(), len, &id));
    return id;
}


void nc::rename_dim(int ncid, int dimid, const string &name)
{
    NETCDF4_TIMING1("nc_rename_dim");
    ERRNO_CHECK(nc_rename_dim(ncid, dimid, name.c_str()));
}


int nc::inq_dimid(int ncid, const string &name)
{
    int id;
    NETCDF4_TIMING1("nc_inq_dimid");
    ERRNO_CHECK(nc_inq_dimid(ncid, name.c_str(), &id));
    return id;
}


void nc::inq_dim(int ncid, int dimid, string &name, size_t &lenp)
{
    char cname[NC_MAX_NAME];
    NETCDF4_TIMING1("nc_inq_dim");
    ERRNO_CHECK(nc_inq_dim(ncid, dimid, cname, &lenp));
    name = cname;
}


string nc::inq_dimname(int ncid, int dimid)
{
    char name[NC_MAX_NAME];
    NETCDF4_TIMING1("nc_inq_dimname");
    ERRNO_CHECK(nc_inq_dimname(ncid, dimid, name));
    return string(name);
}


size_t nc::inq_dimlen(int ncid, int dimid)
{
    size_t len;
    NETCDF4_TIMING1("nc_inq_dimlen");
    ERRNO_CHECK(nc_inq_dimlen(ncid, dimid, &len));
    return len;
}


int nc::def_var(int ncid, const string &name, nc_type xtype,
        const vector<int> &dimids)
{
    int id;
    NETCDF4_TIMING1("nc_def_var");
    ERRNO_CHECK(nc_def_var(ncid, name.c_str(), xtype, dimids.size(),
                &dimids[0], &id));
    return id;
}


void nc::def_var_chunking(int ncid, int varid, int storage,
        const vector<size_t> &chunksizes)
{
    NETCDF4_TIMING1("nc_def_var_chunking");
    assert(nc::inq_varndims(ncid, varid)==chunksizes.size());
    ERRNO_CHECK(nc_def_var_chunking(ncid, varid, storage, &chunksizes[0]));
}


void nc::def_var_fill(int ncid, int varid, int no_fill, void *fill_value)
{
    NETCDF4_TIMING1("nc_def_var_fill");
    ERRNO_CHECK(nc_def_var_fill(ncid, varid, no_fill, fill_value));
}


void nc::def_var_deflate(int ncid, int varid, int shuffle, int deflate,
        int deflate_level)
{
    NETCDF4_TIMING1("nc_def_var_deflate");
    ERRNO_CHECK(nc_def_var_deflate(ncid, varid, shuffle, deflate,
                deflate_level));
}


void nc::rename_var(int ncid, int varid, const string &name)
{
    NETCDF4_TIMING1("nc_rename_var");
    ERRNO_CHECK(nc_rename_var(ncid, varid, name.c_str()));
}


void nc::inq_var(int ncid, int varid, string &name, nc_type &xtypep,
        vector<int> &dimids, int &natts)
{
    char cname[NC_MAX_NAME];
    int ndimsp;
    int dimidsp[NC_MAX_VAR_DIMS];
    NETCDF4_TIMING1("nc_inq_var");
    ERRNO_CHECK(nc_inq_var(ncid, varid, cname, &xtypep, &ndimsp,
                dimidsp, &natts));
    name = cname;
    dimids.assign(dimidsp, dimidsp+ndimsp);
}


void nc::inq_var_chunking(int ncid, int varid, int &storage,
        vector<size_t> &chunksizes)
{
    NETCDF4_TIMING1("nc_inq_var_chunking");
    chunksizes.resize(nc::inq_varndims(ncid, varid));
    ERRNO_CHECK(nc_inq_var_chunking(ncid, varid, &storage, &chunksizes[0]));
}


void nc::inq_var_fill(int ncid, int varid, int &no_fill, void *fill_value)
{
    NETCDF4_TIMING1("nc_inq_var_fill");
    ERRNO_CHECK(nc_inq_var_fill(ncid, varid, &no_fill, fill_value));
}


void nc::inq_var_deflate(int ncid, int varid, int &shuffle, int &deflate,
        int &deflate_level)
{
    NETCDF4_TIMING1("nc_inq_var_deflate");
    ERRNO_CHECK(nc_inq_var_deflate(ncid, varid, &shuffle, &deflate,
                &deflate_level));
}


int nc::inq_varid(int ncid, const string &name)
{
    int id;
    NETCDF4_TIMING1("nc_inq_varid");
    ERRNO_CHECK(nc_inq_varid(ncid, name.c_str(), &id));
    return id;
}


string nc::inq_varname(int ncid, int varid)
{
    char name[NC_MAX_NAME];
    NETCDF4_TIMING1("nc_inq_varname");
    ERRNO_CHECK(nc_inq_varname(ncid, varid, name));
    return string(name);
}


nc_type nc::inq_vartype(int ncid, int varid)
{
    nc_type xtype;
    NETCDF4_TIMING1("nc_inq_vartype");
    ERRNO_CHECK(nc_inq_vartype(ncid, varid, &xtype));
    return xtype;
}


int nc::inq_varndims(int ncid, int varid)
{
    int ndims;
    NETCDF4_TIMING1("nc_inq_varndims");
    ERRNO_CHECK(nc_inq_varndims(ncid, varid, &ndims));
    return ndims;
}


vector<int> nc::inq_vardimid(int ncid, int varid)
{
    vector<int> dimids;
    int dimidsp[NC_MAX_VAR_DIMS];
    NETCDF4_TIMING1("nc_inq_vardimid");
    ERRNO_CHECK(nc_inq_vardimid(ncid, varid, dimidsp));
    dimids.assign(dimidsp, dimidsp+nc::inq_varndims(ncid, varid));
    return dimids;
}


int nc::inq_varnatts(int ncid, int varid)
{
    int natts;
    NETCDF4_TIMING1("nc_inq_varnatts");
    ERRNO_CHECK(nc_inq_varnatts(ncid, varid, &natts));
    return natts;
}


void nc::set_var_chunk_cache(int ncid, int varid, size_t size, size_t nelems, float preemption)
{
    NETCDF4_TIMING1("nc_set_var_chunk_cache");
    ERRNO_CHECK(nc_set_var_chunk_cache(ncid, varid, size, nelems, preemption));
}


void nc::get_var_chunk_cache(int ncid, int varid, size_t &size, size_t &nelems, float &preemption)
{
    NETCDF4_TIMING1("nc_get_var_chunk_cache");
    ERRNO_CHECK(nc_get_var_chunk_cache(ncid, varid, &size, &nelems, &preemption));
}


void nc::var_par_access(int ncid, int varid, int access)
{
    NETCDF4_TIMING1("nc_var_par_access");
    ERRNO_CHECK(nc_var_par_access(ncid, varid, access));
}


void nc::copy_att(int ncid, int varid, const string &name,
        int ncid_out, int varid_out)
{
    NETCDF4_TIMING1("nc_copy_att");
    ERRNO_CHECK(nc_copy_att(ncid, varid, name.c_str(), ncid_out, varid_out));
}


void nc::del_att(int ncid, int varid, const string &name)
{
    NETCDF4_TIMING1("nc_del_att");
    ERRNO_CHECK(nc_del_att(ncid, varid, name.c_str()));
}


void nc::rename_att(int ncid, int varid, const string &name,
        const string &newname)
{
    NETCDF4_TIMING1("nc_rename_att");
    ERRNO_CHECK(nc_rename_att(ncid, varid, name.c_str(), newname.c_str()));
}


void nc::inq_att(int ncid, int varid, const string &name,
        nc_type &xtype, size_t &len)
{
    NETCDF4_TIMING1("nc_inq_att");
    ERRNO_CHECK(nc_inq_att(ncid, varid, name.c_str(), &xtype, &len));
}


int nc::inq_attid(int ncid, int varid, const string &name)
{
    int id;
    NETCDF4_TIMING1("nc_inq_attid");
    ERRNO_CHECK(nc_inq_attid(ncid, varid, name.c_str(), &id));
    return id;
}


nc_type nc::inq_atttype(int ncid, int varid, const string &name)
{
    nc_type xtype;
    NETCDF4_TIMING1("nc_inq_atttype");
    ERRNO_CHECK(nc_inq_atttype(ncid, varid, name.c_str(), &xtype));
    return xtype;
}


size_t nc::inq_attlen(int ncid, int varid, const string &name)
{
    size_t len;
    NETCDF4_TIMING1("nc_inq_attlen");
    ERRNO_CHECK(nc_inq_attlen(ncid, varid, name.c_str(), &len));
    return len;
}


string nc::inq_attname(int ncid, int varid, int attnum)
{
    char name[NC_MAX_NAME];
    NETCDF4_TIMING1("nc_inq_attname");
    ERRNO_CHECK(nc_inq_attname(ncid, varid, attnum, name));
    return string(name);
}


void nc::put_att(int ncid, int varid, const string &name,
        const string &values)
{
    size_t len = values.size();
    NETCDF4_TIMING3("nc_put_att_text", len, NC_CHAR);
    ERRNO_CHECK(nc_put_att_text(ncid, varid, name.c_str(), len,
                values.c_str()));
}


void nc::put_att(int ncid, int varid, const string &name,
        const vector<char> &values, nc_type xtype)
{
    size_t len = values.size();
    NETCDF4_TIMING3("nc_put_att_uchar", len, NC_CHAR);
    ERRNO_CHECK(nc_put_att_text(ncid, varid, name.c_str(), len,
                &values[0]));
}


void nc::put_att(int ncid, int varid, const string &name,
        const vector<unsigned char> &values, nc_type xtype)
{
    size_t len = values.size();
    NETCDF4_TIMING3("nc_put_att_uchar", len, NC_BYTE);
    ERRNO_CHECK(nc_put_att_uchar(ncid, varid, name.c_str(), xtype, len,
                &values[0]));
}


void nc::put_att(int ncid, int varid, const string &name,
        const vector<signed char> &values, nc_type xtype)
{
    size_t len = values.size();
    NETCDF4_TIMING3("nc_put_att_schar", len, NC_BYTE);
    ERRNO_CHECK(nc_put_att_schar(ncid, varid, name.c_str(), xtype, len,
                &values[0]));
}


void nc::put_att(int ncid, int varid, const string &name,
        const vector<short> &values, nc_type xtype)
{
    size_t len = values.size();
    NETCDF4_TIMING3("nc_put_att_short", len, NC_SHORT);
    ERRNO_CHECK(nc_put_att_short(ncid, varid, name.c_str(), xtype, len,
                &values[0]));
}


void nc::put_att(int ncid, int varid, const string &name,
        const vector<int> &values, nc_type xtype)
{
    size_t len = values.size();
    NETCDF4_TIMING3("nc_put_att_int", len, NC_INT);
    ERRNO_CHECK(nc_put_att_int(ncid, varid, name.c_str(), xtype, len,
                &values[0]));
}


void nc::put_att(int ncid, int varid, const string &name,
        const vector<long> &values, nc_type xtype)
{
    size_t len = values.size();
    NETCDF4_TIMING3("nc_put_att_long", len, NC_INT);
    ERRNO_CHECK(nc_put_att_long(ncid, varid, name.c_str(), xtype, len,
                &values[0]));
}


void nc::put_att(int ncid, int varid, const string &name,
        const vector<float> &values, nc_type xtype)
{
    size_t len = values.size();
    NETCDF4_TIMING3("nc_put_att_float", len, NC_FLOAT);
    ERRNO_CHECK(nc_put_att_float(ncid, varid, name.c_str(), xtype, len,
                &values[0]));
}


void nc::put_att(int ncid, int varid, const string &name,
        const vector<double> &values, nc_type xtype)
{
    size_t len = values.size();
    NETCDF4_TIMING3("nc_put_att_double", len, NC_DOUBLE);
    ERRNO_CHECK(nc_put_att_double(ncid, varid, name.c_str(), xtype, len,
                &values[0]));
}


void nc::get_att(int ncid, int varid, const string &name, string &values)
{
    size_t len = nc::inq_attlen(ncid, varid, name);
    char tmp[len];
    NETCDF4_TIMING3("nc_get_att_text", len, NC_CHAR);
    ERRNO_CHECK(nc_get_att_text(ncid, varid, name.c_str(), tmp));
    values = tmp;
}


void nc::get_att(int ncid, int varid, const string &name,
        vector<char> &values)
{
    size_t len = nc::inq_attlen(ncid, varid, name);
    NETCDF4_TIMING3("nc_get_att_uchar", len, NC_CHAR);
    values.resize(len);
    ERRNO_CHECK(nc_get_att_text(ncid, varid, name.c_str(), &values[0]));
}


void nc::get_att(int ncid, int varid, const string &name,
        vector<unsigned char> &values)
{
    size_t len = nc::inq_attlen(ncid, varid, name);
    NETCDF4_TIMING3("nc_get_att_uchar", len, NC_CHAR);
    values.resize(len);
    ERRNO_CHECK(nc_get_att_uchar(ncid, varid, name.c_str(), &values[0]));
}


void nc::get_att(int ncid, int varid, const string &name,
        vector<signed char> &values)
{
    size_t len = nc::inq_attlen(ncid, varid, name);
    NETCDF4_TIMING3("nc_get_att_schar", len, NC_BYTE);
    values.resize(len);
    ERRNO_CHECK(nc_get_att_schar(ncid, varid, name.c_str(), &values[0]));
}


void nc::get_att(int ncid, int varid, const string &name,
        vector<short> &values)
{
    size_t len = nc::inq_attlen(ncid, varid, name);
    NETCDF4_TIMING3("nc_get_att_short", len, NC_SHORT);
    values.resize(len);
    ERRNO_CHECK(nc_get_att_short(ncid, varid, name.c_str(), &values[0]));
}


void nc::get_att(int ncid, int varid, const string &name,
        vector<int> &values)
{
    size_t len = nc::inq_attlen(ncid, varid, name);
    NETCDF4_TIMING3("nc_get_att_int", len, NC_INT);
    values.resize(len);
    ERRNO_CHECK(nc_get_att_int(ncid, varid, name.c_str(), &values[0]));
}


void nc::get_att(int ncid, int varid, const string &name,
        vector<long> &values)
{
    size_t len = nc::inq_attlen(ncid, varid, name);
    NETCDF4_TIMING3("nc_get_att_long", len, NC_INT);
    values.resize(len);
    ERRNO_CHECK(nc_get_att_long(ncid, varid, name.c_str(), &values[0]));
}


void nc::get_att(int ncid, int varid, const string &name,
        vector<float> &values)
{
    size_t len = nc::inq_attlen(ncid, varid, name);
    NETCDF4_TIMING3("nc_get_att_float", len, NC_FLOAT);
    values.resize(len);
    ERRNO_CHECK(nc_get_att_float(ncid, varid, name.c_str(), &values[0]));
}


void nc::get_att(int ncid, int varid, const string &name,
        vector<double> &values)
{
    size_t len = nc::inq_attlen(ncid, varid, name);
    NETCDF4_TIMING3("nc_get_att_double", len, NC_DOUBLE);
    values.resize(len);
    ERRNO_CHECK(nc_get_att_double(ncid, varid, name.c_str(), &values[0]));
}


void nc::put_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, const unsigned char *op)
{
    NETCDF4_TIMING3("nc_put_vara_uchar", count, NC_CHAR);
    ERRNO_CHECK(nc_put_vara_uchar(ncid, varid, &start[0], &count[0], op));
}


void nc::put_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, const signed char *op)
{
    NETCDF4_TIMING3("nc_put_vara_schar", count, NC_CHAR);
    ERRNO_CHECK(nc_put_vara_schar(ncid, varid, &start[0], &count[0], op));
}


void nc::put_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, const char *op)
{
    NETCDF4_TIMING3("nc_put_vara_text", count, NC_CHAR);
    ERRNO_CHECK(nc_put_vara_text(ncid, varid, &start[0], &count[0], op));
}


void nc::put_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, const short *op)
{
    NETCDF4_TIMING3("nc_put_vara_short", count, NC_SHORT);
    ERRNO_CHECK(nc_put_vara_short(ncid, varid, &start[0], &count[0], op));
}


void nc::put_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, const int *op)
{
    NETCDF4_TIMING3("nc_put_vara_int", count, NC_INT);
    ERRNO_CHECK(nc_put_vara_int(ncid, varid, &start[0], &count[0], op));
}


void nc::put_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, const long *op)
{
    NETCDF4_TIMING3("nc_put_vara_long", count, NC_INT);
    ERRNO_CHECK(nc_put_vara_long(ncid, varid, &start[0], &count[0], op));
}


void nc::put_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, const float *op)
{
    NETCDF4_TIMING3("nc_put_vara_float", count, NC_FLOAT);
    ERRNO_CHECK(nc_put_vara_float(ncid, varid, &start[0], &count[0], op));
}


void nc::put_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, const double *op)
{
    NETCDF4_TIMING3("nc_put_vara_double", count, NC_DOUBLE);
    ERRNO_CHECK(nc_put_vara_double(ncid, varid, &start[0], &count[0], op));
}


void nc::get_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, unsigned char *ip)
{
    NETCDF4_TIMING3("nc_get_vara_uchar", count, NC_CHAR);
    ERRNO_CHECK(nc_get_vara_uchar(ncid, varid, &start[0], &count[0], ip));
}


void nc::get_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, signed char *ip)
{
    NETCDF4_TIMING3("nc_get_vara_schar", count, NC_CHAR);
    ERRNO_CHECK(nc_get_vara_schar(ncid, varid, &start[0], &count[0], ip));
}


void nc::get_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, char *ip)
{
    NETCDF4_TIMING3("nc_get_vara_text", count, NC_CHAR);
    ERRNO_CHECK(nc_get_vara_text(ncid, varid, &start[0], &count[0], ip));
}


void nc::get_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, short *ip)
{
    NETCDF4_TIMING3("nc_get_vara_short", count, NC_SHORT);
    ERRNO_CHECK(nc_get_vara_short(ncid, varid, &start[0], &count[0], ip));
}


void nc::get_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, int *ip)
{
    NETCDF4_TIMING3("nc_get_vara_int", count, NC_INT);
    ERRNO_CHECK(nc_get_vara_int(ncid, varid, &start[0], &count[0], ip));
}


void nc::get_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, long *ip)
{
    NETCDF4_TIMING3("nc_get_vara_long", count, NC_INT);
    ERRNO_CHECK(nc_get_vara_long(ncid, varid, &start[0], &count[0], ip));
}


void nc::get_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, float *ip)
{
    NETCDF4_TIMING3("nc_get_vara_float", count, NC_FLOAT);
    ERRNO_CHECK(nc_get_vara_float(ncid, varid, &start[0], &count[0], ip));
}


void nc::get_vara(int ncid, int varid,
        const vector<size_t> &start,
        const vector<size_t> &count, double *ip)
{
    NETCDF4_TIMING3("nc_get_vara_double", count, NC_DOUBLE);
    ERRNO_CHECK(nc_get_vara_double(ncid, varid, &start[0], &count[0], ip));
}
