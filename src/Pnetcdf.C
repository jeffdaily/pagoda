#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <pnetcdf.h>

#include <string>
#include <vector>

#include "Debug.H"
#include "PnetcdfError.H"
#include "Pnetcdf.H"
#include "PnetcdfTiming.H"

using std::string;
using std::vector;


int ncmpi::create(MPI_Comm comm, const string &path, int cmode, MPI_Info info)
{
    int ncid;
    PNETCDF_TIMING1("ncmpi_create");
    ERRNO_CHECK(ncmpi_create(comm, path.c_str(), cmode, info, &ncid));
    return ncid;
}


int ncmpi::open(MPI_Comm comm, const string &path, int omode, MPI_Info info)
{
    int ncid;
    PNETCDF_TIMING1("ncmpi_open");
    ERRNO_CHECK(ncmpi_open(comm, path.c_str(), omode, info, &ncid));
    return ncid;
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


MPI_Info ncmpi::get_file_info(int ncid)
{
    MPI_Info info;
    PNETCDF_TIMING1("ncmpi_get_file_info");
    ERRNO_CHECK(ncmpi_get_file_info(ncid, &info));
    return info;
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


int ncmpi::def_dim(int ncid, const string &name, MPI_Offset len)
{
    int id;
    PNETCDF_TIMING1("ncmpi_def_dim");
    ERRNO_CHECK(ncmpi_def_dim(ncid, name.c_str(), len, &id));
    return id;
}


int ncmpi::def_var(int ncid, const string &name, nc_type xtype,
                   const vector<int> &dimids)
{
    int id;
    PNETCDF_TIMING1("ncmpi_def_var");
    ERRNO_CHECK(ncmpi_def_var(ncid, name.c_str(), xtype, dimids.size(),
                              &dimids[0], &id));
    return id;
}


void ncmpi::rename_dim(int ncid, int dimid, const string &name)
{
    PNETCDF_TIMING1("ncmpi_rename_dim");
    ERRNO_CHECK(ncmpi_rename_dim(ncid, dimid, name.c_str()));
}


void ncmpi::rename_var(int ncid, int varid, const string &name)
{
    PNETCDF_TIMING1("ncmpi_rename_var");
    ERRNO_CHECK(ncmpi_rename_var(ncid, varid, name.c_str()));
}


int ncmpi::inq_format(int ncid)
{
#if HAVE_NCMPI_INQ_FORMAT
    int format;
    PNETCDF_TIMING1("ncmpi_inq_format");
    ERRNO_CHECK(ncmpi_inq_format(ncid, &format));
    return format;
#else
    return NC_FORMAT_64BIT;
#endif
}

void ncmpi::inq(int ncid, int &ndims, int &nvars, int &ngatts,
                int &unlimdimid)
{
    PNETCDF_TIMING1("ncmpi_inq");
    ERRNO_CHECK(ncmpi_inq(ncid, &ndims, &nvars, &ngatts, &unlimdimid));
}


int ncmpi::inq_ndims(int ncid)
{
    int ndims;
    PNETCDF_TIMING1("ncmpi_inq_ndims");
    ERRNO_CHECK(ncmpi_inq_ndims(ncid, &ndims));
    return ndims;
}


int ncmpi::inq_nvars(int ncid)
{
    int nvars;
    PNETCDF_TIMING1("ncmpi_inq_nvars");
    ERRNO_CHECK(ncmpi_inq_nvars(ncid, &nvars));
    return nvars;
}


int ncmpi::inq_natts(int ncid)
{
    int natts;
    PNETCDF_TIMING1("ncmpi_inq_natts");
    ERRNO_CHECK(ncmpi_inq_natts(ncid, &natts));
    return natts;
}


int ncmpi::inq_unlimdim(int ncid)
{
    int dim;
    PNETCDF_TIMING1("ncmpi_inq_unlimdim");
    ERRNO_CHECK(ncmpi_inq_unlimdim(ncid, &dim));
    return dim;
}


int ncmpi::inq_dimid(int ncid, const string &name)
{
    int id;
    PNETCDF_TIMING1("ncmpi_inq_dimid");
    ERRNO_CHECK(ncmpi_inq_dimid(ncid, name.c_str(), &id));
    return id;
}


void ncmpi::inq_dim(int ncid, int dimid, string &name, MPI_Offset &lenp)
{
    char cname[NC_MAX_NAME];
    PNETCDF_TIMING1("ncmpi_inq_dim");
    ERRNO_CHECK(ncmpi_inq_dim(ncid, dimid, cname, &lenp));
    name = cname;
}


string ncmpi::inq_dimname(int ncid, int dimid)
{
    char name[NC_MAX_NAME];
    PNETCDF_TIMING1("ncmpi_inq_dimname");
    ERRNO_CHECK(ncmpi_inq_dimname(ncid, dimid, name));
    return string(name);
}


MPI_Offset ncmpi::inq_dimlen(int ncid, int dimid)
{
    MPI_Offset len;
    PNETCDF_TIMING1("ncmpi_inq_dimlen");
    ERRNO_CHECK(ncmpi_inq_dimlen(ncid, dimid, &len));
    return len;
}


void ncmpi::inq_var(int ncid, int varid, string &name, nc_type &xtypep,
                    vector<int> &dimids, int &natts)
{
    char cname[NC_MAX_NAME];
    int ndimsp;
    int dimidsp[NC_MAX_VAR_DIMS];
    PNETCDF_TIMING1("ncmpi_inq_var");
    ERRNO_CHECK(ncmpi_inq_var(ncid, varid, cname, &xtypep, &ndimsp,
                              dimidsp, &natts));
    name = cname;
    dimids.assign(dimidsp, dimidsp+ndimsp);
}


int ncmpi::inq_varid(int ncid, const string &name)
{
    int id;
    PNETCDF_TIMING1("ncmpi_inq_varid");
    ERRNO_CHECK(ncmpi_inq_varid(ncid, name.c_str(), &id));
    return id;
}


string ncmpi::inq_varname(int ncid, int varid)
{
    char name[NC_MAX_NAME];
    PNETCDF_TIMING1("ncmpi_inq_varname");
    ERRNO_CHECK(ncmpi_inq_varname(ncid, varid, name));
    return string(name);
}


nc_type ncmpi::inq_vartype(int ncid, int varid)
{
    nc_type xtype;
    PNETCDF_TIMING1("ncmpi_inq_vartype");
    ERRNO_CHECK(ncmpi_inq_vartype(ncid, varid, &xtype));
    return xtype;
}


int ncmpi::inq_varndims(int ncid, int varid)
{
    int ndims;
    PNETCDF_TIMING1("ncmpi_inq_varndims");
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndims));
    return ndims;
}


vector<int> ncmpi::inq_vardimid(int ncid, int varid)
{
    vector<int> dimids;
    int dimidsp[NC_MAX_VAR_DIMS];
    PNETCDF_TIMING1("ncmpi_inq_vardimid");
    ERRNO_CHECK(ncmpi_inq_vardimid(ncid, varid, dimidsp));
    dimids.assign(dimidsp, dimidsp+ncmpi::inq_varndims(ncid, varid));
    return dimids;
}


int ncmpi::inq_varnatts(int ncid, int varid)
{
    int natts;
    PNETCDF_TIMING1("ncmpi_inq_varnatts");
    ERRNO_CHECK(ncmpi_inq_varnatts(ncid, varid, &natts));
    return natts;
}


void ncmpi::inq_att(int ncid, int varid, const string &name,
                    nc_type &xtype, MPI_Offset &len)
{
    PNETCDF_TIMING1("ncmpi_inq_att");
    ERRNO_CHECK(ncmpi_inq_att(ncid, varid, name.c_str(), &xtype, &len));
}


int ncmpi::inq_attid(int ncid, int varid, const string &name)
{
    int id;
    PNETCDF_TIMING1("ncmpi_inq_attid");
    ERRNO_CHECK(ncmpi_inq_attid(ncid, varid, name.c_str(), &id));
    return id;
}


nc_type ncmpi::inq_atttype(int ncid, int varid, const string &name)
{
    nc_type xtype;
    PNETCDF_TIMING1("ncmpi_inq_atttype");
    ERRNO_CHECK(ncmpi_inq_atttype(ncid, varid, name.c_str(), &xtype));
    return xtype;
}


MPI_Offset ncmpi::inq_attlen(int ncid, int varid, const string &name)
{
    MPI_Offset len;
    PNETCDF_TIMING1("ncmpi_inq_attlen");
    ERRNO_CHECK(ncmpi_inq_attlen(ncid, varid, name.c_str(), &len));
    return len;
}


string ncmpi::inq_attname(int ncid, int varid, int attnum)
{
    char name[NC_MAX_NAME];
    PNETCDF_TIMING1("ncmpi_inq_attname");
    ERRNO_CHECK(ncmpi_inq_attname(ncid, varid, attnum, name));
    return string(name);
}


void ncmpi::copy_att(int ncid, int varid, const string &name,
                     int ncid_out, int varid_out)
{
    PNETCDF_TIMING1("ncmpi_copy_att");
    ERRNO_CHECK(ncmpi_copy_att(ncid, varid, name.c_str(), ncid_out, varid_out));
}


void ncmpi::rename_att(int ncid, int varid, const string &name,
                       const string &newname)
{
    PNETCDF_TIMING1("ncmpi_rename_att");
    ERRNO_CHECK(ncmpi_rename_att(ncid, varid, name.c_str(), newname.c_str()));
}


void ncmpi::del_att(int ncid, int varid, const string &name)
{
    PNETCDF_TIMING1("ncmpi_del_att");
    ERRNO_CHECK(ncmpi_del_att(ncid, varid, name.c_str()));
}


void ncmpi::put_att(int ncid, int varid, const string &name,
                    const string &values)
{
    MPI_Offset len = values.size();
    PNETCDF_TIMING3("ncmpi_put_att_text", len, NC_CHAR);
    ERRNO_CHECK(ncmpi_put_att_text(ncid, varid, name.c_str(), len,
                                   values.c_str()));
}


void ncmpi::put_att(int ncid, int varid, const string &name,
                    const vector<char> &values, nc_type xtype)
{
    MPI_Offset len = values.size();
    PNETCDF_TIMING3("ncmpi_put_att_uchar", len, NC_CHAR);
    ERRNO_CHECK(ncmpi_put_att_text(ncid, varid, name.c_str(), len,
                                   &values[0]));
}


void ncmpi::put_att(int ncid, int varid, const string &name,
                    const vector<unsigned char> &values, nc_type xtype)
{
    MPI_Offset len = values.size();
    PNETCDF_TIMING3("ncmpi_put_att_uchar", len, NC_BYTE);
    ERRNO_CHECK(ncmpi_put_att_uchar(ncid, varid, name.c_str(), xtype, len,
                                    &values[0]));
}


void ncmpi::put_att(int ncid, int varid, const string &name,
                    const vector<signed char> &values, nc_type xtype)
{
    MPI_Offset len = values.size();
    PNETCDF_TIMING3("ncmpi_put_att_schar", len, NC_BYTE);
    ERRNO_CHECK(ncmpi_put_att_schar(ncid, varid, name.c_str(), xtype, len,
                                    &values[0]));
}


void ncmpi::put_att(int ncid, int varid, const string &name,
                    const vector<short> &values, nc_type xtype)
{
    MPI_Offset len = values.size();
    PNETCDF_TIMING3("ncmpi_put_att_short", len, NC_SHORT);
    ERRNO_CHECK(ncmpi_put_att_short(ncid, varid, name.c_str(), xtype, len,
                                    &values[0]));
}


void ncmpi::put_att(int ncid, int varid, const string &name,
                    const vector<int> &values, nc_type xtype)
{
    MPI_Offset len = values.size();
    PNETCDF_TIMING3("ncmpi_put_att_int", len, NC_INT);
    ERRNO_CHECK(ncmpi_put_att_int(ncid, varid, name.c_str(), xtype, len,
                                  &values[0]));
}


void ncmpi::put_att(int ncid, int varid, const string &name,
                    const vector<long> &values, nc_type xtype)
{
    MPI_Offset len = values.size();
    PNETCDF_TIMING3("ncmpi_put_att_long", len, NC_INT);
    ERRNO_CHECK(ncmpi_put_att_long(ncid, varid, name.c_str(), xtype, len,
                                   &values[0]));
}


void ncmpi::put_att(int ncid, int varid, const string &name,
                    const vector<float> &values, nc_type xtype)
{
    MPI_Offset len = values.size();
    PNETCDF_TIMING3("ncmpi_put_att_float", len, NC_FLOAT);
    ERRNO_CHECK(ncmpi_put_att_float(ncid, varid, name.c_str(), xtype, len,
                                    &values[0]));
}


void ncmpi::put_att(int ncid, int varid, const string &name,
                    const vector<double> &values, nc_type xtype)
{
    MPI_Offset len = values.size();
    PNETCDF_TIMING3("ncmpi_put_att_double", len, NC_DOUBLE);
    ERRNO_CHECK(ncmpi_put_att_double(ncid, varid, name.c_str(), xtype, len,
                                     &values[0]));
}


void ncmpi::get_att(int ncid, int varid, const string &name, string &values)
{
    MPI_Offset len = ncmpi::inq_attlen(ncid, varid, name);
    char tmp[len];
    PNETCDF_TIMING3("ncmpi_get_att_text", len, NC_CHAR);
    ERRNO_CHECK(ncmpi_get_att_text(ncid, varid, name.c_str(), tmp));
    values = tmp;
}


void ncmpi::get_att(int ncid, int varid, const string &name,
                    vector<char> &values)
{
    MPI_Offset len = ncmpi::inq_attlen(ncid, varid, name);
    PNETCDF_TIMING3("ncmpi_get_att_uchar", len, NC_CHAR);
    values.resize(len);
    ERRNO_CHECK(ncmpi_get_att_text(ncid, varid, name.c_str(), &values[0]));
}


void ncmpi::get_att(int ncid, int varid, const string &name,
                    vector<unsigned char> &values)
{
    MPI_Offset len = ncmpi::inq_attlen(ncid, varid, name);
    PNETCDF_TIMING3("ncmpi_get_att_uchar", len, NC_CHAR);
    values.resize(len);
    ERRNO_CHECK(ncmpi_get_att_uchar(ncid, varid, name.c_str(), &values[0]));
}


void ncmpi::get_att(int ncid, int varid, const string &name,
                    vector<signed char> &values)
{
    MPI_Offset len = ncmpi::inq_attlen(ncid, varid, name);
    PNETCDF_TIMING3("ncmpi_get_att_schar", len, NC_BYTE);
    values.resize(len);
    ERRNO_CHECK(ncmpi_get_att_schar(ncid, varid, name.c_str(), &values[0]));
}


void ncmpi::get_att(int ncid, int varid, const string &name,
                    vector<short> &values)
{
    MPI_Offset len = ncmpi::inq_attlen(ncid, varid, name);
    PNETCDF_TIMING3("ncmpi_get_att_short", len, NC_SHORT);
    values.resize(len);
    ERRNO_CHECK(ncmpi_get_att_short(ncid, varid, name.c_str(), &values[0]));
}


void ncmpi::get_att(int ncid, int varid, const string &name,
                    vector<int> &values)
{
    MPI_Offset len = ncmpi::inq_attlen(ncid, varid, name);
    PNETCDF_TIMING3("ncmpi_get_att_int", len, NC_INT);
    values.resize(len);
    ERRNO_CHECK(ncmpi_get_att_int(ncid, varid, name.c_str(), &values[0]));
}


void ncmpi::get_att(int ncid, int varid, const string &name,
                    vector<long> &values)
{
    MPI_Offset len = ncmpi::inq_attlen(ncid, varid, name);
    PNETCDF_TIMING3("ncmpi_get_att_long", len, NC_INT);
    values.resize(len);
    ERRNO_CHECK(ncmpi_get_att_long(ncid, varid, name.c_str(), &values[0]));
}


void ncmpi::get_att(int ncid, int varid, const string &name,
                    vector<float> &values)
{
    MPI_Offset len = ncmpi::inq_attlen(ncid, varid, name);
    PNETCDF_TIMING3("ncmpi_get_att_float", len, NC_FLOAT);
    values.resize(len);
    ERRNO_CHECK(ncmpi_get_att_float(ncid, varid, name.c_str(), &values[0]));
}


void ncmpi::get_att(int ncid, int varid, const string &name,
                    vector<double> &values)
{
    MPI_Offset len = ncmpi::inq_attlen(ncid, varid, name);
    PNETCDF_TIMING3("ncmpi_get_att_double", len, NC_DOUBLE);
    values.resize(len);
    ERRNO_CHECK(ncmpi_get_att_double(ncid, varid, name.c_str(), &values[0]));
}


void ncmpi::put_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, const unsigned char *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_uchar_all", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_put_vara_uchar_all(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::put_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, const signed char *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_schar_all", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_put_vara_schar_all(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::put_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, const char *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_text_all", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_put_vara_text_all(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::put_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, const short *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_short_all", count, NC_SHORT);
    ERRNO_CHECK(ncmpi_put_vara_short_all(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::put_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, const int *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_int_all", count, NC_INT);
    ERRNO_CHECK(ncmpi_put_vara_int_all(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::put_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, const long *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_long_all", count, NC_INT);
    ERRNO_CHECK(ncmpi_put_vara_long_all(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::put_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, const float *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_float_all", count, NC_FLOAT);
    ERRNO_CHECK(ncmpi_put_vara_float_all(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::put_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, const double *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_double_all", count, NC_DOUBLE);
    ERRNO_CHECK(ncmpi_put_vara_double_all(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::get_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, unsigned char *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_uchar_all", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_get_vara_uchar_all(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::get_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, signed char *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_schar_all", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_get_vara_schar_all(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::get_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, char *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_text_all", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_get_vara_text_all(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::get_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, short *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_short_all", count, NC_SHORT);
    ERRNO_CHECK(ncmpi_get_vara_short_all(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::get_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, int *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_int_all", count, NC_INT);
    ERRNO_CHECK(ncmpi_get_vara_int_all(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::get_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, long *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_long_all", count, NC_INT);
    ERRNO_CHECK(ncmpi_get_vara_long_all(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::get_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, float *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_float_all", count, NC_FLOAT);
    ERRNO_CHECK(ncmpi_get_vara_float_all(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::get_vara_all(int ncid, int varid,
                         const vector<MPI_Offset> &start,
                         const vector<MPI_Offset> &count, double *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_double_all", count, NC_DOUBLE);
    ERRNO_CHECK(ncmpi_get_vara_double_all(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::put_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, const unsigned char *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_uchar", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_put_vara_uchar(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::put_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, const signed char *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_schar", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_put_vara_schar(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::put_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, const char *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_text", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_put_vara_text(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::put_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, const short *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_short", count, NC_SHORT);
    ERRNO_CHECK(ncmpi_put_vara_short(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::put_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, const int *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_int", count, NC_INT);
    ERRNO_CHECK(ncmpi_put_vara_int(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::put_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, const long *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_long", count, NC_INT);
    ERRNO_CHECK(ncmpi_put_vara_long(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::put_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, const float *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_float", count, NC_FLOAT);
    ERRNO_CHECK(ncmpi_put_vara_float(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::put_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, const double *op)
{
    PNETCDF_TIMING3("ncmpi_put_vara_double", count, NC_DOUBLE);
    ERRNO_CHECK(ncmpi_put_vara_double(ncid, varid, &start[0], &count[0], op));
}


void ncmpi::get_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, unsigned char *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_uchar", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_get_vara_uchar(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::get_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, signed char *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_schar", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_get_vara_schar(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::get_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, char *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_text", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_get_vara_text(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::get_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, short *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_short", count, NC_SHORT);
    ERRNO_CHECK(ncmpi_get_vara_short(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::get_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, int *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_int", count, NC_INT);
    ERRNO_CHECK(ncmpi_get_vara_int(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::get_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, long *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_long", count, NC_INT);
    ERRNO_CHECK(ncmpi_get_vara_long(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::get_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, float *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_float", count, NC_FLOAT);
    ERRNO_CHECK(ncmpi_get_vara_float(ncid, varid, &start[0], &count[0], ip));
}


void ncmpi::get_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, double *ip)
{
    PNETCDF_TIMING3("ncmpi_get_vara_double", count, NC_DOUBLE);
    ERRNO_CHECK(ncmpi_get_vara_double(ncid, varid, &start[0], &count[0], ip));
}


int ncmpi::iget_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, unsigned char *ip)
{
#if HAVE_PNETCDF_NEW_NB
    int request;
    PNETCDF_TIMING3("ncmpi_iget_vara_uchar", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_iget_vara_uchar(ncid, varid, &start[0], &count[0], ip, &request));
    return request;
#else
    get_vara_all(ncid, varid, start, count, ip);
    return 0;
#endif
}


int ncmpi::iget_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, signed char *ip)
{
#if HAVE_PNETCDF_NEW_NB
    int request;
    PNETCDF_TIMING3("ncmpi_iget_vara_schar", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_iget_vara_schar(ncid, varid, &start[0], &count[0], ip, &request));
    return request;
#else
    get_vara_all(ncid, varid, start, count, ip);
    return 0;
#endif
}


int ncmpi::iget_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, char *ip)
{
#if HAVE_PNETCDF_NEW_NB
    int request;
    PNETCDF_TIMING3("ncmpi_iget_vara_text", count, NC_CHAR);
    ERRNO_CHECK(ncmpi_iget_vara_text(ncid, varid, &start[0], &count[0], ip, &request));
    return request;
#else
    get_vara_all(ncid, varid, start, count, ip);
    return 0;
#endif
}


int ncmpi::iget_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, short *ip)
{
#if HAVE_PNETCDF_NEW_NB
    int request;
    PNETCDF_TIMING3("ncmpi_iget_vara_short", count, NC_SHORT);
    ERRNO_CHECK(ncmpi_iget_vara_short(ncid, varid, &start[0], &count[0], ip, &request));
    return request;
#else
    get_vara_all(ncid, varid, start, count, ip);
    return 0;
#endif
}


int ncmpi::iget_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, int *ip)
{
#if HAVE_PNETCDF_NEW_NB
    int request;
    PNETCDF_TIMING3("ncmpi_iget_vara_int", count, NC_INT);
    ERRNO_CHECK(ncmpi_iget_vara_int(ncid, varid, &start[0], &count[0], ip, &request));
    return request;
#else
    get_vara_all(ncid, varid, start, count, ip);
    return 0;
#endif
}


int ncmpi::iget_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, long *ip)
{
#if HAVE_PNETCDF_NEW_NB
    int request;
    PNETCDF_TIMING3("ncmpi_iget_vara_long", count, NC_INT);
    ERRNO_CHECK(ncmpi_iget_vara_long(ncid, varid, &start[0], &count[0], ip, &request));
    return request;
#else
    get_vara_all(ncid, varid, start, count, ip);
    return 0;
#endif
}


int ncmpi::iget_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, float *ip)
{
#if HAVE_PNETCDF_NEW_NB
    int request;
    PNETCDF_TIMING3("ncmpi_iget_vara_float", count, NC_FLOAT);
    ERRNO_CHECK(ncmpi_iget_vara_float(ncid, varid, &start[0], &count[0], ip, &request));
    return request;
#else
    get_vara_all(ncid, varid, start, count, ip);
    return 0;
#endif
}


int ncmpi::iget_vara(int ncid, int varid,
                     const vector<MPI_Offset> &start,
                     const vector<MPI_Offset> &count, double *ip)
{
#if HAVE_PNETCDF_NEW_NB
    int request;
    PNETCDF_TIMING3("ncmpi_iget_vara_double", count, NC_DOUBLE);
    ERRNO_CHECK(ncmpi_iget_vara_double(ncid, varid, &start[0], &count[0], ip, &request));
    return request;
#else
    get_vara_all(ncid, varid, start, count, ip);
    return 0;
#endif
}


void ncmpi::wait_all(int ncid,
                     vector<int> &array_of_requests,
                     vector<int> &array_of_statuses)
{
#if HAVE_PNETCDF_NEW_NB
    PNETCDF_TIMING1("ncmpi_waitall");
    ERRNO_CHECK(ncmpi_wait_all(ncid, array_of_requests.size(),
                               &array_of_requests[0], &array_of_statuses[0]));
#endif
}
