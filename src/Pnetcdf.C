#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <pnetcdf.h>

#include "Debug.H"
#include "NetcdfError.H"
#include "Pnetcdf.H"
#include "PnetcdfTiming.H"


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


int ncmpi::iget_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], short *ip)
{
#if HAVE_PNETCDF_NEW_NB
    int request;
#   ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#   endif
    PNETCDF_TIMING4("ncmpi_iget_vara_short", ndim, count, NC_SHORT);
    ERRNO_CHECK(ncmpi_iget_vara_short(ncid, varid, start, count, ip, &request));
    return request;
#else
    get_vara_all(ncid, varid, start, count, ip);
    return 0;
#endif
}


int ncmpi::iget_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], int *ip)
{
#if HAVE_PNETCDF_NEW_NB
    int request;
#   ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#   endif
    PNETCDF_TIMING4("ncmpi_iget_vara_int", ndim, count, NC_SHORT);
    ERRNO_CHECK(ncmpi_iget_vara_int(ncid, varid, start, count, ip, &request));
    return request;
#else
    get_vara_all(ncid, varid, start, count, ip);
    return 0;
#endif
}


int ncmpi::iget_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], float *ip)
{
#if HAVE_PNETCDF_NEW_NB
    int request;
#   ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#   endif
    PNETCDF_TIMING4("ncmpi_iget_vara_float", ndim, count, NC_SHORT);
    ERRNO_CHECK(ncmpi_iget_vara_float(ncid, varid, start, count, ip, &request));
    return request;
#else
    get_vara_all(ncid, varid, start, count, ip);
    return 0;
#endif
}


int ncmpi::iget_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], double *ip)
{
#if HAVE_PNETCDF_NEW_NB
    int request;
#   ifdef GATHER_PNETCDF_TIMING
    int ndim;
    ERRNO_CHECK(ncmpi_inq_varndims(ncid, varid, &ndim));
#   endif
    PNETCDF_TIMING4("ncmpi_iget_vara_double", ndim, count, NC_SHORT);
    ERRNO_CHECK(ncmpi_iget_vara_double(ncid, varid, start, count, ip, &request));
    return request;
#else
    get_vara_all(ncid, varid, start, count, ip);
    return 0;
#endif
}


void ncmpi::wait_all(int ncid, int count, int array_of_requests[],
        int array_of_statuses[])
{
#if HAVE_PNETCDF_NEW_NB
    PNETCDF_TIMING1("ncmpi_waitall");
    ERRNO_CHECK(ncmpi_wait_all(ncid, count, array_of_requests, array_of_statuses));
#endif
}
