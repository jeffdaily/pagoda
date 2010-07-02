#ifndef PNETCDF_H
#define PNETCDF_H

#include <pnetcdf.h>

/**
 * The pnetcdf namespace, providing a C++ layer to pnetcdf.
 *
 * Also uses the custom timing gathering functions for pnetcdf calls.
 */
namespace ncmpi
{
    void create(MPI_Comm comm, const char *path, int cmode, MPI_Info info, int *ncidp);
    void open(MPI_Comm comm, const char *path, int omode, MPI_Info info, int *ncidp);
    void enddef(int ncid);
    void redef(int ncid);
    void get_file_info(int ncid, MPI_Info *info_used);
    void sync(int ncid);
    void abort(int ncid);
    void begin_indep_data(int ncid);
    void end_indep_data(int ncid);
    void close(int ncid);

    void def_dim(int ncid, const char *name, MPI_Offset len, int *idp);
    void def_var(int ncid, const char *name, nc_type xtype, int ndims, const int *dimidsp, int *varidp);
    void rename_dim(int ncid, int dimid, const char *name);
    void rename_var(int ncid, int varid, const char *name);

    void inq(int ncid, int *ndimsp, int *nvarsp, int *ngattsp, int *unlimdimidp);
    void inq_ndims(int ncid, int *ndimsp);
    void inq_nvars(int ncid, int *nvarsp);
    void inq_natts(int ncid, int *ngattsp);
    void inq_unlimdim(int ncid, int *unlimdimidp);
    void inq_dimid(int ncid, const char *name, int *idp);
    void inq_dim(int ncid, int dimid, char *name, MPI_Offset *lenp);
    void inq_dimname(int ncid, int dimid, char *name);
    void inq_dimlen(int ncid, int dimid, MPI_Offset *lenp);
    void inq_var(int ncid, int varid, char *name, nc_type *xtypep, int *ndimsp, int *dimidsp, int *nattsp);
    void inq_varid(int ncid, const char *name, int *varidp);
    void inq_varname(int ncid, int varid, char *name);
    void inq_vartype(int ncid, int varid, nc_type *xtypep);
    void inq_varndims(int ncid, int varid, int *ndimsp);
    void inq_vardimid(int ncid, int varid, int *dimidsp);
    void inq_varnatts(int ncid, int varid, int *nattsp);

    void inq_att    (int ncid, int varid, const char *name, nc_type *xtypep, MPI_Offset *lenp);
    void inq_attid  (int ncid, int varid, const char *name, int *idp);
    void inq_atttype(int ncid, int varid, const char *name, nc_type *xtypep);
    void inq_attlen (int ncid, int varid, const char *name, MPI_Offset *lenp);
    void inq_attname(int ncid, int varid, int attnum, char *name);
    void copy_att   (int ncid, int varid, const char *name, int ncid_out, int varid_out);
    void rename_att (int ncid, int varid, const char *name, const char *newname);
    void del_att    (int ncid, int varid, const char *name);

    void put_att(int ncid, int varid, const char *name, MPI_Offset len, const char *op);
    void put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const char *op);
    void put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const unsigned char *op);
    void put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const signed char *op);
    void put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const short *op);
    void put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const int *op);
    void put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const long *op);
    void put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const float *op);
    void put_att(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const double *op);

    void get_att(int ncid, int varid, const char *name, char *ip);
    void get_att(int ncid, int varid, const char *name, unsigned char *ip);
    void get_att(int ncid, int varid, const char *name, signed char *ip);
    void get_att(int ncid, int varid, const char *name, short *ip);
    void get_att(int ncid, int varid, const char *name, int *ip);
    void get_att(int ncid, int varid, const char *name, long *ip);
    void get_att(int ncid, int varid, const char *name, float *ip);
    void get_att(int ncid, int varid, const char *name, double *ip);

    void put_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const short *op);
    void put_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const int *op);
    void put_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const float *op);
    void put_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const double *op);
    void put_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const short *op);
    void put_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const int *op);
    void put_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const float *op);
    void put_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const double *op);

    void get_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], short *ip);
    void get_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], int *ip);
    void get_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], float *ip);
    void get_vara_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], double *ip);
    void get_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], short *ip);
    void get_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], int *ip);
    void get_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], float *ip);
    void get_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], double *ip);

    int iget_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], short *ip);
    int iget_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], int *ip);
    int iget_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], float *ip);
    int iget_vara(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], double *ip);
    
    void wait_all(int ncid, int count, int array_of_requests[], int array_of_statuses[]);

    void put_vars_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const short *op);
    void put_vars_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const int *op);
    void put_vars_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const float *op);
    void put_vars_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const double *op);
    void put_vars(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const short *op);
    void put_vars(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const int *op);
    void put_vars(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const float *op);
    void put_vars(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const double *op);

    void get_vars_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], short *ip);
    void get_vars_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], int *ip);
    void get_vars_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], float *ip);
    void get_vars_all(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], double *ip);
    void get_vars(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], short *ip);
    void get_vars(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], int *ip);
    void get_vars(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], float *ip);
    void get_vars(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], double *ip);
}; // namespace ncmpi

#endif // PNETCDF_H
