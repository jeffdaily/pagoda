#ifndef PNETCDF_H
#define PNETCDF_H

#include <pnetcdf.h>
#include <string>
#include <vector>

using std::string;
using std::vector;


/**
 * The pnetcdf namespace, providing a C++ layer to pnetcdf.
 *
 * Provides a more C++ friendly interface, using vector<>, string,
 * and references.  Return single values, otherwise by reference.
 *
 * Also uses the custom timing gathering functions for pnetcdf calls.
 */
namespace ncmpi
{
    int      create(MPI_Comm comm, const string &path, int cmode, MPI_Info info);
    int      open(MPI_Comm comm, const string &path, int omode, MPI_Info info);
    void     enddef(int ncid);
    void     redef(int ncid);
    MPI_Info get_file_info(int ncid);
    void     sync(int ncid);
    void     abort(int ncid);
    void     begin_indep_data(int ncid);
    void     end_indep_data(int ncid);
    void     close(int ncid);

    int  def_dim   (int ncid, const string &name, MPI_Offset len);
    int  def_var   (int ncid, const string &name, nc_type xtype, const vector<int> &dimids);
    void rename_dim(int ncid, int dimid, const string &name);
    void rename_var(int ncid, int varid, const string &name);

    void        inq(int ncid, int &ndims, int &nvars, int &ngatts, int &unlimdimid);
    int         inq_ndims(int ncid);
    int         inq_nvars(int ncid);
    int         inq_natts(int ncid);
    int         inq_unlimdim(int ncid);
    int         inq_dimid(int ncid, const string &name);
    void        inq_dim(int ncid, int dimid, string &name, MPI_Offset &len);
    string      inq_dimname(int ncid, int dimid);
    MPI_Offset  inq_dimlen(int ncid, int dimid);
    void        inq_var(int ncid, int varid, string &name, nc_type &xtype, vector<int> &dimids, int &natts);
    int         inq_varid(int ncid, const string &name);
    string      inq_varname(int ncid, int varid);
    nc_type     inq_vartype(int ncid, int varid);
    int         inq_varndims(int ncid, int varid);
    vector<int> inq_vardimid(int ncid, int varid);
    int         inq_varnatts(int ncid, int varid);

    void        inq_att    (int ncid, int varid, const string &name, nc_type &xtype, MPI_Offset &len);
    int         inq_attid  (int ncid, int varid, const string &name);
    nc_type     inq_atttype(int ncid, int varid, const string &name);
    MPI_Offset  inq_attlen (int ncid, int varid, const string &name);
    string      inq_attname(int ncid, int varid, int attnum);
    void        copy_att   (int ncid, int varid, const string &name, int ncid_out, int varid_out);
    void        rename_att (int ncid, int varid, const string &name, const string &newname);
    void        del_att    (int ncid, int varid, const string &name);

    void put_att(int ncid, int varid, const string &name, const string &values);
    void put_att(int ncid, int varid, const string &name, const vector<char> &values, nc_type xtype=NC_CHAR);
    void put_att(int ncid, int varid, const string &name, const vector<unsigned char> &values, nc_type xtype=NC_BYTE);
    void put_att(int ncid, int varid, const string &name, const vector<signed char> &values, nc_type xtype=NC_BYTE);
    void put_att(int ncid, int varid, const string &name, const vector<short> &values, nc_type xtype=NC_SHORT);
    void put_att(int ncid, int varid, const string &name, const vector<int> &values, nc_type xtype=NC_INT);
    void put_att(int ncid, int varid, const string &name, const vector<long> &values, nc_type xtype=NC_INT);
    void put_att(int ncid, int varid, const string &name, const vector<float> &values, nc_type xtype=NC_FLOAT);
    void put_att(int ncid, int varid, const string &name, const vector<double> &values, nc_type xtype=NC_DOUBLE);

    void get_att(int ncid, int varid, const string &name, string &values);
    void get_att(int ncid, int varid, const string &name, vector<char> &values);
    void get_att(int ncid, int varid, const string &name, vector<unsigned char> &values);
    void get_att(int ncid, int varid, const string &name, vector<signed char> &values);
    void get_att(int ncid, int varid, const string &name, vector<short> &values);
    void get_att(int ncid, int varid, const string &name, vector<int> &values);
    void get_att(int ncid, int varid, const string &name, vector<long> &values);
    void get_att(int ncid, int varid, const string &name, vector<float> &values);
    void get_att(int ncid, int varid, const string &name, vector<double> &values);

    void put_vara_all(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, const short *op);
    void put_vara_all(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, const int *op);
    void put_vara_all(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, const float *op);
    void put_vara_all(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, const double *op);
    void put_vara(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, const short *op);
    void put_vara(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, const int *op);
    void put_vara(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, const float *op);
    void put_vara(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, const double *op);

    void get_vara_all(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, short *ip);
    void get_vara_all(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, int *ip);
    void get_vara_all(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, float *ip);
    void get_vara_all(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, double *ip);
    void get_vara(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, short *ip);
    void get_vara(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, int *ip);
    void get_vara(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, float *ip);
    void get_vara(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, double *ip);

    int iget_vara(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, short *ip);
    int iget_vara(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, int *ip);
    int iget_vara(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, float *ip);
    int iget_vara(int ncid, int varid, const vector<MPI_Offset> &start, const vector<MPI_Offset> &count, double *ip);
    
    void wait_all(int ncid, vector<int> &array_of_requests, vector<int> &array_of_statuses);

    /*
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
    */
}; // namespace ncmpi

#endif // PNETCDF_H
