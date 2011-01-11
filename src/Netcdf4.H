#ifndef NETCDF4_H
#define NETCDF4_H

#include <netcdf.h>
#include <string>
#include <vector>

using std::string;
using std::vector;


/**
 * The netcdf namespace, providing a C++ layer to netcdf.
 *
 * Provides a more C++ friendly interface, using vector<>, string,
 * and references.  Return single values, otherwise by reference.
 *
 * Also uses the custom timing gathering functions for netcdf calls.
 */
namespace nc
{
    int         create       (const string &path, int cmode);
    int         create       (const string &path, int cmode, size_t initialsz, size_t &bufrsizehint);
    int         create       (const string &path, int cmode, MPI_Comm comm, MPI_Info info);
    int         open         (const string &path, int omode);
    int         open         (const string &path, int omode, size_t &bufrsizehint);
    int         open         (const string &path, int omode, MPI_Comm comm, MPI_Info info);
    void        enddef       (int ncid);
    void        enddef       (int ncid, size_t h_minfree, size_t v_align, size_t v_minfree, size_t r_align);
    void        redef        (int ncid);
    void        sync         (int ncid);
    void        abort        (int ncid);
    void        close        (int ncid);
    int         inq_format   (int ncid);
    void        inq          (int ncid, int &ndims, int &nvars, int &ngatts, int &unlimdimid);
    int         inq_ndims    (int ncid);
    int         inq_nvars    (int ncid);
    int         inq_natts    (int ncid);
    int         inq_unlimdim (int ncid);
    vector<int> inq_unlimdims(int ncid);
    int         set_fill     (int ncid, int fillmode);
    int         set_default_format(int format);
    void        set_chunk_cache(size_t  size, size_t  nelems, float  preemption);
    void        get_chunk_cache(size_t &size, size_t &nelems, float &preemption);

    int         def_dim      (int ncid, const string &name, size_t len);
    void        rename_dim   (int ncid, int dimid, const string &name);
    int         inq_dimid    (int ncid, const string &name);
    void        inq_dim      (int ncid, int dimid, string &name, size_t &len);
    string      inq_dimname  (int ncid, int dimid);
    size_t      inq_dimlen   (int ncid, int dimid);

    int         def_var      (int ncid, const string &name, nc_type xtype, const vector<int> &dimids);
    void        def_var_chunking(int ncid, int varid, int storage, const vector<size_t> &chunksizes);
    void        def_var_fill (int ncid, int varid, int no_fill, void *fill_value);
    void        def_var_deflate(int ncid, int varid, int shuffle, int deflate, int deflate_level);
    void        rename_var   (int ncid, int varid, const string &name);
    void        inq_var      (int ncid, int varid, string &name, nc_type &xtype, vector<int> &dimids, int &natts);
    void        inq_var_chunking(int ncid, int varid, int &storage, vector<size_t> &chunksize);
    void        inq_var_fill (int ncid, int varid, int &no_fill, void *fill_value);
    void        inq_var_deflate(int ncid, int varid, int &shuffle, int &deflate, int &deflate_level);
    int         inq_varid    (int ncid, const string &name);
    string      inq_varname  (int ncid, int varid);
    nc_type     inq_vartype  (int ncid, int varid);
    int         inq_varndims (int ncid, int varid);
    vector<int> inq_vardimid (int ncid, int varid);
    int         inq_varnatts (int ncid, int varid);
    void        set_var_chunk_cache(int ncid, int varid, size_t  size, size_t  nelems, float  preemption);
    void        get_var_chunk_cache(int ncid, int varid, size_t &size, size_t &nelems, float &preemption);
    void        var_par_access(int ncid, int varid, int access);

    void        copy_att     (int ncid, int varid, const string &name, int ncid_out, int varid_out);
    void        del_att      (int ncid, int varid, const string &name);
    void        rename_att   (int ncid, int varid, const string &name, const string &newname);
    void        inq_att      (int ncid, int varid, const string &name, nc_type &xtype, size_t &len);
    int         inq_attid    (int ncid, int varid, const string &name);
    nc_type     inq_atttype  (int ncid, int varid, const string &name);
    size_t      inq_attlen   (int ncid, int varid, const string &name);
    string      inq_attname  (int ncid, int varid, int attnum);

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

    void put_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, const unsigned char *op);
    void put_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, const signed char *op);
    void put_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, const char *op);
    void put_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, const short *op);
    void put_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, const int *op);
    void put_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, const long *op);
    void put_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, const float *op);
    void put_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, const double *op);

    void get_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, unsigned char *ip);
    void get_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, signed char *ip);
    void get_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, char *ip);
    void get_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, short *ip);
    void get_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, int *ip);
    void get_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, long *ip);
    void get_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, float *ip);
    void get_vara(int ncid, int varid, const vector<size_t> &start, const vector<size_t> &count, double *ip);

    /*
    void put_vars_all(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], const short *op);
    void put_vars_all(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], const int *op);
    void put_vars_all(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], const float *op);
    void put_vars_all(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], const double *op);
    void put_vars(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], const short *op);
    void put_vars(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], const int *op);
    void put_vars(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], const float *op);
    void put_vars(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], const double *op);

    void get_vars_all(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], short *ip);
    void get_vars_all(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], int *ip);
    void get_vars_all(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], float *ip);
    void get_vars_all(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], double *ip);
    void get_vars(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], short *ip);
    void get_vars(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], int *ip);
    void get_vars(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], float *ip);
    void get_vars(int ncid, int varid, const size_t start[], const size_t count[], const size_t stride[], double *ip);
    */
}; // namespace ncmpi

#endif // NETCDF4_H
