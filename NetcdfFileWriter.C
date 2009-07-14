#include <iostream>
    using std::cout;
    using std::endl;
#include <vector>
    using std::vector;

#include <mpi.h>
#include <pnetcdf.h>

#include "Attribute.H"
#include "ConnectivityVariable.H"
#include "Dataset.H"
#include "Dimension.H"
#include "Mask.H"
#include "NetcdfDimension.H"
#include "NetcdfFileWriter.H"
#include "NetcdfVariable.H"
#include "Util.H"
#include "Values.H"

static int err = 0;


void NetcdfFileWriter::write(const string &filename, Dataset *dataset)
{
    int ncid;
    vector<Dimension*> dims_in = dataset->get_dims();
    vector<Dimension*>::const_iterator dims_in_it;
    vector<Variable*> vars_in = dataset->get_vars();
    vector<Variable*>::const_iterator vars_in_it;
    map<string, NetcdfDimension*> dims_out;
    map<string, NetcdfDimension*>::iterator dims_out_it;
    map<string, NetcdfVariable*> vars_out;
    map<string, NetcdfVariable*>::iterator vars_out_it;

    // create the output file
    err = ncmpi_create(MPI_COMM_WORLD, filename.c_str(), NC_WRITE,
            MPI_INFO_NULL, &ncid);
    ERRNO_CHECK(err);

    //////////////
    // define it
    //////////////

    // copy global attributes
    copy_atts(dataset->get_atts(), ncid, NC_GLOBAL);
    
    // define dimensions
    for (dims_in_it=dims_in.begin(); dims_in_it!=dims_in.end(); ++dims_in_it) {
        Dimension *dim = *dims_in_it;
        string name = dim->get_name();
        Mask *mask = dim->get_mask();
        MPI_Offset size;
        int dimid;

        if (mask) {
            size = mask->get_count();
        } else {
            size = dim->get_size();
        }
        err = ncmpi_def_dim(ncid, name.c_str(), size, &dimid);
        dims_out[name] = new NetcdfDimension(ncid, dimid);
    }

    // define variables
    for (vars_in_it=vars_in.begin(); vars_in_it!=vars_in.end(); ++vars_in_it) {
        Variable *var = *vars_in_it;
        string var_name = var->get_name();
        int ndim = var->num_dims();
        int dims[ndim];
        bool skip = false;
        int varid;

        for (int dimidx=0; dimidx<ndim; ++dimidx) {
            string dim_name = var->get_dims()[dimidx]->get_name();
            dims_out_it = dims_out.find(dim_name);
            // skip variables that don't have a corresponding dimension in
            // the output file
            if (dims_out_it != dims_out.end()) {
                dims[dimidx] = dims_out_it->second->get_id();
            } else {
                skip = true;
                break;
            }
        }
        if (skip) {
            continue;
        }
        err = ncmpi_def_var(ncid, var_name.c_str(), var->get_type(),
                ndim, dims, &varid);
        ERRNO_CHECK(err);
        vars_out[var_name] = new NetcdfVariable(ncid, varid);

        // copy variable attributes
        copy_atts(var->get_atts(), ncid, varid);
    }

    err = ncmpi_enddef(ncid);
    ERRNO_CHECK(err);

    //////////////
    // write it
    //////////////
    
    // copy var data
    for (vars_out_it=vars_out.begin(); vars_out_it!=vars_out.end(); ++vars_out_it) {
        NetcdfVariable *var_out = vars_out_it->second;
        Variable *var_in = *Util::find(vars_in, vars_out_it->first);
        ConnectivityVariable *connectivity_var;
        if ((connectivity_var = dynamic_cast<ConnectivityVariable*>(var_in))) {
            copy_var(connectivity_var, var_out);
        } else {
            if (var_in->has_record()) {
                copy_record_var(var_in, var_out);
            } else {
                copy_var(var_in, var_out);
            }
        }
    }
    
    // close it
    err = ncmpi_close(ncid);
    ERRNO_CHECK(err);
}


void NetcdfFileWriter::copy_att(Attribute *attr, int ncid, int varid)
{
    string name = attr->get_name();
    DataType type = attr->get_type();
    MPI_Offset len = attr->get_count();
#define put_attr_values(DT, CT, NAME) \
    if (type == DT) { \
        CT *data; \
        attr->get_values()->as(data); \
        err = ncmpi_put_att_##NAME(ncid, varid, name.c_str(), DT, len, data); \
        ERRNO_CHECK(err); \
    } else
    put_attr_values(DataType::BYTE, unsigned char, uchar)
    put_attr_values(DataType::SHORT, short, short)
    put_attr_values(DataType::INT, int, int)
    //put_attr_values(DataType::LONG, long, long)
    put_attr_values(DataType::FLOAT, float, float)
    put_attr_values(DataType::DOUBLE, double, double)
#undef put_attr_values
    if (type == DataType::CHAR) {
        char *data;
        attr->get_values()->as(data);
        err = ncmpi_put_att_text(ncid, varid, name.c_str(), len, data);
    }
}


void NetcdfFileWriter::copy_atts(
        const vector<Attribute*> &atts,
        int ncid,
        int varid)
{
    vector<Attribute*>::const_iterator att_it;
    for (att_it=atts.begin(); att_it!=atts.end(); ++att_it) {
        copy_att(*att_it, ncid, varid);
    }
}


void NetcdfFileWriter::copy_var(
        ConnectivityVariable *var_in,
        NetcdfVariable *var_out)
{
}


void NetcdfFileWriter::copy_var(Variable *var_in, NetcdfVariable *var_out)
{
    cout << "NetcdfFileWriter::copy_var " << var_in->get_name() << endl;
    size_t ndim = var_in->num_dims();
    int ga_var_in = var_in->get_handle();
    int ga_var_out = var_out->get_handle();

    var_in->read();

    if (var_in->num_masks() > 0) {
        cout << "\tMASK PRESENT" << endl;
        if (ndim == 1) {
            //GA_Pack64(ga_var_in, ga_var_out, TODO_MASK_HANDLE, 0,
        } else {
        }
    } else {
        cout << "\tno masks, so a direct copy" << endl;
        // no masks, so a direct copy
        write(var_in->get_handle(), var_out->get_ncid(), var_out->get_id());
    }

    var_in->release_handle();
    var_out->release_handle();
}


void NetcdfFileWriter::copy_record_var(
        Variable *var_in,
        NetcdfVariable *var_out)
{
    cout << "NetcdfFileWriter::copy_record_var " << var_in->get_name() << endl;
}


void NetcdfFileWriter::write(int handle, int ncid, int varid)
{
    int mt_type;
    int ndim;
    int64_t dim_sizes[GA_MAX_DIM];

    NGA_Inquire64(handle, &mt_type, &ndim, dim_sizes);

    DataType type = DataType::CHAR; type = mt_type;
    int64_t lo[ndim];
    int64_t hi[ndim];
    int64_t ld[ndim-1];
    MPI_Offset start[ndim];
    MPI_Offset count[ndim];
    int err;

    NGA_Distribution64(handle, ME, lo, hi);
    for (size_t dimidx=0; dimidx<ndim; ++dimidx) {
        start[dimidx] = lo[dimidx];
        count[dimidx] = hi[dimidx] - lo[dimidx] + 1;
    }
#define write_var_all(TYPE, NC_TYPE) \
    if (type == NC_TYPE) { \
        TYPE *ptr; \
        NGA_Access64(handle, lo, hi, &ptr, ld); \
        err = ncmpi_put_vara_##TYPE##_all(ncid, varid, start, count, ptr); \
    } else
    write_var_all(int, NC_INT)
    write_var_all(float, NC_FLOAT)
    write_var_all(double, NC_DOUBLE)
    ; // for last else above
#undef write_var_all
}

