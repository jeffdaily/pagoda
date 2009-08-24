#include <vector>
using std::vector;

#include <mpi.h>
#include <pnetcdf.h>

#include "Attribute.H"
#include "ConnectivityVariable.H"
#include "Dataset.H"
#include "Dimension.H"
#include "DistributedMask.H"
#include "Mask.H"
#include "NetcdfDimension.H"
#include "NetcdfFileWriter.H"
#include "NetcdfVariable.H"
#include "Pack.H"
#include "Util.H"
#include "Values.H"

static int err = 0;


void NetcdfFileWriter::write(const string &filename, Dataset *dataset)
{
    TRACER1("NetcdfFileWriter::write(%s)\n", filename.c_str());
    vector<Dimension*> dims_in = dataset->get_dims();
    vector<Dimension*>::const_iterator dims_in_it;
    vector<Variable*> vars_in = dataset->get_vars();
    vector<Variable*>::const_iterator vars_in_it;
    int ncid;
    map<string,int> dims_out;
    map<string,int>::iterator dims_out_it;
    map<string,int> vars_out;
    map<string,int>::iterator vars_out_it;

    // create the output file
    //err = ncmpi_create(MPI_COMM_WORLD, filename.c_str(), NC_64BIT_OFFSET, MPI_INFO_NULL, &ncid);
    err = ncmpi_create(MPI_COMM_WORLD, filename.c_str(), NC_64BIT_DATA, MPI_INFO_NULL, &ncid);
    ERRNO_CHECK(err);

    //////////////
    // define it
    //////////////

    // copy global attributes
    TRACER("copy global attributes\n");
    copy_atts(dataset->get_atts(), ncid, NC_GLOBAL);

    // define dimensions
    TRACER("define dimensions\n");
    for (dims_in_it=dims_in.begin(); dims_in_it!=dims_in.end(); ++dims_in_it) {
        Dimension *dim = *dims_in_it;
        string name = dim->get_name();
        Mask *mask = dim->get_mask();
        MPI_Offset size;
        int dimid;

        if (dim->is_unlimited()) {
            size = NC_UNLIMITED;
        } else if (mask) {
            size = mask->get_count();
        } else {
            size = dim->get_size();
        }
        TRACER2("define dimension '%s'=%lld\n", name.c_str(), size);
        err = ncmpi_def_dim(ncid, name.c_str(), size, &dimid);
        ERRNO_CHECK(err);
        dims_out[name] = dimid;
    }

    // define variables
    TRACER("define variables\n");
    for (vars_in_it=vars_in.begin(); vars_in_it!=vars_in.end(); ++vars_in_it) {
        Variable *var = *vars_in_it;
        string var_name = var->get_name();
        int ndim = var->num_dims();
        int dims[ndim];
        bool skip = false;
        int varid;
        TRACER1("define variable '%s'\n", var_name.c_str());

        for (int dimidx=0; dimidx<ndim; ++dimidx) {
            string dim_name = var->get_dims()[dimidx]->get_name();
            dims_out_it = dims_out.find(dim_name);
            // skip variables that don't have a corresponding dimension in
            // the output file
            if (dims_out_it != dims_out.end()) {
                dims[dimidx] = dims_out_it->second;
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
        vars_out[var_name] = varid;

        copy_atts(var->get_atts(), ncid, varid);
    }

    err = ncmpi_enddef(ncid);
    ERRNO_CHECK(err);

    //////////////
    // write it
    //////////////

    // copy var data
    TRACER("copy var data\n");
    for (vars_out_it=vars_out.begin(); vars_out_it!=vars_out.end(); ++vars_out_it) {
        Variable *var_in = *Util::find(vars_in, vars_out_it->first);
        if (var_in->has_record() && var_in->num_dims() > 1) {
            copy_record_var(var_in, ncid, vars_out_it->second);
        } else {
            copy_var(var_in, ncid, vars_out_it->second);
        }
    }

    // close it
    TRACER("close it\n");
    err = ncmpi_close(ncid);
    ERRNO_CHECK(err);
    TRACER("after ncmpi_close\n");
}


void NetcdfFileWriter::copy_att(Attribute *attr, int ncid, int varid)
{
    TRACER1("NetcdfFileWriter::copy_att %s\n", attr->get_name().c_str());
    string name = attr->get_name();
    DataType type = attr->get_type();
    MPI_Offset len = attr->get_count();
#define put_attr_values(DT, CT, NAME) \
    if (type == DT) { \
        CT *data; \
        attr->get_values()->as(data); \
        err = ncmpi_put_att_##NAME(ncid, varid, name.c_str(), DT, len, data); \
        ERRNO_CHECK(err); \
        delete [] data; \
    } else
    put_attr_values(DataType::BYTE, unsigned char, uchar)
    put_attr_values(DataType::SHORT, short, short)
    put_attr_values(DataType::INT, int, int)
    put_attr_values(DataType::FLOAT, float, float)
    put_attr_values(DataType::DOUBLE, double, double)
#undef put_attr_values
    if (type == DataType::CHAR) {
        char *data;
        attr->get_values()->as(data);
        err = ncmpi_put_att_text(ncid, varid, name.c_str(), len, data);
        ERRNO_CHECK(err);
        delete [] data;
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


void NetcdfFileWriter::copy_var(Variable *var_in, int ncid, int varid)
{
    TRACER1("NetcdfFileWriter::copy_var %s BEGIN\n", var_in->get_name().c_str());
    size_t ndim = var_in->num_dims();
    int ga_var_in = var_in->get_handle();
    int ga_masks[ndim];
    //size_t num_masks = var_in->num_masks();
    size_t num_cleared_masks = var_in->num_cleared_masks();

    var_in->read();
    var_in->reindex(); // noop if not ConnectivityVariable

    if (num_cleared_masks > 0) {
        int ga_var_out;
        int dim_ids[ndim];
        int64_t dim_lens[ndim];

        TRACER("determine size of GA to create for packed destination\n");
        err = ncmpi_inq_vardimid(ncid, varid, dim_ids);
        ERRNO_CHECK(err);
        for (size_t dimidx=0; dimidx<ndim; ++dimidx) {
            MPI_Offset tmp;
            err = ncmpi_inq_dimlen(ncid, dim_ids[dimidx], &tmp);
            ERRNO_CHECK(err);
            dim_lens[dimidx] = tmp;
            ga_masks[dimidx] = ((DistributedMask*)var_in->get_dims()[dimidx]->get_mask())->get_handle();
        }
        TRACER("before ga_var_out (pack_dst) create\n");
        ga_var_out = NGA_Create64(var_in->get_type().as_mt(), ndim, dim_lens,
                "pack_dst", NULL);
        pack(ga_var_in, ga_var_out, ga_masks);
        write(ga_var_out, ncid, varid);
        GA_Destroy(ga_var_out);
    } else {
        // no masks, so a direct copy
        write(var_in->get_handle(), ncid, varid);
    }

    var_in->release_handle();
    TRACER1("NetcdfFileWriter::copy_var %s END\n", var_in->get_name().c_str());
}


void NetcdfFileWriter::copy_record_var(Variable *var_in, int ncid, int varid)
{
    TRACER1("NetcdfFileWriter::copy_record_var %s\n", var_in->get_name().c_str());
    vector<Dimension*> dims = var_in->get_dims();
    size_t ndim = dims.size();
    int64_t nrec = dims[0]->get_size();
    int ga_var_in = var_in->get_handle();
    int ga_masks[ndim];
    // assuming currently that record dimension has a mask
    int *rec_mask = dims[0]->get_mask()->get_data();
    size_t recidx=0;
    size_t packed_recidx=0;

    int ga_var_out;
    int dim_ids[ndim];
    int64_t dim_lens[ndim];

    // determine size of GA to create for packed destination
    err = ncmpi_inq_vardimid(ncid, varid, dim_ids);
    ERRNO_CHECK(err);
    for (size_t dimidx=0; dimidx<ndim; ++dimidx) {
        MPI_Offset tmp;
        err = ncmpi_inq_dimlen(ncid, dim_ids[dimidx], &tmp);
        ERRNO_CHECK(err);
        dim_lens[dimidx] = tmp;
        ga_masks[dimidx] = ((DistributedMask*)var_in->get_dims()[dimidx]->get_mask())->get_handle();
    }
    ga_var_out = NGA_Create64(var_in->get_type().as_mt(), ndim-1, dim_lens+1,
            "pack_dst", NULL);

    for (recidx=0; recidx<nrec; ++recidx) {
        if (rec_mask[recidx] != 0) {
            var_in->set_record_index(recidx);
            var_in->read();
            pack(ga_var_in, ga_var_out, ga_masks+1);
            write(ga_var_out, ncid, varid, packed_recidx++);
        }
    }

    GA_Destroy(ga_var_out);
    var_in->release_handle();
}


void NetcdfFileWriter::write(int handle, int ncid, int varid, int recidx)
{
    TRACER4("NetcdfFileWriter::write %d %d %d %d\n", handle, ncid, varid, recidx);
    DataType type = DataType::CHAR;
    int mt_type;
    int ndim;
    int64_t dim_sizes[GA_MAX_DIM];
    int64_t lo[GA_MAX_DIM];
    int64_t hi[GA_MAX_DIM];
    int64_t ld[GA_MAX_DIM-1];
    MPI_Offset start[GA_MAX_DIM];
    MPI_Offset count[GA_MAX_DIM];
    int dimidx=0;

    NGA_Inquire64(handle, &mt_type, &ndim, dim_sizes);
    type.from_mt(mt_type);
    NGA_Distribution64(handle, ME, lo, hi);

    if (0 > lo[0] && 0 > hi[0]) {
        // make a non-participating process a no-op
        for (dimidx=0; dimidx<ndim; ++dimidx) {
            start[dimidx] = 0;
            count[dimidx] = 0;
        }
#define write_var_all(TYPE, NC_TYPE) \
        if (type == NC_TYPE) { \
            err = ncmpi_put_vara_##TYPE##_all(ncid, varid, start, count, NULL);\
        } else
        write_var_all(int, NC_INT)
            write_var_all(float, NC_FLOAT)
            write_var_all(double, NC_DOUBLE)
            ; // for last else above
#undef write_var_all
        ERRNO_CHECK(err);
    } else {
        if (recidx >= 0) {
            start[0] = recidx;
            count[0] = 1;
            for (dimidx=0; dimidx<ndim; ++dimidx) {
                start[dimidx+1] = lo[dimidx];
                count[dimidx+1] = hi[dimidx] - lo[dimidx] + 1;
            }
        } else {
            for (dimidx=0; dimidx<ndim; ++dimidx) {
                start[dimidx] = lo[dimidx];
                count[dimidx] = hi[dimidx] - lo[dimidx] + 1;
            }
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
        ERRNO_CHECK(err);
        NGA_Release64(handle, lo, hi);
    }
}

