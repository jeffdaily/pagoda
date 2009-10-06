#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <ga.h>
#include <pnetcdf.h>

#include "Attribute.H"
#include "Common.H"
#include "NetcdfAttribute.H"
#include "NetcdfDataset.H"
#include "NetcdfDimension.H"
#include "NetcdfError.H"
#include "NetcdfVariable.H"
#include "PnetcdfTiming.H"
#include "Timing.H"


NetcdfVariable::NetcdfVariable(NetcdfDataset *dataset, int varid)
    :   AbstractVariable()
    ,   dataset(dataset)
    ,   id(varid)
    ,   name("")
    ,   dims()
    ,   atts()
    ,   type(NC_CHAR)
{
    TIMING("NetcdfVariable::NetcdfVariable(NetcdfDataset*,int)");
    int ncid = dataset->get_id();
    int ndim;
    ncmpi::inq_ndims(ncid, &ndim);
    char cname[MAX_NAME];
    int dim_ids[ndim]; // plenty big (file's ndim <= variable's ndim)
    nc_type type_tmp;
    int natt;
    ncmpi::inq_var(ncid, varid, cname, &type_tmp, &ndim, dim_ids, &natt);
    name = string(cname);
    type = type_tmp;
    for (int dimidx=0; dimidx<ndim; ++dimidx) {
        dims.push_back(dataset->get_dim(dim_ids[dimidx]));
    }
    for (int attid=0; attid<natt; ++attid) {
        atts.push_back(new NetcdfAttribute(dataset, attid, this));
    }
}


NetcdfVariable::~NetcdfVariable()
{
    TIMING("NetcdfVariable::~NetcdfVariable()");
}


string NetcdfVariable::get_name() const
{
    TIMING("NetcdfVariable::get_name()");
    return name;
}


vector<Dimension*> NetcdfVariable::get_dims() const
{
    TIMING("NetcdfVariable::get_dims()");
    return vector<Dimension*>(dims.begin(), dims.end());
}


vector<Attribute*> NetcdfVariable::get_atts() const
{
    TIMING("NetcdfVariable::get_atts()");
    return vector<Attribute*>(atts.begin(), atts.end());
}


DataType NetcdfVariable::get_type() const
{
    TIMING("NetcdfVariable::get_type()");
    return type;
}


void NetcdfVariable::read()
{
    TIMING("NetcdfVariable::read()");
    int me = GA_Nodeid();
    int ncid = dataset->get_id();
    DataType type = get_type();
    size_t ndim = num_dims();
    size_t dimidx = 0;
    int64_t lo[ndim];
    int64_t hi[ndim];
    int64_t ld[ndim-1];
    MPI_Offset start[ndim];
    MPI_Offset count[ndim];
    int err;

    NGA_Distribution64(get_handle(), me, lo, hi);

    if (0 > lo[0] && 0 > hi[0]) {
        // make a non-participating process a no-op
        for (dimidx=0; dimidx<ndim; ++dimidx) {
            start[dimidx] = 0;
            count[dimidx] = 0;
        }
#define read_var_all(TYPE, NC_TYPE) \
        if (type == NC_TYPE) { \
            ncmpi::get_vara_all(ncid, id, start, count, (TYPE*)NULL); \
        } else
        read_var_all(int,    NC_INT)
        read_var_all(float,  NC_FLOAT)
        read_var_all(double, NC_DOUBLE)
        ; // for last else above
#undef read_var_all
    } else {
        if (has_record() && ndim > 1) {
            start[0] = record_index;
            count[0] = 1;
            for (dimidx=1; dimidx<ndim; ++dimidx) {
                start[dimidx] = lo[dimidx-1];
                count[dimidx] = hi[dimidx-1] - lo[dimidx-1] + 1;
            }
        } else {
            for (dimidx=0; dimidx<ndim; ++dimidx) {
                start[dimidx] = lo[dimidx];
                count[dimidx] = hi[dimidx] - lo[dimidx] + 1;
            }
        }
#define read_var_all(TYPE, NC_TYPE) \
        if (type == NC_TYPE) { \
            TYPE *ptr; \
            NGA_Access64(get_handle(), lo, hi, &ptr, ld); \
            ncmpi::get_vara_all(ncid, id, start, count, ptr); \
        } else
        read_var_all(int, NC_INT)
        read_var_all(float, NC_FLOAT)
        read_var_all(double, NC_DOUBLE)
        ; // for last else above
#undef read_var_all
        NGA_Release_update64(get_handle(), lo, hi);
    }
}


ostream& NetcdfVariable::print(ostream &os) const
{
    TIMING("NetcdfVariable::print(ostream)");
    os << "NetcdfVariable(" << name << ")";
    return os;
}


NetcdfDataset* NetcdfVariable::get_dataset() const
{
    TIMING("NetcdfVariable::get_dataset()");
    return dataset;
}


int NetcdfVariable::get_id() const
{
    TIMING("NetcdfVariable::get_id()");
    return id;
}
