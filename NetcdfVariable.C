#include <pnetcdf.h>

#include "Attribute.H"
#include "NetcdfAttribute.H"
#include "NetcdfDimension.H"
#include "NetcdfVariable.H"
#include "Util.H"


NetcdfVariable::NetcdfVariable(int ncid, int varid)
    :   AbstractVariable()
    ,   ncid(ncid)
    ,   id(varid)
    ,   name("")
    ,   dims()
    ,   atts()
    ,   type(DataType::CHAR)
{
    int ndim;
    int err = ncmpi_inq_ndims(ncid, &ndim);
    ERRNO_CHECK(err);
    char cname[MAX_NAME];
    int dim_ids[ndim]; // plenty big (file's ndim <= variable's ndim)
    nc_type type_tmp;
    int natt;
    err = ncmpi_inq_var(ncid, varid, cname, &type_tmp, &ndim, dim_ids, &natt);
    ERRNO_CHECK(err);
    name = string(cname);
    type = type_tmp;
    for (int dimidx=0; dimidx<ndim; ++dimidx) {
        dims.push_back(new NetcdfDimension(ncid, dim_ids[dimidx]));
    }
    for (int attid=0; attid<natt; ++attid) {
        atts.push_back(new NetcdfAttribute(ncid, attid, varid));
    }
}


NetcdfVariable::~NetcdfVariable()
{
}


string NetcdfVariable::get_name() const
{
    return name;
}


bool NetcdfVariable::has_record() const
{
    return dims[0]->is_unlimited();
}


size_t NetcdfVariable::num_dims() const
{
    return dims.size();
}


vector<Dimension*> NetcdfVariable::get_dims() const
{
    return vector<Dimension*>(dims.begin(), dims.end());
}


size_t NetcdfVariable::num_atts() const
{
    return atts.size();
}


vector<Attribute*> NetcdfVariable::get_atts() const
{
    return vector<Attribute*>(atts.begin(), atts.end());
}


DataType NetcdfVariable::get_type() const
{
    return type;
}


void NetcdfVariable::read()
{
    DataType type = get_type();
    size_t ndim = num_dims();
    size_t dimidx = 0;
    int64_t lo[ndim];
    int64_t hi[ndim];
    int64_t ld[ndim-1];
    MPI_Offset start[ndim];
    MPI_Offset count[ndim];
    int err;

    if (has_record()) {
        dimidx = 1;
        start[0] = record_index;
    } else {
        dimidx = 0;
    }
    NGA_Distribution64(handle, ME, lo, hi);
    for (size_t dimidx=0; dimidx<ndim; ++dimidx) {
        start[dimidx] = lo[dimidx];
        count[dimidx] = hi[dimidx] - lo[dimidx] + 1;
    }

#define read_var_all(TYPE, NC_TYPE) \
    if (type == NC_TYPE) { \
        TYPE *ptr; \
        NGA_Access64(handle, lo, hi, &ptr, ld); \
        err = ncmpi_get_vara_##TYPE##_all(ncid, id, start, count, ptr); \
    } else
    read_var_all(int, NC_INT)
    read_var_all(float, NC_FLOAT)
    read_var_all(double, NC_DOUBLE)
    ; // for last else above
#undef read_var_all
    ERRNO_CHECK(err);
    NGA_Release_update64(handle, lo, hi);
}


ostream& NetcdfVariable::print(ostream &os) const
{
    os << "NetcdfVariable(" << name << ")";
    return os;
}

