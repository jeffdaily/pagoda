#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <pnetcdf.h>

#include "Array.H"
#include "Attribute.H"
#include "Common.H"
#include "Debug.H"
#include "Mask.H"
#include "NetcdfAttribute.H"
#include "NetcdfDataset.H"
#include "NetcdfDimension.H"
#include "NetcdfError.H"
#include "NetcdfVariable.H"
#include "Pack.H"
#include "Pnetcdf.H"
#include "Timing.H"
#include "Util.H"


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

    transform(atts.begin(), atts.end(), atts.begin(),
            pagoda::ptr_deleter<NetcdfAttribute*>);
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


Dataset* NetcdfVariable::get_dataset() const
{
    TIMING("NetcdfVariable::get_dataset()");
    return dataset;
}


DataType NetcdfVariable::get_type() const
{
    TIMING("NetcdfVariable::get_type()");
    return type;
}


Array* NetcdfVariable::read(Array *dst) const
{
    return read(dst, false);
}


Array* NetcdfVariable::iread(Array *dst)
{
    return read(dst, true);
}


Array* NetcdfVariable::read(Array *dst, bool nonblocking) const
{
    int64_t ndim = get_ndim();
    vector<int64_t> lo(ndim);
    vector<int64_t> hi(ndim);
    vector<MPI_Offset> start(ndim);
    vector<MPI_Offset> count(ndim);
    bool found_bit = true;
    Array *tmp;

    TRACER("NetcdfVariable::read(Array*) %s\n", get_name().c_str());
    TIMING("NetcdfVariable::read(Array*)");

    // if we are subsetting, then the passed in array is different than the
    // one in which the data is read into
    if (needs_subset()) {
        get_dataset()->push_masks(NULL);
        tmp = Array::create(type, get_shape());
        get_dataset()->pop_masks();
    } else {
        tmp = dst;
    }
    
    tmp->get_distribution(lo,hi);

    if (tmp->get_ndim() != ndim) {
        pagoda::abort("NetcdfVariable::read(Array*) :: shape mismatch");
    }

    found_bit = find_bit(get_dims(), lo, hi);

    if (tmp->owns_data() && found_bit) {
        for (int64_t dimidx=0; dimidx<ndim; ++dimidx) {
            start[dimidx] = lo[dimidx];
            count[dimidx] = hi[dimidx] - lo[dimidx] + 1;
        }
    } else {
        // make a non-participating process a no-op
        fill(start.begin(), start.end(), 0);
        fill(count.begin(), count.end(), 0);
    }

    do_read(tmp, start, count, found_bit, nonblocking);

    // check whether a subset is needed
    if (needs_subset()) {
        pagoda::pack(tmp, dst, get_masks());
        delete tmp;
    }

    return dst;
}


Array* NetcdfVariable::read(int64_t record, Array *dst) const
{
    return read(record, dst, false);
}


Array* NetcdfVariable::iread(int64_t record, Array *dst)
{
    return read(record, dst, true);
}


Array* NetcdfVariable::read(int64_t record, Array *dst, bool nonblocking) const
{
    int64_t ndim = get_ndim();
    vector<int64_t> lo(ndim);
    vector<int64_t> hi(ndim);
    vector<MPI_Offset> start(ndim);
    vector<MPI_Offset> count(ndim);
    vector<Dimension*> adims(dims.begin()+1,dims.end());
    bool found_bit = true;

    TRACER("NetcdfVariable::read(int64_t,Array*) %s\n", get_name().c_str());
    TIMING("NetcdfVariable::read(int64_t,Array*)");

    dst->get_distribution(lo,hi);

    if (dst->get_ndim()+1 != ndim) {
        pagoda::abort("NetcdfVariable::read(int64_t,Array*) :: shape mismatch");
    }

    found_bit = find_bit(adims, lo, hi);

    if (dst->owns_data() && found_bit) {
        start[0] = record;
        count[0] = 1;
        for (int64_t dimidx=1; dimidx<ndim; ++dimidx) {
            start[dimidx] = lo[dimidx-1];
            count[dimidx] = hi[dimidx-1] - lo[dimidx-1] + 1;
        }
    } else {
        // make a non-participating process a no-op
        fill(start.begin(), start.end(), 0);
        fill(count.begin(), count.end(), 0);
    }

    do_read(dst, start, count, found_bit, nonblocking);

    // check whether a subset is needed
    if (needs_subset()) {
        pagoda::print_sync("needs subset record\n");
    }

    return dst;
}


bool NetcdfVariable::find_bit(const vector<Dimension*> &adims,
        const vector<int64_t> &lo, const vector<int64_t> &hi) const
{
    bool found_bit = true;

#ifdef READ_OPT
    // only attempt the optimization for record variables
    // **Why did I decide to do it that way??**
    // check all dimension masks for a non-zero bit

    int64_t ndim = adims.size();

    for (int64_t i=0; i<ndim; ++i) {
        bool current_found_bit = false;
        Mask *mask = get_dataset()->get_masks()->get(adims[i]);
        int *data = mask->get(lo[i],hi[i]);

        for (int64_t j=0,limit=hi[i]-lo[i]+1; j<limit; ++j) {
            if (data[j] != 0) {
                current_found_bit = true;
                break;
            }
        }
        found_bit &= current_found_bit;
        delete [] data;
    }
#endif

    return found_bit;
}


void NetcdfVariable::do_read(Array *dst, const vector<MPI_Offset> &start,
        const vector<MPI_Offset> &count, bool found_bit, bool nonblocking) const
{
    int ncid = get_netcdf_dataset()->get_id();
    DataType type = dst->get_type();

#define read_var_all(TYPE, DT) \
    if (type == DT) { \
        TYPE *ptr = NULL; \
        if (dst->owns_data() && found_bit) { \
            ptr = (TYPE*)dst->access(); \
        } \
        if (nonblocking) { \
            int request; \
            request = ncmpi::iget_vara(ncid, id, &start[0], &count[0], ptr); \
            get_netcdf_dataset()->requests.push_back(request); \
            get_netcdf_dataset()->arrays_to_release.push_back(dst); \
        } else { \
            ncmpi::get_vara_all(ncid, id, &start[0], &count[0], ptr); \
        } \
        if (dst->owns_data() && found_bit && !nonblocking) { \
            dst->release_update(); \
        } \
    } else
    read_var_all(int,    DataType::INT)
    read_var_all(float,  DataType::FLOAT)
    read_var_all(double, DataType::DOUBLE)
    {
        throw DataTypeException("DataType not handled", type);
    }
#undef read_var_all
}


ostream& NetcdfVariable::print(ostream &os) const
{
    TIMING("NetcdfVariable::print(ostream)");
    os << "NetcdfVariable(" << name << ")";
    return os;
}


NetcdfDataset* NetcdfVariable::get_netcdf_dataset() const
{
    TIMING("NetcdfVariable::get_netcdf_dataset()");
    return dataset;
}


int NetcdfVariable::get_id() const
{
    TIMING("NetcdfVariable::get_id()");
    return id;
}
