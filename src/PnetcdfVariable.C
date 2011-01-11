#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <pnetcdf.h>

#include <algorithm>

#include "Array.H"
#include "Attribute.H"
#include "Common.H"
#include "Debug.H"
#include "Mask.H"
#include "PnetcdfAttribute.H"
#include "PnetcdfDataset.H"
#include "PnetcdfDimension.H"
#include "PnetcdfError.H"
#include "PnetcdfVariable.H"
#include "Pack.H"
#include "PnetcdfNS.H"
#include "Timing.H"
#include "Util.H"

using std::fill;


PnetcdfVariable::PnetcdfVariable(PnetcdfDataset *dataset, int varid)
    :   AbstractVariable()
    ,   dataset(dataset)
    ,   id(varid)
    ,   name("")
    ,   dims()
    ,   atts()
    ,   type(DataType::CHAR)
{
    TIMING("PnetcdfVariable::PnetcdfVariable(PnetcdfDataset*,int)");
    int ncid = dataset->get_id();
    vector<int> dim_ids;
    nc_type type_tmp;
    int natt;
    ncmpi::inq_var(ncid, varid, name, type_tmp, dim_ids, natt);
    type = PnetcdfDataset::to_dt(type_tmp);
    for (size_t dimidx=0; dimidx<dim_ids.size(); ++dimidx) {
        dims.push_back(dataset->get_dim(dim_ids[dimidx]));
    }
    for (int attid=0; attid<natt; ++attid) {
        atts.push_back(new PnetcdfAttribute(dataset, attid, this));
    }
}


PnetcdfVariable::~PnetcdfVariable()
{
    TIMING("PnetcdfVariable::~PnetcdfVariable()");

    transform(atts.begin(), atts.end(), atts.begin(),
              pagoda::ptr_deleter<PnetcdfAttribute*>);
}


string PnetcdfVariable::get_name() const
{
    TIMING("PnetcdfVariable::get_name()");
    return name;
}


vector<Dimension*> PnetcdfVariable::get_dims() const
{
    TIMING("PnetcdfVariable::get_dims()");
    return vector<Dimension*>(dims.begin(), dims.end());
}


vector<Attribute*> PnetcdfVariable::get_atts() const
{
    TIMING("PnetcdfVariable::get_atts()");
    return vector<Attribute*>(atts.begin(), atts.end());
}


Dataset* PnetcdfVariable::get_dataset() const
{
    TIMING("PnetcdfVariable::get_dataset()");
    return dataset;
}


DataType PnetcdfVariable::get_type() const
{
    TIMING("PnetcdfVariable::get_type()");
    return type;
}


Array* PnetcdfVariable::read(Array *dst) const
{
    if (dst == NULL) {
        return AbstractVariable::read_alloc();
    }
    return read(dst, false);
}


Array* PnetcdfVariable::iread(Array *dst)
{
    if (dst == NULL) {
        return AbstractVariable::iread_alloc();
    }
    return read(dst, true);
}


Array* PnetcdfVariable::read(Array *dst, bool nonblocking) const
{
    int64_t ndim = get_ndim();
    vector<int64_t> lo(ndim);
    vector<int64_t> hi(ndim);
    vector<MPI_Offset> start(ndim);
    vector<MPI_Offset> count(ndim);
    bool found_bit = true;
    Array *tmp;

    TRACER("PnetcdfVariable::read(Array*) %s\n", get_name().c_str());
    TIMING("PnetcdfVariable::read(Array*)");

    // if we are subsetting, then the passed in array is different than the
    // one in which the data is read into i.e. subset occurs after the fact
    if (needs_subset()) {
        get_dataset()->push_masks(NULL);
        tmp = Array::create(type, get_shape());
        get_dataset()->pop_masks();
    }
    else {
        tmp = dst;
    }

    tmp->get_distribution(lo,hi);

    if (tmp->get_ndim() != ndim) {
        pagoda::abort("PnetcdfVariable::read(Array*) :: shape mismatch");
    }

    found_bit = find_bit(get_dims(), lo, hi);

    if (tmp->owns_data() && found_bit) {
        for (int64_t dimidx=0; dimidx<ndim; ++dimidx) {
            start[dimidx] = lo[dimidx];
            count[dimidx] = hi[dimidx] - lo[dimidx] + 1;
        }
    }
    else {
        // make a non-participating process a no-op
        fill(start.begin(), start.end(), 0);
        fill(count.begin(), count.end(), 0);
    }

    do_read(tmp, start, count, found_bit, nonblocking);

    if (nonblocking) {
        // can't perform subset until read is finished
        if (needs_subset()) {
            get_netcdf_dataset()->arrays_to_pack.push_back(dst);
            get_netcdf_dataset()->vars_to_pack.push_back(this);
        } else {
            get_netcdf_dataset()->arrays_to_pack.push_back(NULL);
            get_netcdf_dataset()->vars_to_pack.push_back(NULL);
        }
    } else {
        // check whether a subset is needed
        if (needs_subset()) {
            pagoda::pack(tmp, dst, get_masks());
            delete tmp;
            if (needs_renumber()) {
                renumber(dst);
            }
        }
    }

    // propagate fill value to Array
    if (has_fill_value()) {
        dst->set_fill_value(get_fill_value());
    }

    return dst;
}


Array* PnetcdfVariable::read(int64_t record, Array *dst) const
{
    if (dst == NULL) {
        return AbstractVariable::read_alloc(record);
    }
    return read(record, dst, false);
}


Array* PnetcdfVariable::iread(int64_t record, Array *dst)
{
    if (dst == NULL) {
        return AbstractVariable::iread_alloc(record);
    }
    return read(record, dst, true);
}


Array* PnetcdfVariable::read(int64_t record, Array *dst, bool nonblocking) const
{
    int64_t ndim = get_ndim();
    vector<int64_t> lo(ndim);
    vector<int64_t> hi(ndim);
    vector<MPI_Offset> start(ndim);
    vector<MPI_Offset> count(ndim);
    vector<Dimension*> adims(dims.begin()+1,dims.end());
    bool found_bit = true;
    Array *tmp;

    TRACER("PnetcdfVariable::read(int64_t,Array*) %s\n", get_name().c_str());
    TIMING("PnetcdfVariable::read(int64_t,Array*)");

    // if we are subsetting, then the passed in array is different than the
    // one in which the data is read into i.e. subset occurs after the fact
    if (needs_subset_record()) {
        vector<int64_t> shape;
        get_dataset()->push_masks(NULL);
        shape = get_shape();
        get_dataset()->pop_masks();
        shape.erase(shape.begin());
        tmp = Array::create(type, shape);
    }
    else {
        tmp = dst;
    }

    tmp->get_distribution(lo,hi);

    if (tmp->get_ndim()+1 != ndim) {
        pagoda::abort("PnetcdfVariable::read(int64_t,Array*) :: shape mismatch");
    }

    found_bit = find_bit(adims, lo, hi);

    if (tmp->owns_data() && found_bit) {
        if (needs_subset_record()) {
            start[0] = translate_record(record);
        }
        else {
            start[0] = record;
        }
        count[0] = 1;
        for (int64_t dimidx=1; dimidx<ndim; ++dimidx) {
            start[dimidx] = lo[dimidx-1];
            count[dimidx] = hi[dimidx-1] - lo[dimidx-1] + 1;
        }
    }
    else {
        // make a non-participating process a no-op
        fill(start.begin(), start.end(), 0);
        fill(count.begin(), count.end(), 0);
    }

    do_read(tmp, start, count, found_bit, nonblocking);

    if (nonblocking) {
        // can't perform subset until read is finished
        if (needs_subset_record()) {
            get_netcdf_dataset()->arrays_to_pack.push_back(dst);
            get_netcdf_dataset()->vars_to_pack.push_back(this);
        } else {
            get_netcdf_dataset()->arrays_to_pack.push_back(NULL);
            get_netcdf_dataset()->vars_to_pack.push_back(NULL);
        }
    } else {
        // check whether a subset is needed
        if (needs_subset_record()) {
            vector<Mask*> masks = get_masks();
            masks.erase(masks.begin());
            pagoda::pack(tmp, dst, masks);
            delete tmp;
            if (needs_renumber()) {
                renumber(dst);
            }
        }
    }

    // propagate fill value to Array
    if (has_fill_value(record)) {
        dst->set_fill_value(get_fill_value(record));
    }

    return dst;
}


bool PnetcdfVariable::find_bit(const vector<Dimension*> &adims,
                               const vector<int64_t> &lo,
                               const vector<int64_t> &hi) const
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
        if (mask) {
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
    }
#endif

    return found_bit;
}


void PnetcdfVariable::do_read(Array *dst, const vector<MPI_Offset> &start,
                              const vector<MPI_Offset> &count, bool found_bit,
                              bool nonblocking) const
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
            request = ncmpi::iget_vara(ncid, id, start, count, ptr); \
            get_netcdf_dataset()->requests.push_back(request); \
            get_netcdf_dataset()->arrays_to_release.push_back(dst); \
        } else { \
            ncmpi::get_vara_all(ncid, id, start, count, ptr); \
        } \
        if (dst->owns_data() && found_bit && !nonblocking) { \
            dst->release_update(); \
        } \
    } else
    read_var_all(unsigned char, DataType::UCHAR)
    read_var_all(signed char,   DataType::SCHAR)
    read_var_all(char,          DataType::CHAR)
    read_var_all(int,           DataType::INT)
    read_var_all(long,          DataType::LONG)
    read_var_all(float,         DataType::FLOAT)
    read_var_all(double,        DataType::DOUBLE) {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
#undef read_var_all
}


bool PnetcdfVariable::needs_renumber() const
{
    return AbstractVariable::needs_renumber();
}


void PnetcdfVariable::renumber(Array *array) const
{
    AbstractVariable::renumber(array);
}


ostream& PnetcdfVariable::print(ostream &os) const
{
    TIMING("PnetcdfVariable::print(ostream)");
    os << "PnetcdfVariable(" << name << ")";
    return os;
}


PnetcdfDataset* PnetcdfVariable::get_netcdf_dataset() const
{
    TIMING("PnetcdfVariable::get_netcdf_dataset()");
    return dataset;
}


int PnetcdfVariable::get_id() const
{
    TIMING("PnetcdfVariable::get_id()");
    return id;
}
