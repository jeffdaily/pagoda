#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <netcdf.h>

#include <algorithm>
#include <cassert>

#include "Array.H"
#include "Attribute.H"
#include "Common.H"
#include "Mask.H"
#include "Netcdf4Attribute.H"
#include "Netcdf4Dataset.H"
#include "Netcdf4Dimension.H"
#include "Netcdf4Error.H"
#include "Netcdf4Variable.H"
#include "Pack.H"
#include "Netcdf4.H"
#include "Util.H"

using std::fill;


Netcdf4Variable::Netcdf4Variable(Netcdf4Dataset *dataset, int varid)
    :   AbstractVariable()
    ,   dataset(dataset)
    ,   id(varid)
    ,   name("")
    ,   dims()
    ,   atts()
    ,   type(DataType::NOT_A_TYPE)
{
    int ncid = dataset->get_id();
    vector<int> dim_ids;
    nc_type type_tmp;
    int natt;
    nc::inq_var(ncid, varid, name, type_tmp, dim_ids, natt);
    type = Netcdf4Dataset::to_dt(type_tmp);
    for (size_t dimidx=0; dimidx<dim_ids.size(); ++dimidx) {
        dims.push_back(dataset->get_dim(dim_ids[dimidx]));
    }
    for (int attid=0; attid<natt; ++attid) {
        atts.push_back(new Netcdf4Attribute(dataset, attid, this));
    }
}


Netcdf4Variable::~Netcdf4Variable()
{

    transform(atts.begin(), atts.end(), atts.begin(),
              pagoda::ptr_deleter<Netcdf4Attribute*>);
}


string Netcdf4Variable::get_name() const
{
    return name;
}


vector<Dimension*> Netcdf4Variable::get_dims() const
{
    return vector<Dimension*>(dims.begin(), dims.end());
}


vector<Attribute*> Netcdf4Variable::get_atts() const
{
    return vector<Attribute*>(atts.begin(), atts.end());
}


Dataset* Netcdf4Variable::get_dataset() const
{
    return dataset;
}


DataType Netcdf4Variable::get_type() const
{
    return type;
}


Array* Netcdf4Variable::read(Array *dst) const
{
    if (dst == NULL) {
        return AbstractVariable::read_alloc();
    }
    return read_wrapper(dst);
}


Array* Netcdf4Variable::iread(Array *dst)
{
    return read(dst);
}


Array* Netcdf4Variable::read_wrapper(Array *dst) const
{
    int64_t ndim = get_ndim();
    vector<int64_t> lo(ndim);
    vector<int64_t> hi(ndim);
    vector<size_t> start(ndim);
    vector<size_t> count(ndim);
    bool found_bit = true;
    Array *tmp;


    // if we are subsetting, then the passed in array is different than the
    // one in which the data is read into i.e. subset occurs after the fact
    if (needs_subset()) {
        get_dataset()->push_masks(NULL);
        tmp = Array::create(type, get_shape());
        assert(NULL != tmp);
        get_dataset()->pop_masks();
    }
    else {
        assert(NULL != dst);
        tmp = dst;
    }

    tmp->get_distribution(lo,hi);

    if (tmp->get_ndim() != ndim) {
        pagoda::abort("Netcdf4Variable::read(Array*) :: shape mismatch");
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

    do_read(tmp, start, count, found_bit);

    // check whether a subset is needed
    if (needs_subset()) {
        pagoda::pack(tmp, dst, get_masks());
        delete tmp;
        if (needs_renumber()) {
            renumber(dst);
        }
    }

    // propagate fill value to Array
    if (has_validator()) {
        dst->set_validator(get_validator());
    }

    return dst;
}


Array* Netcdf4Variable::read(int64_t record, Array *dst) const
{
    if (dst == NULL) {
        return AbstractVariable::read_alloc(record);
    }
    return read_wrapper(record, dst);
}


Array* Netcdf4Variable::iread(int64_t record, Array *dst)
{
    return read(record, dst);
}


Array* Netcdf4Variable::read_wrapper(int64_t record, Array *dst) const
{
    int64_t ndim = get_ndim();
    vector<int64_t> lo(ndim);
    vector<int64_t> hi(ndim);
    vector<size_t> start(ndim);
    vector<size_t> count(ndim);
    vector<Dimension*> adims(dims.begin()+1,dims.end());
    bool found_bit = true;
    Array *tmp;


    // if we are subsetting, then the passed in array is different than the
    // one in which the data is read into i.e. subset occurs after the fact
    if (needs_subset_record()) {
        vector<int64_t> shape;
        get_dataset()->push_masks(NULL);
        shape = get_shape();
        get_dataset()->pop_masks();
        shape.erase(shape.begin());
        tmp = Array::create(type, shape);
        assert(NULL != tmp);
    }
    else {
        assert(NULL != dst);
        tmp = dst;
    }

    tmp->get_distribution(lo,hi);

    if (tmp->get_ndim()+1 != ndim) {
        pagoda::abort("Netcdf4Variable::read(int64_t,Array*) :: shape mismatch");
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

    do_read(tmp, start, count, found_bit);

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

    // propagate fill value to Array
    if (has_validator(record)) {
        dst->set_validator(get_validator(record));
    }

    return dst;
}


bool Netcdf4Variable::find_bit(const vector<Dimension*> &adims,
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


void Netcdf4Variable::do_read(Array *dst, const vector<size_t> &start,
        const vector<size_t> &count, bool found_bit) const
{
    int ncid = get_netcdf_dataset()->get_id();
    DataType type = dst->get_type();

#define read_var(TYPE, DT) \
    if (type == DT) { \
        TYPE *ptr = NULL; \
        if (dst->owns_data() && found_bit) { \
            ptr = static_cast<TYPE*>(dst->access()); \
        } \
        nc::get_vara(ncid, id, start, count, ptr); \
        if (dst->owns_data() && found_bit) { \
            dst->release_update(); \
        } \
    } else
    read_var(unsigned char, DataType::UCHAR)
    read_var(signed char,   DataType::SCHAR)
    read_var(char,          DataType::CHAR)
    read_var(int,           DataType::INT)
    read_var(long,          DataType::LONG)
    read_var(float,         DataType::FLOAT)
    read_var(double,        DataType::DOUBLE) {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
#undef read_var
}


bool Netcdf4Variable::needs_renumber() const
{
    return AbstractVariable::needs_renumber();
}


void Netcdf4Variable::renumber(Array *array) const
{
    AbstractVariable::renumber(array);
}


ostream& Netcdf4Variable::print(ostream &os) const
{
    os << "Netcdf4Variable(" << name << ")";
    return os;
}


Netcdf4Dataset* Netcdf4Variable::get_netcdf_dataset() const
{
    return dataset;
}


int Netcdf4Variable::get_id() const
{
    return id;
}
