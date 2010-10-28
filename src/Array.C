#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Array.H"
#include "GlobalArray.H"
#include "NodeZeroArray.H"
#include "ScalarArray.H"
#include "Timing.H"
#include "Util.H"


Array* Array::create(DataType type, vector<int64_t> shape)
{
    // GlobalArray doesn't handle all types. Those not handled instantiate a
    // NodeZeroArray instead.
    TIMING("Array::create(DataType,vector<int64_t>)");
    if (shape.empty()) {
        return new ScalarArray(type);
    }
    else if (type == DataType::CHAR) {
        return new NodeZeroArray<char>(shape);
    }
    else if (type == DataType::UCHAR) {
        return new NodeZeroArray<unsigned char>(shape);
    }
    else if (type == DataType::SCHAR) {
        return new NodeZeroArray<signed char>(shape);
    }
    else if (type == DataType::SHORT) {
        return new NodeZeroArray<short>(shape);
    }
    return new GlobalArray(type, shape);
}


Array* Array::create(DataType type, vector<Dimension*> dims)
{
    vector<int64_t> shape;

    TIMING("Array::create(DataType,vector<Dimension*>)");

    for (vector<Dimension*>::const_iterator it=dims.begin(), end=dims.end();
            it!=end; ++it) {
        shape.push_back((*it)->get_size());
    }

    if (dims.empty()) {
        return new ScalarArray(type);
    }
    else if (type == DataType::CHAR) {
        return new NodeZeroArray<char>(shape);
    }
    return new GlobalArray(type, shape);
}


Array::Array()
{
    TIMING("Array::Array()");
}


Array::~Array()
{
    TIMING("Array::~Array()");
}


int64_t Array::get_size() const
{
    TIMING("Array::get_size()");
    return pagoda::shape_to_size(get_shape());
}


int64_t Array::get_local_size() const
{
    TIMING("Array::get_local_size()");
    return pagoda::shape_to_size(get_local_shape());
}


bool Array::is_scalar() const
{
    return get_ndim() == 0;
}


bool Array::same_distribution(const Array *other) const
{
    vector<long> values(1,
                        (get_local_shape() == other->get_local_shape()) ? 1 : 0);

    TIMING("Array::same_distribution(Array*)");

    pagoda::gop_min(values);
    return (values.at(0) == 1) ? true : false;
}


void* Array::get(int64_t lo, int64_t hi) const
{
    return get(vector<int64_t>(1,lo),vector<int64_t>(1,hi));
}


void* Array::get(void *buffer, int64_t lo, int64_t hi) const
{
    return get(buffer, vector<int64_t>(1,lo),vector<int64_t>(1,hi));
}


void* Array::get(void *buffer,
                 const vector<int64_t> &lo, const vector<int64_t> &hi) const
{
    vector<int64_t> ld = pagoda::get_shape(lo,hi);

    ld.erase(ld.begin());

    return get(buffer, lo, hi, ld);
}


void* Array::get(const vector<int64_t> &lo, const vector<int64_t> &hi) const
{
    void *buffer;
    vector<int64_t> ld = pagoda::get_shape(lo,hi);
    int64_t buffer_size = pagoda::shape_to_size(ld);
    DataType type = get_type();

    TIMING("Array::get(vector<int64_t>,vector<int64_t>)");

    ld.erase(ld.begin());

#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        buffer = new T[buffer_size]; \
    } else
#include "DataType.def"

    return get(buffer, lo, hi, ld);
}


void Array::put(void *buffer, int64_t lo, int64_t hi)
{
    put(buffer, vector<int64_t>(1,lo), vector<int64_t>(1,hi));
}


void Array::put(void *buffer,
                const vector<int64_t> &lo, const vector<int64_t> &hi)
{
    vector<int64_t> ld = pagoda::get_shape(lo,hi);

    ld.erase(ld.begin());

    put(buffer, lo, hi, ld);
}


ostream& operator << (ostream &os, const Array *array)
{
    TIMING("operator<<(ostream,Array*)");
    return array->print(os);
}


bool Array::broadcast_check(const Array *rhs) const
{
    vector<int64_t> my_shape = get_shape();
    vector<int64_t> rhs_shape = rhs->get_shape();
    vector<int64_t>::reverse_iterator my_it;
    vector<int64_t>::reverse_iterator rhs_it;

    // this Array must have the same or greater rank
    if (my_shape.size() < rhs_shape.size()) {
        return false;
    }

    while (my_shape.size() != rhs_shape.size()) {
        my_shape.erase(my_shape.begin());
    }

    if (my_shape != rhs_shape) {
        return false;
    }

    return true;
}
