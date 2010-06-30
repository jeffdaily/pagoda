#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Array.H"
#include "GlobalArray.H"
#include "Timing.H"
#include "Util.H"


Array* Array::create(DataType type, vector<int64_t> shape)
{
    TIMING("Array::create(DataType,vector<int64_t>)");
    return new GlobalArray(type,shape);
}


Array* Array::create(DataType type, vector<Dimension*> dims)
{
    TIMING("Array::create(DataType,vector<Dimension*>)");
    return new GlobalArray(type,dims);
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
    return Util::shape_to_size(get_shape());
}


int64_t Array::get_local_size() const
{
    TIMING("Array::get_local_size()");
    return Util::shape_to_size(get_local_shape());
}


bool Array::same_distribution(const Array *other) const
{
    vector<long> values(1,
            (get_local_shape() == other->get_local_shape()) ? 1 : 0);

    TIMING("Array::same_distribution(Array*)");

    Util::gop_min(values);
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
    vector<int64_t> ld = Util::get_shape(lo,hi);

    ld.erase(ld.begin());

    return get(buffer, lo, hi, ld);
}


void* Array::get(const vector<int64_t> &lo, const vector<int64_t> &hi) const
{
    void *buffer;
    vector<int64_t> ld = Util::get_shape(lo,hi);
    int64_t buffer_size = Util::shape_to_size(ld);
    DataType type = get_type();

    TIMING("Array::get(vector<int64_t>,vector<int64_t>)");

    ld.erase(ld.begin());

#define get_helper(mt,t) \
    if (type == mt) { \
        buffer = new t[buffer_size]; \
    } else
    get_helper(DataType::INT,       int)
    get_helper(DataType::LONG,      long)
    get_helper(DataType::LONGLONG,  long long)
    get_helper(DataType::FLOAT,     float)
    get_helper(DataType::DOUBLE,    double)
    get_helper(DataType::LONGDOUBLE,long double)
    {
        Util::abort("Array::get DataType not handled");
    }
#undef get_helper

    return get(buffer, lo, hi, ld);
}


void Array::put(void *buffer, int64_t lo, int64_t hi)
{
    put(buffer, vector<int64_t>(1,lo), vector<int64_t>(1,hi));
}


void Array::put(void *buffer,
        const vector<int64_t> &lo, const vector<int64_t> &hi)
{
    vector<int64_t> ld = Util::get_shape(lo,hi);

    ld.erase(ld.begin());

    put(buffer, lo, hi, ld);
}


ostream& operator << (ostream &os, const Array *array)
{
    TIMING("operator<<(ostream,Array*)");
    return array->print(os);
}
