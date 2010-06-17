#if HAVE_CONFIG_H
#   include <config.h>
#endif

#if HAVE_GA
#   include <ga.h>
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


bool Array::same_distribution(const Array *other)
{
    vector<long> values(1,
            (get_local_shape() == other->get_local_shape()) ? 1 : 0);

    TIMING("Array::same_distribution(Array*)");

    Util::gop_min(values);
    return (values.at(0) == 1) ? true : false;
}


ostream& operator << (ostream &os, const Array *array)
{
    TIMING("operator<<(ostream,Array*)");
    return array->print(os);
}
