#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <ga.h>

#include "Array.H"
#include "GlobalArray.H"
#include "Timing.H"
#include "Util.H"


Array* Array::create(DataType type, vector<int64_t> shape)
{
    return new GlobalArray(type,shape);
}


Array* Array::create(DataType type, vector<Dimension*> dims)
{
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
    return Util::shape_to_size(get_shape());
}


int64_t Array::get_local_size() const
{
    return Util::shape_to_size(get_local_shape());
}


bool Array::same_distribution(const Array *other)
{
    long val = (get_local_shape() == other->get_local_shape()) ? 1 : 0;
    GA_Lgop(&val, 1, "min");
    return (val == 1) ? true : false;
}


ostream& operator << (ostream &os, const Array *array)
{
    TIMING("operator<<(ostream,Array*)");
    return array->print(os);
}
