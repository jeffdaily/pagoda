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


ostream& operator << (ostream &os, const Array *array)
{
    TIMING("operator<<(ostream,Array*)");
    return array->print(os);
}
