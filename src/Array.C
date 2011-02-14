#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cassert>

#include "Array.H"
#include "GlobalArray.H"
#include "NodeZeroArray.H"
#include "ScalarArray.H"
#include "Timing.H"
#include "Util.H"


Array* Array::create(DataType type, vector<int64_t> shape)
{
    Array *ret = NULL;


    // GlobalArray doesn't handle all types. Those not handled use a different
    // read and write types.
    if (shape.empty()) {
        ret = new ScalarArray(type);
    }
    else if (type == DataType::CHAR) {
        ret = new GlobalArray(DataType::INT, shape);
        ret->set_read_type(DataType::CHAR);
        ret->set_write_type(DataType::CHAR);
    }
    else if (type == DataType::UCHAR) {
        ret = new GlobalArray(DataType::INT, shape);
        ret->set_write_type(DataType::UCHAR);
    }
    else if (type == DataType::SCHAR) {
        ret = new GlobalArray(DataType::INT, shape);
        ret->set_write_type(DataType::SCHAR);
    }
    else if (type == DataType::SHORT) {
        ret = new GlobalArray(DataType::INT, shape);
        ret->set_write_type(DataType::SHORT);
    } else {
        ret = new GlobalArray(type, shape);
    }
    assert(NULL != ret);

    return ret;
}


Array* Array::create(DataType type, vector<Dimension*> dims)
{
    vector<int64_t> shape;


    for (vector<Dimension*>::const_iterator it=dims.begin(), end=dims.end();
            it!=end; ++it) {
        shape.push_back((*it)->get_size());
    }

    return Array::create(type, shape);
}


Array::Array()
{
}


Array::~Array()
{
}


ostream& operator << (ostream &os, const Array *array)
{
    return array->print(os);
}
