#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cassert>
#include <vector>

using std::vector;

#include "Array.H"
#include "DataType.H"
#include "Dimension.H"
#include "Error.H"
#include "GlobalArray.H"
#include "GlobalScalar.H"
#include "NodeZeroArray.H"
#include "ProcessGroup.H"
#include "ScalarArray.H"
#include "Util.H"


Array* Array::create(DataType type, vector<int64_t> shape)
{
    return Array::create(type, shape, ProcessGroup::get_default());
}


#include "Debug.H"
#include "Print.H"
Array* Array::create(DataType type, vector<int64_t> shape,
        const ProcessGroup &group)
{
    Array *ret = NULL;

    // GlobalArray doesn't handle all types. Those not handled use a different
    // read and write types.
    if (shape.empty()) {
#if 0
        if (type == DataType::CHAR) {
            ret = new GlobalScalar(DataType::INT, group);
            ret->set_read_type(DataType::CHAR);
            ret->set_write_type(DataType::CHAR);
        }
        else if (type == DataType::UCHAR) {
            ret = new GlobalScalar(DataType::INT, group);
            ret->set_write_type(DataType::UCHAR);
        }
        else if (type == DataType::SCHAR) {
            ret = new GlobalScalar(DataType::INT, group);
            ret->set_write_type(DataType::SCHAR);
        }
        else if (type == DataType::SHORT) {
            ret = new GlobalScalar(DataType::INT, group);
            ret->set_write_type(DataType::SHORT);
        } else {
            ret = new GlobalScalar(type, group);
        }
#else
        ret = new ScalarArray(type);
#endif
    }
    else {
        if (type == DataType::CHAR) {
            ret = new GlobalArray(DataType::INT, shape, group);
            ret->set_read_type(DataType::CHAR);
            ret->set_write_type(DataType::CHAR);
        }
        else if (type == DataType::UCHAR) {
            ret = new GlobalArray(DataType::INT, shape, group);
            ret->set_write_type(DataType::UCHAR);
        }
        else if (type == DataType::SCHAR) {
            ret = new GlobalArray(DataType::INT, shape, group);
            ret->set_write_type(DataType::SCHAR);
        }
        else if (type == DataType::SHORT) {
            ret = new GlobalArray(DataType::INT, shape, group);
            ret->set_write_type(DataType::SHORT);
        } else {
            ret = new GlobalArray(type, shape, group);
        }
    }
    ASSERT(NULL != ret);

    return ret;
}


Array* Array::create(DataType type, vector<Dimension*> dims)
{
    return Array::create(type, dims, ProcessGroup::get_default());
}


Array* Array::create(DataType type, vector<Dimension*> dims,
        const ProcessGroup &group)
{
    vector<int64_t> shape;


    for (vector<Dimension*>::const_iterator it=dims.begin(), end=dims.end();
            it!=end; ++it) {
        shape.push_back((*it)->get_size());
    }

    return Array::create(type, shape, group);
}


Array::Array()
{
}


Array::~Array()
{
}


Array* Array::mask_create(const string &name, int64_t size)
{
    vector<int64_t> shape(1, size);
    Array *mask = Array::create(DataType::INT, shape);
    mask->set_name(name);
    mask->reset();
    return mask;
}


Array* Array::mask_create(const Dimension *dim)
{
    return Array::mask_create(dim->get_name(), dim->get_size());
}


Array* Array::mask_create(const vector<int64_t> &shape)
{
    Array *mask = Array::create(DataType::INT, shape);
    mask->reset();
    return mask;
}


ostream& operator << (ostream &os, const Array *array)
{
    return array->print(os);
}
