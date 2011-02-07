#if HAVE_CONFIG_H
#   include "config.h"
#endif

#include <stdint.h>

#include <cmath>

#include "Debug.H"
#include "Error.H"
#include "ScalarArray.H"


ScalarArray::ScalarArray(DataType type)
    :   AbstractArray(type)
    ,   type(type)
    ,   value(NULL)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        value = static_cast<void*>(new T); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
}


ScalarArray::ScalarArray(const ScalarArray &that)
    :   AbstractArray(that)
    ,   type(that.type)
    ,   value(NULL)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        T *tmp = new T; \
        *tmp = *static_cast<T*>(that.value); \
        value = static_cast<void*>(tmp); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
}


ScalarArray::~ScalarArray()
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        delete static_cast<T*>(value); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
}


DataType ScalarArray::get_type() const
{
    return type;
}


vector<int64_t> ScalarArray::get_shape() const
{
    return vector<int64_t>();
}


vector<int64_t> ScalarArray::get_local_shape() const
{
    return vector<int64_t>();
}


int64_t ScalarArray::get_ndim() const
{
    return 0;
}


void ScalarArray::fill(void *new_value)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        *static_cast<T*>(value) = *static_cast<T*>(new_value); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
}


void ScalarArray::copy(const Array *src)
{
    const ScalarArray *sa_src = dynamic_cast<const ScalarArray*>(src);
    if (type != src->get_type()) {
        ERR("arrays must be same type");
    }
    if (get_shape() != src->get_shape()) {
        ERR("arrays must be same shape");
    }
    if (sa_src) {
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            *static_cast<T*>(value) = *static_cast<T*>(sa_src->value); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    }
}


void ScalarArray::copy(const Array *src, const vector<int64_t> &src_lo, const vector<int64_t> &src_hi, const vector<int64_t> &dst_lo, const vector<int64_t> &dst_hi)
{
    ERR("not implemented ScalarArray::copy");
}


ScalarArray* ScalarArray::clone() const
{
    return new ScalarArray(*this);
}


bool ScalarArray::owns_data() const
{
    return true;
}


void ScalarArray::get_distribution(vector<int64_t> &lo, vector<int64_t> &hi) const
{
    lo.clear();
    hi.clear();
}


void* ScalarArray::access()
{
    return value;
}


const void* ScalarArray::access() const
{
    return value;
}


void ScalarArray::release() const
{
    /** @todo could verify at this point that all values are the same */
}


void ScalarArray::release_update()
{
    /** @todo could verify at this point that all values are the same */
}


void* ScalarArray::get(void *buffer) const
{
    void *ret;

#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        if (buffer == NULL) { \
            ret = static_cast<void*>(new T); \
        } else { \
            ret = buffer; \
        } \
        *static_cast<T*>(ret) = *static_cast<T*>(value); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }

    return ret;
}


void* ScalarArray::get(int64_t lo, int64_t hi, void *buffer) const
{
    ERR("ScalarArray::get(int64_t,int64_t,void*) invalid for ScalarArray");
    //return NULL; // unreachable
}


void* ScalarArray::get(const vector<int64_t> &lo, const vector<int64_t> &hi,
                       void *buffer) const
{
    ASSERT(lo.empty());
    ASSERT(hi.empty());
    return get(buffer);;
}


void ScalarArray::put(void *buffer)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        *static_cast<T*>(value) = *static_cast<T*>(buffer); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
}


void ScalarArray::put(void *buffer, int64_t lo, int64_t hi)
{
    ERR("ScalarArray::put(void*,int64_t,int64_t) invalid for ScalarArray");
}


void ScalarArray::put(void *buffer,
                      const vector<int64_t> &lo, const vector<int64_t> &hi)
{
    ASSERT(lo.empty());
    ASSERT(hi.empty());
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        *static_cast<T*>(value) = *static_cast<T*>(buffer); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
}


void ScalarArray::scatter(void *buffer, vector<int64_t> &subscripts)
{
    ASSERT(subscripts.empty());
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        *static_cast<T*>(value) = *static_cast<T*>(buffer); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
}


void* ScalarArray::gather(vector<int64_t> &subscripts, void *buffer) const
{
    ASSERT(subscripts.empty());
    return get(buffer);
}


Array* ScalarArray::add(const Array *rhs) const
{
    const ScalarArray *array = dynamic_cast<const ScalarArray*>(rhs);
    if (array) {
        ScalarArray *self_copy = new ScalarArray(*this);
        (*self_copy) += *array;
        return self_copy;
    }
    ERR("ScalarArray::add(Array*) fell through");
}


Array* ScalarArray::sub(const Array *rhs) const
{
    const ScalarArray *array = dynamic_cast<const ScalarArray*>(rhs);
    if (array) {
        ScalarArray *self_copy = new ScalarArray(*this);
        (*self_copy) -= *array;
        return self_copy;
    }
    ERR("ScalarArray::sub(Array*) fell through");
}


Array* ScalarArray::mul(const Array *rhs) const
{
    const ScalarArray *array = dynamic_cast<const ScalarArray*>(rhs);
    if (array) {
        ScalarArray *self_copy = new ScalarArray(*this);
        (*self_copy) *= *array;
        return self_copy;
    }
    ERR("ScalarArray::mul(Array*) fell through");
}


Array* ScalarArray::div(const Array *rhs) const
{
    const ScalarArray *array = dynamic_cast<const ScalarArray*>(rhs);
    if (array) {
        ScalarArray *self_copy = new ScalarArray(*this);
        (*self_copy) /= *array;
        return self_copy;
    }
    ERR("ScalarArray::div(Array*) fell through");
}


Array* ScalarArray::max(const Array *rhs) const
{
    ScalarArray *self_copy = new ScalarArray(*this);
    self_copy->imax(rhs);
    return self_copy;
}


Array* ScalarArray::min(const Array *rhs) const
{
    ScalarArray *self_copy = new ScalarArray(*this);
    self_copy->imin(rhs);
    return self_copy;
}


Array* ScalarArray::pow(double exponent) const
{
    ScalarArray *self_copy = new ScalarArray(*this);
    self_copy->ipow(exponent);
    return self_copy;
}


Array* ScalarArray::iadd(const Array *rhs)
{
    const ScalarArray *array = dynamic_cast<const ScalarArray*>(rhs);
    if (array) {
        (*this) += *array;
        return this;
    }
    ERR("ScalarArray::iadd(Array*) fell through");
}


Array* ScalarArray::isub(const Array *rhs)
{
    const ScalarArray *array = dynamic_cast<const ScalarArray*>(rhs);
    if (array) {
        (*this) -= *array;
        return this;
    }
    ERR("ScalarArray::isub(Array*) fell through");
}


Array* ScalarArray::imul(const Array *rhs)
{
    const ScalarArray *array = dynamic_cast<const ScalarArray*>(rhs);
    if (array) {
        (*this) *= *array;
        return this;
    }
    ERR("ScalarArray::imul(Array*) fell through");
}


Array* ScalarArray::idiv(const Array *rhs)
{
    const ScalarArray *array = dynamic_cast<const ScalarArray*>(rhs);
    if (array) {
        (*this) /= *array;
        return this;
    }
    ERR("ScalarArray::idiv(Array*) fell through");
}


Array* ScalarArray::imax(const Array *rhs)
{
    const ScalarArray *array = dynamic_cast<const ScalarArray*>(rhs);
    if (array) {
#define DATATYPE_EXPAND(DT,T) \
        if (type == DT) { \
            T *this_value = (T*)value; \
            T that_value = array->as<T>(); \
            *this_value = (*this_value)>that_value ? *this_value : that_value; \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
        return this;
    }
    ERR("ScalarArray::idiv(Array*) fell through");
}


Array* ScalarArray::imin(const Array *rhs)
{
    const ScalarArray *array = dynamic_cast<const ScalarArray*>(rhs);
    if (array) {
#define DATATYPE_EXPAND(DT,T) \
        if (type == DT) { \
            T *this_value = (T*)value; \
            T that_value = array->as<T>(); \
            *this_value = (*this_value)<that_value ? *this_value : that_value; \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
        return this;
    }
    ERR("ScalarArray::idiv(Array*) fell through");
}


Array* ScalarArray::ipow(double exponent)
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        T *this_value = (T*)value; \
        *this_value = static_cast<T>(std::pow(static_cast<double>(*this_value),exponent)); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
    return this;
}


ScalarArray& ScalarArray::operator+=(const ScalarArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        *((T*)value) += *((T*)that.value); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
    return *this;
}


ScalarArray& ScalarArray::operator-=(const ScalarArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        *((T*)value) -= *((T*)that.value); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
    return *this;
}


ScalarArray& ScalarArray::operator*=(const ScalarArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        *((T*)value) *= *((T*)that.value); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
    return *this;
}


ScalarArray& ScalarArray::operator/=(const ScalarArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        *((T*)value) /= *((T*)that.value); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
    return *this;
}


ostream& ScalarArray::print(ostream &os) const
{
    os << "ScalarArray";
    return os;
}


void ScalarArray::dump() const
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        std::ostringstream os; \
        os << *((T*)value) << std::endl; \
        pagoda::print_zero(os.str()); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
}
