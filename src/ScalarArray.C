#if HAVE_CONFIG_H
#   include "config.h"
#endif

#include "Debug.H"
#include "Error.H"
#include "ScalarArray.H"


ScalarArray::ScalarArray(DataType type)
    :   type(type)
    ,   value(NULL)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        value = (void*)(new T); \
    } else
#include "DataType.def"
}


ScalarArray::ScalarArray(const ScalarArray &that)
    :   type(that.type)
    ,   value(NULL)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        value = (void*)(new T); \
        *((T*)value) = *((T*)(that.value)); \
    } else
#include "DataType.def"
}


ScalarArray::~ScalarArray()
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        delete ((T*)value); \
    } else
#include "DataType.def"
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


void ScalarArray::fill(int new_value)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        T cast = new_value; \
        *((T*)value) = cast; \
    } else
#include "DataType.def"
}


void ScalarArray::fill(long new_value)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        T cast = new_value; \
        *((T*)value) = cast; \
    } else
#include "DataType.def"
}


void ScalarArray::fill(long long new_value)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        T cast = new_value; \
        *((T*)value) = cast; \
    } else
#include "DataType.def"
}


void ScalarArray::fill(float new_value)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        T cast = new_value; \
        *((T*)value) = cast ; \
    } else
#include "DataType.def"
}


void ScalarArray::fill(double new_value)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        T cast = new_value; \
        *((T*)value) = cast ; \
    } else
#include "DataType.def"
}


void ScalarArray::fill(long double new_value)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        T cast = new_value; \
        *((T*)value) = cast ; \
    } else
#include "DataType.def"
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
            *((T*)value) = *((T*)(sa_src->value)); \
        } else
#include "DataType.def"
    }
}


void ScalarArray::copy(const Array *src, const vector<int64_t> &src_lo, const vector<int64_t> &src_hi, const vector<int64_t> &dst_lo, const vector<int64_t> &dst_hi)
{
    ERR("not implemented ScalarArray::copy");
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


void* ScalarArray::access() const
{
    return value;
}


void ScalarArray::release() const
{
    // TODO could verify at this point that all values are the same
}


void ScalarArray::release_update()
{
    // TODO could verify at this point that all values are the same
}


void* ScalarArray::get() const
{
    return value;
}


void* ScalarArray::get(void *buffer, const vector<int64_t> &lo, const vector<int64_t> &hi, const vector<int64_t> &ld) const
{
    ASSERT(lo.empty());
    ASSERT(hi.empty());
    ASSERT(ld.empty());
    return value;
}


void ScalarArray::put(void *buffer, const vector<int64_t> &lo, const vector<int64_t> &hi, const vector<int64_t> &ld)
{
    ASSERT(lo.empty());
    ASSERT(hi.empty());
    ASSERT(ld.empty());
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        *((T*)value) = *((T*)(buffer)); \
    } else
#include "DataType.def"
}


void ScalarArray::scatter(void *buffer, vector<int64_t> &subscripts)
{
    ASSERT(subscripts.empty());
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        *((T*)value) = *((T*)(buffer)); \
    } else
#include "DataType.def"
}


void* ScalarArray::gather(vector<int64_t> &subscripts) const
{
    ASSERT(subscripts.empty());
    return value;
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


ScalarArray& ScalarArray::operator+=(const ScalarArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        *((T*)value) += *((T*)that.value); \
    } else
#include "DataType.def"
}


ScalarArray& ScalarArray::operator-=(const ScalarArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        *((T*)value) -= *((T*)that.value); \
    } else
#include "DataType.def"
}


ScalarArray& ScalarArray::operator*=(const ScalarArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        *((T*)value) *= *((T*)that.value); \
    } else
#include "DataType.def"
}


ScalarArray& ScalarArray::operator/=(const ScalarArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        *((T*)value) /= *((T*)that.value); \
    } else
#include "DataType.def"
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
}
