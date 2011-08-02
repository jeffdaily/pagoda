#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cassert>

#include "AbstractArray.H"
#include "Array.H"
#include "Copy.H"
#include "DataType.H"
#include "Error.H"
#include "Util.H"
#include "Validator.H"


AbstractArray::AbstractArray(DataType type)
    :   Array()
    ,   validator(NULL)
    ,   counter(NULL)
    ,   write_type(type)
    ,   read_type(type)
{
}


AbstractArray::AbstractArray(const AbstractArray &that)
    :   Array()
    ,   validator(that.validator == NULL ? NULL : that.validator->clone())
    ,   counter(that.counter == NULL ? NULL : that.counter->clone())
    ,   write_type(that.write_type)
    ,   read_type(that.write_type)
{
    /** @todo should counter be deep copied? */
}


AbstractArray::~AbstractArray()
{
    // delete validator since it is an inherent property acquired from the
    // associated Variable
    if (NULL != validator) {
        delete validator;
    }
    // don't delete counter; they are transient and set by caller
}


DataType AbstractArray::get_write_type() const
{
    return write_type;
}


void AbstractArray::set_write_type(DataType type)
{
    write_type = type;
}


DataType AbstractArray::get_read_type() const
{
    return read_type;
}


void AbstractArray::set_read_type(DataType type)
{
    read_type = type;
}


int64_t AbstractArray::get_size() const
{
    return pagoda::shape_to_size(get_shape());
}


vector<int64_t> AbstractArray::get_local_shape() const
{
    if (owns_data()) {
        vector<int64_t> local_shape(get_ndim());
        vector<int64_t> lo;
        vector<int64_t> hi;

        get_distribution(lo, hi);
        for (int64_t i=0,limit=get_ndim(); i<limit; ++i) {
            local_shape[i] = hi[i]-lo[i]+1;
        }
        return local_shape;
    }

    return vector<int64_t>();
}


int64_t AbstractArray::get_local_size() const
{
    return pagoda::shape_to_size(get_local_shape());
}


int64_t AbstractArray::get_ndim() const
{
    return static_cast<int64_t>(get_shape().size());
}


bool AbstractArray::is_scalar() const
{
    return get_ndim() == 0;
}


void AbstractArray::fill(void *value)
{
    void *data = access();
    DataType type = get_type();

    if (NULL != data) {
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            T tvalue = *static_cast<T*>(value); \
            T *tdata =  static_cast<T*>(data); \
            for (int64_t i=0,limit=get_local_size(); i<limit; ++i) { \
                tdata[i] = tvalue; \
            } \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
        release_update();
    }
}


void AbstractArray::copy(const Array *src)
{
    /** @todo TODO implement */
    ERR("Not implemented");
#if 0
    void *data = access();

    ASSERT(same_distribution(src));
    ASSERT(get_type() == src->get_type());
    if (NULL != data) {

        release_update();
    }
#endif
}


Array* AbstractArray::cast(DataType new_type) const
{
    DataType type = get_type();
    vector<int64_t> shape = get_shape();
    Array *dst_array = NULL;

    if (type == new_type) {
        dst_array = clone();
    }
    else {
        // types are different, so this is a cast
        dst_array = Array::create(new_type, shape);
        if (dst_array->owns_data()) {
            vector<int64_t> lo;
            vector<int64_t> hi;
            void *dst_data = NULL;
            const void *src_data = NULL;
            DataType type_dst = dst_array->get_type();
            dst_array->get_distribution(lo, hi);
            if (same_distribution(dst_array)) {
                src_data = access();
            } else {
                src_data = get(lo, hi);
            }
            dst_data = dst_array->access();
#define DATATYPE_EXPAND(src_mt,src_t,dst_mt,dst_t) \
            if (type == src_mt && type_dst == dst_mt) { \
                const src_t *src = static_cast<const src_t*>(src_data); \
                dst_t *dst = static_cast<dst_t*>(dst_data); \
                pagoda::copy_cast<dst_t>(src,src+get_size(),dst); \
            } else
#include "DataType2.def"
            {
                EXCEPT(DataTypeException, "DataType not handled", type);
            }
            dst_array->release_update();
            if (same_distribution(dst_array)) {
                release();
            } else {
#define DATATYPE_EXPAND(mt,t) \
                if (type_dst == mt) { \
                    const t *src = static_cast<const t*>(src_data); \
                    delete [] src; \
                } else 
#include "DataType.def"
                {
                    EXCEPT(DataTypeException, "DataType not handled", type_dst);
                }
            }
        }
    }

    return dst_array;
}


void* AbstractArray::get(void *buffer) const
{
    vector<int64_t> my_shape = get_shape();
    vector<int64_t> lo(0, my_shape.size());
    vector<int64_t> hi(my_shape.begin(), my_shape.end());

    // indexing is inclusive i.e. shape is 1 too big
    std::for_each(hi.begin(), hi.end(), std::bind2nd(std::minus<int64_t>(),1));

    return get(lo, hi, buffer);
}


void* AbstractArray::get(int64_t lo, int64_t hi, void *buffer) const
{
    return get(vector<int64_t>(1,lo), vector<int64_t>(1,hi), buffer);
}


void AbstractArray::put(void *buffer)
{
    vector<int64_t> my_shape = get_shape();
    vector<int64_t> lo(0, my_shape.size());
    vector<int64_t> hi(my_shape.begin(), my_shape.end());

    // indexing is inclusive i.e. shape is 1 too big
    std::for_each(hi.begin(), hi.end(), std::bind2nd(std::minus<int64_t>(),1));

    put(buffer, lo, hi);
}


void AbstractArray::put(void *buffer, int64_t lo, int64_t hi)
{
    put(buffer, vector<int64_t>(1,lo), vector<int64_t>(1,hi));
}


void AbstractArray::acc(void *buffer, void *alpha)
{
    vector<int64_t> my_shape = get_shape();
    vector<int64_t> lo(0, my_shape.size());
    vector<int64_t> hi(my_shape.begin(), my_shape.end());

    // indexing is inclusive i.e. shape is 1 too big
    std::for_each(hi.begin(), hi.end(), std::bind2nd(std::minus<int64_t>(),1));

    acc(buffer, lo, hi, alpha);
}


void AbstractArray::acc(void *buffer, int64_t lo, int64_t hi, void *alpha)
{
    acc(buffer, vector<int64_t>(1,lo), vector<int64_t>(1,hi), alpha);
}


Array* AbstractArray::add(const Array *rhs) const
{
    Array *array = clone();
    array->iadd(rhs);
    return array;
}


Array* AbstractArray::sub(const Array *rhs) const
{
    Array *array = clone();
    array->isub(rhs);
    return array;
}


Array* AbstractArray::mul(const Array *rhs) const
{
    Array *array = clone();
    array->imul(rhs);
    return array;
}


Array* AbstractArray::div(const Array *rhs) const
{
    Array *array = clone();
    array->idiv(rhs);
    return array;
}


Array* AbstractArray::max(const Array *rhs) const
{
    Array *array = clone();
    array->imax(rhs);
    return array;
}


Array* AbstractArray::min(const Array *rhs) const
{
    Array *array = clone();
    array->imin(rhs);
    return array;
}


Array* AbstractArray::pow(double exponent) const
{
    Array *array = clone();
    array->ipow(exponent);
    return array;
}


Array* AbstractArray::iadd(const Array *rhs)
{
    operate(rhs, OP_ADD);
    return this;
}


Array* AbstractArray::isub(const Array *rhs)
{
    operate(rhs, OP_SUB);
    return this;
}


Array* AbstractArray::imul(const Array *rhs)
{
    operate(rhs, OP_MUL);
    return this;
}


Array* AbstractArray::idiv(const Array *rhs)
{
    operate(rhs, OP_DIV);
    return this;
}


Array* AbstractArray::imax(const Array *rhs)
{
    operate(rhs, OP_MAX);
    return this;
}


Array* AbstractArray::imin(const Array *rhs)
{
    operate(rhs, OP_MIN);
    return this;
}


Array* AbstractArray::ipow(double exponent)
{
    if (NULL != validator && NULL != counter) {
        operate_scalar_validator_counter(&exponent, DataType::DOUBLE, OP_POW);
    } else if (NULL != validator) {
        operate_scalar_validator(&exponent, DataType::DOUBLE, OP_POW);
    } else {
        operate_scalar(&exponent, DataType::DOUBLE, OP_POW);
    }
    return this;
}


bool AbstractArray::same_distribution(const Array *other) const
{
    vector<long> values(
            1, (get_local_shape() == other->get_local_shape()) ? 1 : 0);


    pagoda::gop_min(values);
    return (values.at(0) == 1) ? true : false;
}


bool AbstractArray::owns_data() const
{
    return get_local_size() > 0;
}


void AbstractArray::set_validator(Validator *validator)
{
    this->validator = validator;
}


bool AbstractArray::has_validator() const
{
    return NULL != validator;
}


Validator* AbstractArray::get_validator() const
{
    return validator;
}


void AbstractArray::set_counter(Array *counter)
{
    this->counter = counter;
}


ostream& AbstractArray::print(ostream &os) const
{
    os << "AbstractArray";
    return os;
}


bool AbstractArray::broadcast_check(const Array *rhs) const
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


void AbstractArray::operate(const Array *rhs, const int op)
{
    if (rhs->is_scalar()) {
        void *val = rhs->get();
        DataType type = rhs->get_type();
        if (NULL != validator && NULL != counter) {
            operate_scalar_validator_counter(val, type, op);
        } else if (NULL != validator) {
            operate_scalar_validator(val, type, op);
        } else {
            operate_scalar(val, type, op);
        }
        // clean up
#define DATATYPE_EXPAND(DT,T)               \
        if (type == DT) {                   \
            T *aval = static_cast<T*>(val); \
            delete [] aval;                 \
        } else 
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    } else if (get_shape() == rhs->get_shape()) {
        if (NULL != validator && NULL != counter) {
            operate_array_validator_counter(rhs, op);
        } else if (NULL != validator) {
            operate_array_validator(rhs, op);
        } else {
            operate_array(rhs, op);
        }
    } else if (broadcast_check(rhs)) {
        ERR("operate_array_broadcast not implemented");
    }
}
