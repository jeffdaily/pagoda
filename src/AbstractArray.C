#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cassert>

#include "AbstractArray.H"
#include "Array.H"
#include "Copy.H"
#include "DataType.H"
#include "Timing.H"
#include "Util.H"
#include "Validator.H"


AbstractArray::AbstractArray(DataType type)
    :   Array()
    ,   validator(NULL)
    ,   counter(NULL)
    ,   write_type(type)
    ,   read_type(type)
{
    TIMING("AbstractArray::AbstractArray()");
}


AbstractArray::AbstractArray(const AbstractArray &that)
    :   Array()
    ,   validator(that.validator->clone())
    ,   counter(that.counter->clone())
    ,   write_type(that.write_type)
    ,   read_type(that.write_type)
{
    TIMING("AbstractArray::AbstractArray()");
    /** @todo should counter be deep copied? */
}


AbstractArray::~AbstractArray()
{
    TIMING("AbstractArray::~AbstractArray()");
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
    TIMING("AbstractArray::get_size()");
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
    TIMING("AbstractArray::get_local_size()");
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
    void *data = access();

    ASSERT(same_distribution(src));
    ASSERT(get_type() == src->get_type());
    if (NULL != data) {

        release_update();
    }
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
            void *src_data = NULL;
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
                src_t *src = static_cast<src_t*>(src_data); \
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
                    t *src = static_cast<t*>(src_data); \
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
    operate_scalar(&exponent, DataType::DOUBLE, OP_POW);
    return this;
}


bool AbstractArray::same_distribution(const Array *other) const
{
    vector<long> values(
            1, (get_local_shape() == other->get_local_shape()) ? 1 : 0);

    TIMING("AbstractArray::same_distribution(Array*)");

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
    TIMING("AbstractArray::print(ostream&)");
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
    if (rhs->get_ndim() == 0) {
        void *val = rhs->get();
        operate_scalar(val, rhs->get_type(), op);
    } else if (broadcast_check(rhs)) {
        ERR("operate_array_broadcast not implemented");
    } else {
        operate_array(rhs, op);
    }
}


template <class L, class R>
static void op_iadd(L *lhs, R *rhs, int64_t count)
{
    for (int64_t i=0; i<count; ++i) {
        lhs[i] += static_cast<L>(rhs[i]);
    }
}


template <class L, class R>
static void op_isub(L *lhs, R *rhs, int64_t count)
{
    for (int64_t i=0; i<count; ++i) {
        lhs[i] -= static_cast<L>(rhs[i]);
    }
}


template <class L, class R>
static void op_imul(L *lhs, R *rhs, int64_t count)
{
    for (int64_t i=0; i<count; ++i) {
        lhs[i] *= static_cast<L>(rhs[i]);
    }
}


template <class L, class R>
static void op_idiv(L *lhs, R *rhs, int64_t count)
{
    for (int64_t i=0; i<count; ++i) {
        lhs[i] /= static_cast<L>(rhs[i]);
    }
}


template <class L, class R>
static void op_imax(L *lhs, R *rhs, int64_t count)
{
    for (int64_t i=0; i<count; ++i) {
        L rval = static_cast<L>(rhs[i]);
        lhs[i] = lhs[i]>rval ? lhs[i] : rval;
    }
}


template <class L, class R>
static void op_imin(L *lhs, R *rhs, int64_t count)
{
    for (int64_t i=0; i<count; ++i) {
        L rval = static_cast<L>(rhs[i]);
        lhs[i] = lhs[i]<rval ? lhs[i] : rval;
    }
}


void AbstractArray::operate_array(const Array *rhs, const int op)
{
    // first, make sure input arrays are compatible
    if (!broadcast_check(rhs)) {
        ERR("incompatible arrays for operation");
    }
    void *lhs_ptr = access();
    if (NULL != lhs_ptr) {
        DataType lhs_type = get_type();
        DataType rhs_type = rhs->get_type();
        void *rhs_ptr = NULL;
        if (same_distribution(rhs)) {
            rhs_ptr = rhs->access();
        } else {
            vector<int64_t> lo,hi;
            get_distribution(lo,hi);
            rhs_ptr = rhs->get(lo,hi);
        }
#define DATATYPE_EXPAND(DT_L,T_L,DT_R,T_R) \
        if (DT_L == lhs_type && DT_R == rhs_type) { \
            T_L *lhs_buf = static_cast<T_L*>(lhs_ptr); \
            T_R *rhs_buf = static_cast<T_R*>(rhs_ptr); \
            switch (op) { \
                case OP_ADD: op_iadd(lhs_buf, rhs_buf, get_local_size()); \
                             break; \
                case OP_SUB: op_isub(lhs_buf, rhs_buf, get_local_size()); \
                             break; \
                case OP_MUL: op_imul(lhs_buf, rhs_buf, get_local_size()); \
                             break; \
                case OP_DIV: op_idiv(lhs_buf, rhs_buf, get_local_size()); \
                             break; \
                case OP_MAX: op_imax(lhs_buf, rhs_buf, get_local_size()); \
                             break; \
                case OP_MIN: op_imin(lhs_buf, rhs_buf, get_local_size()); \
                             break; \
                default: ERRCODE("operation not supported", op); \
            } \
        } else
#include "DataType2.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", lhs_type);
        }
        // clean up
        if (same_distribution(rhs)) {
            rhs->release();
        } else {
            DataType rhs_type = rhs->get_type();
#define DATATYPE_EXPAND(DT_R,T_R) \
            if (rhs_type == DT_R) { \
                T_R *a = static_cast<T_R*>(rhs_ptr); \
                delete [] a; \
            } else 
#include "DataType.def"
            {
                EXCEPT(DataTypeException, "DataType not handled", rhs_type);
            }
        }
        release_update();
    }
}


// too complicated to implement now, see if need arises in future
#if 0
void AbstractArray::operate_array_broadcast(const Array *rhs, const int op)
{
    void *lhs_ptr = access();
    if (NULL != lhs_ptr) {
        DataType lhs_type = get_type();
        DataType rhs_type = rhs->get_type();
        void *rhs_ptr = rhs->get();
#define DATATYPE_EXPAND(DT_L,T_L,DT_R,T_R) \
        if (DT_L == lhs_type && DT_R == rhs_type) { \
            T_L *lhs_buf = static_cast<T_L*>(lhs_ptr); \
            T_R *rhs_buf = static_cast<T_R*>(rhs_ptr); \
            switch (op) { \
                case OP_ADD: op_iadd(lhs_buf, rhs_buf, get_local_size()); \
                             break; \
                case OP_SUB: op_isub(lhs_buf, rhs_buf, get_local_size()); \
                             break; \
                case OP_MUL: op_imul(lhs_buf, rhs_buf, get_local_size()); \
                             break; \
                case OP_DIV: op_idiv(lhs_buf, rhs_buf, get_local_size()); \
                             break; \
                case OP_MAX: op_imax(lhs_buf, rhs_buf, get_local_size()); \
                             break; \
                case OP_MIN: op_imin(lhs_buf, rhs_buf, get_local_size()); \
                             break; \
                default: ERRCODE("operation not supported", op); \
            } \
        } else
#include "DataType2.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", lhs_type);
        }
        // clean up
        if (same_distribution(rhs)) {
            rhs->release();
        } else {
            DataType rhs_type = rhs->get_type();
#define DATATYPE_EXPAND(DT_R,T_R) \
            if (rhs_type == DT_R) { \
                T_R *a = static_cast<T_R*>(rhs_ptr); \
                delete [] a; \
            } else 
#include "DataType.def"
            {
                EXCEPT(DataTypeException, "DataType not handled", rhs_type);
            }
        }
        release_update();
    }
}
#endif


template <class L, class R>
static void op_iadd(L *lhs, R val, int64_t count)
{
    L rval = static_cast<L>(val);
    for (int64_t i=0; i<count; ++i) {
        lhs[i] += rval;
    }
}


template <class L, class R>
static void op_isub(L *lhs, R val, int64_t count)
{
    L rval = static_cast<L>(val);
    for (int64_t i=0; i<count; ++i) {
        lhs[i] -= rval;
    }
}


template <class L, class R>
static void op_imul(L *lhs, R val, int64_t count)
{
    L rval = static_cast<L>(val);
    for (int64_t i=0; i<count; ++i) {
        lhs[i] *= rval;
    }
}


template <class L, class R>
static void op_idiv(L *lhs, R val, int64_t count)
{
    L rval = static_cast<L>(val);
    for (int64_t i=0; i<count; ++i) {
        lhs[i] /= rval;
    }
}


template <class L, class R>
static void op_imax(L *lhs, R val, int64_t count)
{
    L rval = static_cast<L>(val);
    for (int64_t i=0; i<count; ++i) {
        lhs[i] = lhs[i]>rval ? lhs[i] : rval;
    }
}


template <class L, class R>
static void op_imin(L *lhs, R val, int64_t count)
{
    L rval = static_cast<L>(val);
    for (int64_t i=0; i<count; ++i) {
        lhs[i] = lhs[i]<rval ? lhs[i] : rval;
    }
}


template <class L, class R>
static void op_ipow(L *lhs, R exponent, int64_t count)
{
    double exp = static_cast<double>(exponent);
    for (int64_t i=0; i<count; ++i) {
        lhs[i] = static_cast<L>(std::pow(static_cast<double>(lhs[i]),exp));
    }
}


void AbstractArray::operate_scalar(void *rhs_ptr, DataType rhs_type,
                                   const int op)
{
    void *lhs_ptr = access();
    if (NULL != lhs_ptr) {
        DataType lhs_type = get_type();
#define DATATYPE_EXPAND(DT_L,T_L,DT_R,T_R) \
        if (DT_L == lhs_type && DT_R == rhs_type) { \
            T_L *lhs_buf =  static_cast<T_L*>(lhs_ptr); \
            T_R  rhs_val = *static_cast<T_R*>(rhs_ptr); \
            switch (op) { \
                case OP_ADD: op_iadd(lhs_buf, rhs_val, get_local_size()); \
                             break; \
                case OP_SUB: op_isub(lhs_buf, rhs_val, get_local_size()); \
                             break; \
                case OP_MUL: op_imul(lhs_buf, rhs_val, get_local_size()); \
                             break; \
                case OP_DIV: op_idiv(lhs_buf, rhs_val, get_local_size()); \
                             break; \
                case OP_MAX: op_imax(lhs_buf, rhs_val, get_local_size()); \
                             break; \
                case OP_MIN: op_imin(lhs_buf, rhs_val, get_local_size()); \
                             break; \
                case OP_POW: op_ipow(lhs_buf, rhs_val, get_local_size()); \
                             break; \
                default: ERRCODE("operation not supported", op); \
            } \
        } else
#include "DataType2.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", lhs_type);
        }
        // clean up
#define DATATYPE_EXPAND(DT_R,T_R) \
        if (rhs_type == DT_R) { \
            T_R *a = static_cast<T_R*>(rhs_ptr); \
            delete a; \
        } else 
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", rhs_type);
        }
        release_update();
    }
}
