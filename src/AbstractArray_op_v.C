#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cassert>

#include "AbstractArray.H"
#include "Collectives.H"
#include "DataType.H"
#include "Error.H"
#include "ScalarArray.H"
#include "Validator.H"

template <class L, class R>
static void op_iadd(L *lhs, const R *rhs, int64_t count,
        Validator *validator)
{
    ASSERT(lhs != NULL);
    ASSERT(rhs != NULL);
    ASSERT(validator != NULL);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        const L rhs_cast = static_cast<L>(rhs[i]);
        if (validator->is_valid(&lhs[i]) && validator->is_valid(&rhs_cast)) {
            lhs[i] += rhs_cast;
        } else {
            lhs[i] = fill_value;
        }
    }
}
template <class L, class R>
static void op_isub(L *lhs, const R *rhs, int64_t count,
        Validator *validator)
{
    ASSERT(lhs != NULL);
    ASSERT(rhs != NULL);
    ASSERT(validator != NULL);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        const L rhs_cast = static_cast<L>(rhs[i]);
        if (validator->is_valid(&lhs[i]) && validator->is_valid(&rhs_cast)) {
            lhs[i] -= rhs_cast;
        } else {
            lhs[i] = fill_value;
        }
    }
}
template <class L, class R>
static void op_imul(L *lhs, const R *rhs, int64_t count,
        Validator *validator)
{
    ASSERT(lhs != NULL);
    ASSERT(rhs != NULL);
    ASSERT(validator != NULL);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        const L rhs_cast = static_cast<L>(rhs[i]);
        if (validator->is_valid(&lhs[i]) && validator->is_valid(&rhs_cast)) {
            lhs[i] *= rhs_cast;
        } else {
            lhs[i] = fill_value;
        }
    }
}
template <class L, class R>
static void op_idiv(L *lhs, const R *rhs, int64_t count,
        Validator *validator)
{
    ASSERT(lhs != NULL);
    ASSERT(rhs != NULL);
    ASSERT(validator != NULL);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        const L rhs_cast = static_cast<L>(rhs[i]);
        if (validator->is_valid(&lhs[i]) && validator->is_valid(&rhs_cast)) {
            lhs[i] /= rhs_cast;
        } else {
            lhs[i] = fill_value;
        }
    }
}

template <class L, class R>
static void op_imax(L *lhs, const R *rhs, int64_t count,
        Validator *validator)
{
    ASSERT(lhs != NULL);
    ASSERT(rhs != NULL);
    ASSERT(validator != NULL);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        const L rval = static_cast<L>(rhs[i]);
        if (validator->is_valid(&lhs[i]) && validator->is_valid(&rval)) {
            lhs[i] = lhs[i] > rval ? lhs[i] : rval;
        } else {
            lhs[i] = fill_value;
        }
    }
}
template <class L, class R>
static void op_imin(L *lhs, const R *rhs, int64_t count,
        Validator *validator)
{
    ASSERT(lhs != NULL);
    ASSERT(rhs != NULL);
    ASSERT(validator != NULL);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        const L rval = static_cast<L>(rhs[i]);
        if (validator->is_valid(&lhs[i]) && validator->is_valid(&rval)) {
            lhs[i] = lhs[i] < rval ? lhs[i] : rval;
        } else {
            lhs[i] = fill_value;
        }
    }
}

template <class T>
static void op_reduce_add(const T *buf, int64_t count, Validator *validator,
        double &val, int64_t &tally)
{
    int64_t i;

    ASSERT(buf != NULL);
    ASSERT(validator != NULL);
    tally = 0;

    for (i=0; i<count; ++i) {
        if (validator->is_valid(&buf[i])) {
            val = buf[i];
            ++tally;
            break;
        }
    }
    for (/*empty*/; i<count; ++i) {
        if (validator->is_valid(&buf[i])) {
            val += buf[i];
            ++tally;
        }
    }
}
template <class T>
static void op_reduce_max(const T *buf, int64_t count, Validator *validator,
        double &val)
{
    int64_t i;

    ASSERT(buf != NULL);
    ASSERT(validator != NULL);

    for (i=0; i<count; ++i) {
        if (validator->is_valid(&buf[i])) {
            val = buf[i];
            break;
        }
    }
    for (/*empty*/; i<count; ++i) {
        if (validator->is_valid(&buf[i]) && buf[i] > val) {
            val = buf[i];
        }
    }
}
template <class T>
static void op_reduce_min(const T *buf, int64_t count, Validator *validator,
        double &val)
{
    int64_t i;

    ASSERT(buf != NULL);
    ASSERT(validator != NULL);

    for (i=0; i<count; ++i) {
        if (validator->is_valid(&buf[i])) {
            val = buf[i];
            break;
        }
    }
    for (/*empty*/; i<count; ++i) {
        if (validator->is_valid(&buf[i]) && buf[i] < val) {
            val = buf[i];
        }
    }
}


void AbstractArray::operate_array_validator(const Array *rhs, const int op)
{
    // first, make sure input arrays are compatible
    ASSERT(get_shape() == rhs->get_shape());
    void *lhs_ptr = access();
    if (NULL != lhs_ptr) {
        DataType lhs_type = get_type();
        DataType rhs_type = rhs->get_type();
        const void *rhs_ptr = NULL;
        if (same_distribution(rhs)) {
            rhs_ptr = rhs->access();
        } else {
            vector<int64_t> lo,hi;
            get_distribution(lo,hi);
            rhs_ptr = rhs->get(lo,hi);
        }
#define DATATYPE_EXPAND(DT_L,T_L,DT_R,T_R)                                \
        if (DT_L == lhs_type && DT_R == rhs_type) {                       \
            T_L *lhs_buf = static_cast<T_L*>(lhs_ptr);                    \
            const T_R *rhs_buf = static_cast<const T_R*>(rhs_ptr);        \
            switch (op) {                                                 \
                case OP_ADD: op_iadd(lhs_buf, rhs_buf, get_local_size(),  \
                                     validator);                          \
                break;                                                    \
                case OP_SUB: op_isub(lhs_buf, rhs_buf, get_local_size(),  \
                                     validator);                          \
                break;                                                    \
                case OP_MUL: op_imul(lhs_buf, rhs_buf, get_local_size(),  \
                                     validator);                          \
                break;                                                    \
                case OP_DIV: op_idiv(lhs_buf, rhs_buf, get_local_size(),  \
                                     validator);                          \
                break;                                                    \
                case OP_MAX: op_imax(lhs_buf, rhs_buf, get_local_size(),  \
                                     validator);                          \
                break;                                                    \
                case OP_MIN: op_imin(lhs_buf, rhs_buf, get_local_size(),  \
                                     validator);                          \
                break;                                                    \
                default: ERRCODE("operation not supported", op);          \
            }                                                             \
        } else
#include "DataType2_small.def"
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
                const T_R *a = static_cast<const T_R*>(rhs_ptr); \
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


Array* AbstractArray::operate_reduce_validator(const int op) const
{
    Array *ret = NULL;
    const void *ptr = access();
    DataType type = get_type();
    double val = 0.0;
    int64_t tally = 0;

    if (NULL != ptr) {
        int64_t size = get_local_size();
#define DATATYPE_EXPAND(DT,T)                                        \
        if (DT == type) {                                            \
            const T *buf = static_cast<const T*>(ptr);               \
            switch (op) {                                            \
                case OP_ADD:                                         \
                    op_reduce_add(buf, size, validator, val, tally); \
                break;                                               \
                case OP_MAX:                                         \
                    op_reduce_max(buf, size, validator, val);        \
                break;                                               \
                case OP_MIN:                                         \
                    op_reduce_min(buf, size, validator, val);        \
                break;                                               \
                default: ERRCODE("operation not supported", op);     \
            }                                                        \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
        release();
    }

    switch (op) {
        case OP_ADD:
            pagoda::gop_sum(val);
            pagoda::gop_sum(tally);
            val = val/tally;
            break;
        case OP_MAX:
            pagoda::gop_max(val);
            break;
        case OP_MIN:
            pagoda::gop_min(val);
            break;
        default:
            ERRCODE("operation not supported", op);
    }

    ret = new ScalarArray(type);
    ret->fill_value(val);
    return ret;
}
