#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cassert>

#include "AbstractArray.H"
#include "DataType.H"
#include "Error.H"
#include "Validator.H"

template <class L, class R>
static void op_iadd(L *lhs, const R *rhs, int64_t count,
        Validator *validator, int *tally)
{
    ASSERT(lhs != NULL);
    ASSERT(rhs != NULL);
    ASSERT(validator != NULL);
    ASSERT(tally != NULL);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        const L rhs_cast = static_cast<const L>(rhs[i]);
        if (validator->is_valid(&lhs[i]) && validator->is_valid(&rhs_cast)) {
            lhs[i] += rhs_cast;
            ++tally[i];
        } else {
            lhs[i] = fill_value;
        }
    }
}
template <class L, class R>
static void op_isub(L *lhs, const R *rhs, int64_t count,
        Validator *validator, int *tally)
{
    ASSERT(lhs != NULL);
    ASSERT(rhs != NULL);
    ASSERT(validator != NULL);
    ASSERT(tally != NULL);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        const L rhs_cast = static_cast<const L>(rhs[i]);
        if (validator->is_valid(&lhs[i]) && validator->is_valid(&rhs_cast)) {
            lhs[i] -= rhs_cast;
            ++tally[i];
        } else {
            lhs[i] = fill_value;
        }
    }
}
template <class L, class R>
static void op_imul(L *lhs, const R *rhs, int64_t count,
        Validator *validator, int *tally)
{
    ASSERT(lhs != NULL);
    ASSERT(rhs != NULL);
    ASSERT(validator != NULL);
    ASSERT(tally != NULL);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        const L rhs_cast = static_cast<const L>(rhs[i]);
        if (validator->is_valid(&lhs[i]) && validator->is_valid(&rhs_cast)) {
            lhs[i] *= rhs_cast;
            ++tally[i];
        } else {
            lhs[i] = fill_value;
        }
    }
}
template <class L, class R>
static void op_idiv(L *lhs, const R *rhs, int64_t count,
        Validator *validator, int *tally)
{
    ASSERT(lhs != NULL);
    ASSERT(rhs != NULL);
    ASSERT(validator != NULL);
    ASSERT(tally != NULL);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        const L rhs_cast = static_cast<const L>(rhs[i]);
        if (validator->is_valid(&lhs[i]) && validator->is_valid(&rhs_cast)) {
            lhs[i] /= rhs_cast;
            ++tally[i];
        } else {
            lhs[i] = fill_value;
        }
    }
}

template <class L, class R>
static void op_imax(L *lhs, const R *rhs, int64_t count,
        Validator *validator, int *tally)
{
    ASSERT(lhs != NULL);
    ASSERT(rhs != NULL);
    ASSERT(validator != NULL);
    ASSERT(tally != NULL);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        const L rval = static_cast<const L>(rhs[i]);
        if (validator->is_valid(&lhs[i]) && validator->is_valid(&rval)) {
            lhs[i] = lhs[i] > rval ? lhs[i] : rval;
            ++tally[i];
        } else {
            lhs[i] = fill_value;
        }
    }
}


template <class L, class R>
static void op_imin(L *lhs, const R *rhs, int64_t count,
        Validator *validator, int *tally)
{
    ASSERT(lhs != NULL);
    ASSERT(rhs != NULL);
    ASSERT(validator != NULL);
    ASSERT(tally != NULL);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        const L rval = static_cast<const L>(rhs[i]);
        if (validator->is_valid(&lhs[i]) && validator->is_valid(&rval)) {
            lhs[i] = lhs[i] < rval ? lhs[i] : rval;
            ++tally[i];
        } else {
            lhs[i] = fill_value;
        }
    }
}


void AbstractArray::operate_array_validator_counter(const Array *rhs, const int op)
{
    // first, make sure input arrays are compatible
    void *lhs_ptr = access();
    ASSERT(get_shape() == rhs->get_shape());
    ASSERT(same_distribution(counter));
    if (NULL != lhs_ptr) {
        DataType lhs_type = get_type();
        DataType rhs_type = rhs->get_type();
        const void *rhs_ptr = NULL;
        int *ctr_buf = static_cast<int*>(counter->access());
        ASSERT(NULL != ctr_buf);
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
                                     validator, ctr_buf);                 \
                break;                                                    \
                case OP_SUB: op_isub(lhs_buf, rhs_buf, get_local_size(),  \
                                     validator, ctr_buf);                 \
                break;                                                    \
                case OP_MUL: op_imul(lhs_buf, rhs_buf, get_local_size(),  \
                                     validator, ctr_buf);                 \
                break;                                                    \
                case OP_DIV: op_idiv(lhs_buf, rhs_buf, get_local_size(),  \
                                     validator, ctr_buf);                 \
                break;                                                    \
                case OP_MAX: op_imax(lhs_buf, rhs_buf, get_local_size(),  \
                                     validator, ctr_buf);                 \
                break;                                                    \
                case OP_MIN: op_imin(lhs_buf, rhs_buf, get_local_size(),  \
                                     validator, ctr_buf);                 \
                break;                                                    \
                default: ERRCODE("operation not supported", op);          \
            }                                                             \
        } else
#include "DataType2.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", lhs_type);
        }
        counter->release();
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
