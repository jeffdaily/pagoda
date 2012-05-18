#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cassert>
#include <cmath>

#include "AbstractArray.H"
#include "DataType.H"
#include "Error.H"
#include "Validator.H"

template <class L, class R>
static void op_iadd(L *lhs, const R &val, int64_t count,
        Validator *validator, int *tally)
{
    ASSERT(NULL != lhs);
    ASSERT(NULL != validator);
    ASSERT(NULL != tally);
    const L rval = static_cast<L>(val);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        if (validator->is_valid(&lhs[i])) {
            lhs[i] += rval;
            ++tally[i];
        } else {
            lhs[i] = fill_value;
        }
    }
}
template <class L, class R>
static void op_isub(L *lhs, const R &val, int64_t count,
        Validator *validator, int *tally)
{
    ASSERT(NULL != lhs);
    ASSERT(NULL != validator);
    ASSERT(NULL != tally);
    const L rval = static_cast<L>(val);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        if (validator->is_valid(&lhs[i])) {
            lhs[i] -= rval;
            ++tally[i];
        } else {
            lhs[i] = fill_value;
        }
    }
}
template <class L, class R>
static void op_imul(L *lhs, const R &val, int64_t count,
        Validator *validator, int *tally)
{
    ASSERT(NULL != lhs);
    ASSERT(NULL != validator);
    ASSERT(NULL != tally);
    const L rval = static_cast<L>(val);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        if (validator->is_valid(&lhs[i])) {
            lhs[i] *= rval;
            ++tally[i];
        } else {
            lhs[i] = fill_value;
        }
    }
}
template <class L, class R>
static void op_idiv(L *lhs, const R &val, int64_t count,
        Validator *validator, int *tally)
{
    ASSERT(NULL != lhs);
    ASSERT(NULL != validator);
    ASSERT(NULL != tally);
    const L rval = static_cast<L>(val);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        if (validator->is_valid(&lhs[i])) {
            lhs[i] /= rval;
            ++tally[i];
        } else {
            lhs[i] = fill_value;
        }
    }
}
template <class L, class R>
static void op_imax(L *lhs, const R &val, int64_t count,
        Validator *validator, int *tally)
{
    ASSERT(NULL != lhs);
    ASSERT(NULL != validator);
    ASSERT(NULL != tally);
    const L rval = static_cast<L>(val);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        if (validator->is_valid(&lhs[i])) {
            lhs[i] = lhs[i] > rval ? lhs[i] : rval;
            ++tally[i];
        } else {
            lhs[i] = fill_value;
        }
    }
}
template <class L, class R>
static void op_imin(L *lhs, const R &val, int64_t count,
        Validator *validator, int *tally)
{
    ASSERT(NULL != lhs);
    ASSERT(NULL != validator);
    ASSERT(NULL != tally);
    const L rval = static_cast<L>(val);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        if (validator->is_valid(&lhs[i])) {
            lhs[i] = lhs[i] < rval ? lhs[i] : rval;
            ++tally[i];
        } else {
            lhs[i] = fill_value;
        }
    }
}
template <class L, class R>           
static void op_ipow(L *lhs, R exponent, int64_t count,
                    Validator *validator, int *tally)
{
    ASSERT(NULL != lhs);
    ASSERT(NULL != validator);
    ASSERT(NULL != tally);
    double exp = static_cast<double>(exponent);
    const L fill_value = *static_cast<const L*>(validator->get_fill_value());
    for (int64_t i=0; i<count; ++i) {
        if (validator->is_valid(&lhs[i])) {
            lhs[i] = static_cast<L>(std::pow(static_cast<double>(lhs[i]),exp));
            ++tally[i];
        } else {
            lhs[i] = fill_value;
        }
    }
}


void AbstractArray::operate_scalar_validator_counter(const void *rhs_ptr,
                                                     DataType rhs_type,
                                                     const int op)
{
    void *lhs_ptr = access();
    if (NULL != lhs_ptr) {
        DataType lhs_type = get_type();
        int *ctr_buf = static_cast<int*>(counter->access());
        ASSERT(same_distribution(counter));
        ASSERT(NULL != ctr_buf);
#define DATATYPE_EXPAND(DT_L,T_L,DT_R,T_R)                         \
        if (DT_L == lhs_type && DT_R == rhs_type) {                \
            T_L *lhs_buf = static_cast<T_L*>(lhs_ptr);             \
            const T_R rhs_val = *static_cast<const T_R*>(rhs_ptr); \
            switch (op) {                                          \
                case OP_ADD:                                       \
                    op_iadd(lhs_buf, rhs_val, get_local_size(),    \
                            validator, ctr_buf);                   \
                break;                                             \
                case OP_SUB:                                       \
                    op_isub(lhs_buf, rhs_val, get_local_size(),    \
                            validator, ctr_buf);                   \
                break;                                             \
                case OP_MUL:                                       \
                    op_imul(lhs_buf, rhs_val, get_local_size(),    \
                            validator, ctr_buf);                   \
                break;                                             \
                case OP_DIV:                                       \
                    op_idiv(lhs_buf, rhs_val, get_local_size(),    \
                            validator, ctr_buf);                   \
                break;                                             \
                case OP_MAX:                                       \
                    op_imax(lhs_buf, rhs_val, get_local_size(),    \
                            validator, ctr_buf);                   \
                break;                                             \
                case OP_MIN:                                       \
                    op_imin(lhs_buf, rhs_val, get_local_size(),    \
                            validator, ctr_buf);                   \
                break;                                             \
                case OP_POW:                                       \
                    op_ipow(lhs_buf, rhs_val, get_local_size(),    \
                            validator, ctr_buf);                   \
                break;                                             \
                default: ERRCODE("operation not supported", op);   \
            }                                                      \
        } else
#include "DataType2_small.def"
        {
            ERR("triple type not handled");
        }
        counter->release_update();
        release_update();
    }
}
