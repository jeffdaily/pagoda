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
static inline void op_iadd(L *lhs, const R &val, int64_t count)
{
    ASSERT(NULL != lhs);
    const L rval = static_cast<L>(val);
    for (int64_t i=0; i<count; ++i) {
        lhs[i] += rval;
    }
}
template <class L, class R>
static inline void op_isub(L *lhs, const R &val, int64_t count)
{
    ASSERT(NULL != lhs);
    const L rval = static_cast<L>(val);
    for (int64_t i=0; i<count; ++i) {
        lhs[i] -= rval;
    }
}
template <class L, class R>
static inline void op_imul(L *lhs, const R &val, int64_t count)
{
    ASSERT(NULL != lhs);
    const L rval = static_cast<L>(val);
    for (int64_t i=0; i<count; ++i) {
        lhs[i] *= rval;
    }
}
template <class L, class R>
static inline void op_idiv(L *lhs, const R &val, int64_t count)
{
    ASSERT(NULL != lhs);
    const L rval = static_cast<L>(val);
    for (int64_t i=0; i<count; ++i) {
        lhs[i] /= rval;
    }
}
template <class L, class R>
static inline void op_imax(L *lhs, const R &val, int64_t count)
{
    ASSERT(NULL != lhs);
    const L rval = static_cast<L>(val);
    for (int64_t i=0; i<count; ++i) {
        lhs[i] = lhs[i] > rval ? lhs[i] : rval;
    }
}
template <class L, class R>
static inline void op_imin(L *lhs, const R &val, int64_t count)
{
    ASSERT(NULL != lhs);
    const L rval = static_cast<L>(val);
    for (int64_t i=0; i<count; ++i) {
        lhs[i] = lhs[i] < rval ? lhs[i] : rval;
    }
}
template <class L, class R>
static void op_ipow(L *lhs, const R &exponent, int64_t count)
{
    ASSERT(NULL != lhs);
    const double exp = static_cast<double>(exponent);
    for (int64_t i=0; i<count; ++i) {
        lhs[i] = static_cast<L>(std::pow(static_cast<double>(lhs[i]),exp));
    }
}


void AbstractArray::operate_scalar(const void *rhs_ptr, DataType rhs_type,
                                   const int op)
{
    void *lhs_ptr = access();
    if (NULL != lhs_ptr) {
        DataType lhs_type = get_type();
#define DATATYPE_EXPAND(DT_L,T_L,DT_R,T_R)                         \
        if (DT_L == lhs_type && DT_R == rhs_type) {                \
            T_L *lhs_buf = static_cast<T_L*>(lhs_ptr);             \
            const T_R rhs_val = *static_cast<const T_R*>(rhs_ptr); \
            switch (op) {                                          \
                case OP_ADD:                                       \
                    op_iadd(lhs_buf, rhs_val, get_local_size());   \
                    break;                                         \
                case OP_SUB:                                       \
                    op_isub(lhs_buf, rhs_val, get_local_size());   \
                    break;                                         \
                case OP_MUL:                                       \
                    op_imul(lhs_buf, rhs_val, get_local_size());   \
                    break;                                         \
                case OP_DIV:                                       \
                    op_idiv(lhs_buf, rhs_val, get_local_size());   \
                    break;                                         \
                case OP_MAX:                                       \
                    op_imax(lhs_buf, rhs_val, get_local_size());   \
                    break;                                         \
                case OP_MIN:                                       \
                    op_imin(lhs_buf, rhs_val, get_local_size());   \
                    break;                                         \
                case OP_POW:                                       \
                    op_ipow(lhs_buf, rhs_val, get_local_size());   \
                    break;                                         \
                default: ERRCODE("operation not supported", op);   \
            }                                                      \
        } else
#include "DataType2_small.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", lhs_type);
        }
        release_update();
    }
}
