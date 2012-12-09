#include "DataType.H"
#include "Error.H"
#include "Numeric.H"


void pagoda::transpose(const DataType &type,
        const void *src, const vector<int64_t> &shape,
              void *dst, const vector<int64_t> &axes)
{
#define DATATYPE_EXPAND(DT,TYPE)                               \
    if (type == DT) {                                          \
        const TYPE *typed_src = static_cast<const TYPE*>(src); \
        TYPE *typed_dst = static_cast<TYPE*>(dst);             \
        pagoda::transpose(typed_src, shape, typed_dst, axes);  \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
}


void pagoda::in_place_broadcast_add(
              void *dst, const vector<int64_t> &dst_shape, const DataType &dst_type,
        const void *src, const vector<int64_t> &src_shape, const DataType &src_type)
{
#define DATATYPE_EXPAND(DT,TYPE,DT2,TYPE2)                          \
    if (dst_type == DT && src_type == DT2) {                        \
        TYPE *typed_dst = static_cast<TYPE*>(dst);                  \
        const TYPE2 *typed_src = static_cast<const TYPE2*>(src);    \
        pagoda::in_place_broadcast_op(                              \
                typed_dst, dst_shape,                               \
                typed_src, src_shape,                               \
                pagoda::iadd<TYPE,TYPE2>);                          \
    } else
#include "DataType2_small.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", dst_type);
    }
}

void pagoda::in_place_broadcast_mul(
              void *dst, const vector<int64_t> &dst_shape, const DataType &dst_type,
        const void *src, const vector<int64_t> &src_shape, const DataType &src_type)
{
#define DATATYPE_EXPAND(DT,TYPE,DT2,TYPE2)                          \
    if (dst_type == DT && src_type == DT2) {                        \
        TYPE *typed_dst = static_cast<TYPE*>(dst);                  \
        const TYPE2 *typed_src = static_cast<const TYPE2*>(src);    \
        pagoda::in_place_broadcast_op(                              \
                typed_dst, dst_shape,                               \
                typed_src, src_shape,                               \
                pagoda::imul<TYPE,TYPE2>);                          \
    } else
#include "DataType2_small.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", dst_type);
    }
}

void pagoda::reduce_sum(const DataType &type,
        const void *src, const vector<int64_t> &src_shape,
              void *dst, const vector<int64_t> &dst_shape)
{
#define DATATYPE_EXPAND(DT,TYPE)                                        \
    if (type == DT) {                                                   \
        const TYPE *typed_src = static_cast<const TYPE*>(src);          \
        TYPE *typed_dst = static_cast<TYPE*>(dst);                      \
        pagoda::reduce_sum(typed_src, src_shape, typed_dst, dst_shape); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
}

void pagoda::reduce_max(const DataType &type,
        const void *src, const vector<int64_t> &src_shape,
              void *dst, const vector<int64_t> &dst_shape)
{
#define DATATYPE_EXPAND(DT,TYPE)                                        \
    if (type == DT) {                                                   \
        const TYPE *typed_src = static_cast<const TYPE*>(src);          \
        TYPE *typed_dst = static_cast<TYPE*>(dst);                      \
        pagoda::reduce_max(typed_src, src_shape, typed_dst, dst_shape); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
}

void pagoda::reduce_min(const DataType &type,
        const void *src, const vector<int64_t> &src_shape,
              void *dst, const vector<int64_t> &dst_shape)
{
#define DATATYPE_EXPAND(DT,TYPE)                                        \
    if (type == DT) {                                                   \
        const TYPE *typed_src = static_cast<const TYPE*>(src);          \
        TYPE *typed_dst = static_cast<TYPE*>(dst);                      \
        pagoda::reduce_min(typed_src, src_shape, typed_dst, dst_shape); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
}

