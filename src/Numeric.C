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


void pagoda::reduce_sum(const DataType &type,
        const void *src, const vector<int64_t> &src_shape,
              void *dst, const vector<int64_t> &dst_shape,
        const void *msk, const vector<int64_t> &msk_shape)
{
#define DATATYPE_EXPAND(DT,TYPE)                                        \
    if (type == DT) {                                                   \
        const TYPE *typed_src = static_cast<const TYPE*>(src);          \
        TYPE *typed_dst = static_cast<TYPE*>(dst);                      \
        const int *typed_msk = static_cast<const int*>(msk);            \
        pagoda::reduce_sum(typed_src, src_shape,                        \
                           typed_dst, dst_shape,                        \
                           typed_msk, msk_shape);                       \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
}


void pagoda::reduce_sum(const DataType &type, const DataType &wgt_type,
        const void *src, const vector<int64_t> &src_shape,
              void *dst, const vector<int64_t> &dst_shape,
        const void *msk, const vector<int64_t> &msk_shape,
        const void *wgt, const vector<int64_t> &wgt_shape)
{
#define DATATYPE_EXPAND(DT,TYPE,DT2,TYPE2)                              \
    if (type == DT && wgt_type == DT2) {                                \
        const TYPE *typed_src = static_cast<const TYPE*>(src);          \
        TYPE *typed_dst = static_cast<TYPE*>(dst);                      \
        const int *typed_msk = static_cast<const int*>(msk);            \
        const TYPE2 *typed_wgt = static_cast<const TYPE2*>(wgt);        \
        pagoda::reduce_sum(typed_src, src_shape,                        \
                           typed_dst, dst_shape,                        \
                           typed_msk, msk_shape,                        \
                           typed_wgt, wgt_shape);                       \
    } else
#include "DataType2_small.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
}

