#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <vector>

#include <ga.h>
#include <macdecls.h>
#include <message.h>

#include "Array.H"
#include "Debug.H"
#include "Pack.H"
#include "Timing.H"
#include "Util.H"

using std::vector;
using namespace pagoda;


/**
 * Inline version to unravel an index into an N-Dimensional index.
 *
 * x and dims of [a,b,c,d] --> [x/dcb % a, x/dc % b, x/d % c, x/1 % d]
 */
static inline void unravel64i(
        int64_t x,
        int ndim,
        int64_t *dims,
        int64_t *result)
{
    //TIMING("unravel64i(int64_t,int,int64_t*,int64_t*)");
    result[ndim-1] = x % dims[ndim-1];
    for (int i=ndim-2; i>=0; --i) {
        x /= dims[i+1];
        result[i] = x % dims[i];
    }
    for (int i=0; i<ndim; ++i) {
        if (result[i] >= dims[i]) {
            Util::abort("unravel64i: result[i] >= dims[i]");
        }
    }
}


/**
 * Inline version to unravel an index into an N-Dimensional index.
 *
 * x and dims of [a,b,c,d] --> [x/dcb % a, x/dc % b, x/d % c, x/1 % d]
 */
static inline void unravel64i(
        int64_t x, 
        const vector<int64_t> &dims,
        vector<int64_t> &result)
{
    //TIMING("unravel64i(int64_t,int,int64_t*,int64_t*)");
    int64_t ndim = dims.size();
    result.assign(ndim,-1);
    result[ndim-1] = x % dims[ndim-1];
    for (int i=ndim-2; i>=0; --i) {
        x /= dims[i+1];
        result[i] = x % dims[i];
    }
    for (int i=0; i<ndim; ++i) {
        if (result[i] >= dims[i]) {
            Util::abort("unravel64i: result[i] >= dims[i]");
        }
    }
}


/**
 * Perform a partial sum of g_src and place the result into g_dst.
 *
 * For example (exclude=true):
 *  - dst[0] = src[0]
 *  - dst[1] = src[0] + src[1]
 *  - dst[2] = src[0] + src[1] + src[2]
 * For example (exclude=false):
 *  - dst[0] = 0
 *  - dst[1] = 0 + src[1]
 *  - dst[2] = 0 + src[1] + src[2]
 *
 * @param[in] g_src the Array to sum over
 * @param[out] g_dst the result
 * @param[in] excl whether the first value is always 0
 */
void pagoda::partial_sum(const Array *g_src, Array *g_dst, bool excl)
{
    int nproc = Util::num_nodes();
    int me = Util::nodeid();
    DataType type_src = g_src->get_type();
    DataType type_dst = g_dst->get_type();
    int64_t dims;
    void *ptr_src;
    void *ptr_dst;
    int64_t elems;

    TIMING("partial_sum(Array*,Array*,bool)");
    TRACER("partial_sum(Array*,Array*,bool)");

    if (! g_src->same_distribution(g_dst)) {
        Util::abort("partial_sum: Arrays must have same distribution", 0);
    }
    if (g_src->get_ndim() != 1 || g_dst->get_ndim() != 1) {
        Util::abort("partial_sum: supports 1-dim Arrays only", 0);
    }

    //Util::barrier(); // TODO do we need this?

    elems = g_src->get_local_size();

    if (g_src->owns_data()) {
        ptr_src = g_src->access();
        ptr_dst = g_dst->access();
#define partial_sum_op(DTYPE_SRC,TYPE_SRC,DTYPE_DST,TYPE_DST,FMT) \
        if (DTYPE_SRC == type_src && DTYPE_DST == type_dst) { \
            TYPE_DST value = 0; \
            vector<TYPE_DST> values(nproc,0); \
            TYPE_DST *dst = (TYPE_DST*)ptr_dst; \
            TYPE_SRC *src = (TYPE_SRC*)ptr_src; \
            if (!excl) { \
                dst[0] = 0; \
                for (int64_t i=1; i<elems; ++i) { \
                    dst[i] = dst[i-1] + src[i-1]; \
                } \
            } else { \
                dst[0] = src[0]; \
                for (int64_t i=1; i<elems; ++i) { \
                    dst[i] = dst[i-1] + src[i]; \
                } \
            } \
            values[me] = dst[elems-1]; \
            if (!excl) { \
                values[me] += src[elems-1]; \
            } \
            Util::gop_sum(values); \
            for (int64_t i=0; i<me; ++i) { \
                value += values[i]; \
            } \
            for (int64_t i=0; i<elems; ++i) { \
                dst[i] += value; \
            } \
            TRACER("partial_sum_op "#FMT"\n", value); \
        } else
        partial_sum_op(DataType::INT,int,DataType::INT,int,%d)
        partial_sum_op(DataType::INT,int,DataType::LONG,long,%ld)
        partial_sum_op(DataType::INT,int,DataType::LONGLONG,long long,%lld)
        partial_sum_op(DataType::LONG,long,DataType::LONG,long,%ld)
        partial_sum_op(DataType::LONG,long,DataType::LONGLONG,long long,%lld)
        partial_sum_op(DataType::LONGLONG,long long,DataType::LONGLONG,long long,%lld)
        partial_sum_op(DataType::FLOAT,float,DataType::FLOAT,float,%f)
        partial_sum_op(DataType::FLOAT,float,DataType::DOUBLE,double,%f)
        partial_sum_op(DataType::DOUBLE,double,DataType::DOUBLE,double,%f)
        ; // for last else above
#undef partial_sum_op
        g_dst->release_update();
        g_src->release();
    } else {
        /* no elements stored on this process */
        /* broadcast dummy value to all processes */
#define partial_sum_op(DTYPE,TYPE) \
        if (DTYPE == type_dst) { \
            vector<TYPE> values(nproc, 0); \
            Util::gop_sum(values); \
            TRACER("partial_sum_op N/A\n"); \
        } else
        partial_sum_op(DataType::INT,       int)
        partial_sum_op(DataType::LONG,      long)
        partial_sum_op(DataType::LONGLONG,  long long)
        partial_sum_op(DataType::FLOAT,     float)
        partial_sum_op(DataType::DOUBLE,    double)
        partial_sum_op(DataType::LONGDOUBLE,long double)
        ; // for last else above
#undef partial_sum_op
    }
}


/**
 * N-Dimensional packing/subsetting.
 */
void pagoda::pack(const Array *g_src, Array *g_dst,
        vector<Array*> g_masks, vector<Array*> g_masksums)
{
    int me = Util::nodeid();

    DataType type_src = g_src->get_type();
    int ndim_src = g_src->get_ndim();
    vector<int64_t> lo_src(ndim_src,0);
    vector<int64_t> hi_src(ndim_src,0);
    vector<int64_t> ld_src(ndim_src-1,0);

    DataType type_dst = g_dst->get_type();
    int ndim_dst = g_dst->get_ndim();
    vector<int64_t> dims_dst(ndim_dst,0);
    vector<int64_t> lo_dst(ndim_dst,0);
    vector<int64_t> hi_dst(ndim_dst,0);
    vector<int64_t> ld_dst(ndim_dst-1,0);

    TIMING("pack(Array*,Array*,vector<Array*>)");
    TIMING("pack(Array*,Array*,vector<Array*>) BEGIN");

    if (ndim_src != ndim_dst) {
        Util::abort("pack: src and dst ndims don't match", ndim_src-ndim_dst);
    }

    //Util::barrier(); // TODO do we need this?

    g_src->get_distribution(lo_src,hi_src);

    if (0 > lo_src[0] && 0 > hi_src[0]) {
        /* no elements on this process */
        TRACER("no elements on this process\n");
        TRACER("no elements on this process\n");
        TRACER("no Clean up\n");
    } else {
        vector<int64_t> elems_src(ndim_src,0);
        vector<int64_t> index(ndim_src,0);
        vector<int64_t> local_counts(ndim_src,0);
        vector<int*> local_masks(ndim_src,NULL);
        int64_t elems_product_src=1;
        int64_t local_counts_product=1;

        /* Now get the portions of the masks associated with each dim */
        for (int i=0; i<ndim_src; ++i) {
            elems_src[i] = hi_src[i]-lo_src[i]+1;
            elems_product_src *= elems_src[i];
            local_masks[i] = (int*)g_masks[i]->get(lo_src[i], hi_src[i]);
            for (int64_t j=0; j<elems_src[i]; ++j) {
                if (local_masks[i][j]) {
                    ++(local_counts[i]);
                }
            }
            local_counts_product *= local_counts[i];
        }
        //print_local_masks(local_masks, elems, ndim);

        if (0 == local_counts_product) {
            /* if any of the local mask counts are zero, no work is needed */
            TRACER("0 == local_counts_product\n");
            TRACER("0 == local_counts_product\n");
        } else {
            /* determine where the data is to go */
            for (int i=0; i<ndim_src; ++i) {
                int *tmp = (int*)g_masksums[i]->get(lo_src[i],lo_src[i]);
                lo_dst[i] = *tmp;
                hi_dst[i] = *tmp+local_counts[i]-1;
                delete tmp;
            }
            //printf("ld_dst=");
            for (int i=0; i<ndim_src-1; ++i) {
                ld_dst[i] = local_counts[i+1];
                //printf("%d,", ld_dst[i]);
            }
            //printf("\n");
            
            /* Create the destination buffer */
#define pack_bit_copy(DTYPE,TYPE,FMT) \
            if (type_src == DTYPE) { \
                int64_t buf_dst_index = 0; \
                TYPE *buf_src = (TYPE*)g_src->access(); \
                TYPE *buf_dst = new TYPE[local_counts_product]; \
                TRACER("buf_src[0]="#FMT"\n", buf_src[0]); \
                for (int64_t i=0; i<elems_product_src; ++i) { \
                    unravel64i(i, ndim_src, &elems_src[0], &index[0]); \
                    int okay = 1; \
                    for (int j=0; j<ndim_src; ++j) { \
                        okay *= local_masks[j][index[j]]; \
                    } \
                    if (0 != okay) { \
                        buf_dst[buf_dst_index++] = buf_src[i]; \
                    } \
                } \
                if (buf_dst_index != local_counts_product) { \
                    printf("%ld != %ld\n", buf_dst_index, local_counts_product); \
                    Util::abort("pack: mismatch", buf_dst_index); \
                } \
                TRACER("g_dst=%d, lo_dst[0]=%ld, hi_dst[0]=%ld, buf_dst[0]="#FMT"\n", g_dst, lo_dst[0], hi_dst[0], buf_dst[0]); \
                g_dst->put(buf_dst, lo_dst, hi_dst, ld_dst); \
                g_src->release(); \
                delete [] buf_dst; \
            } else
            pack_bit_copy(DataType::INT,int,%d)
            pack_bit_copy(DataType::LONG,long,%ld)
            pack_bit_copy(DataType::LONGLONG,long long,%ld)
            pack_bit_copy(DataType::FLOAT,float,%f)
            pack_bit_copy(DataType::DOUBLE,double,%f)
            pack_bit_copy(DataType::LONGDOUBLE,long double,%f)
            ; // for last else above
#undef pack_bit_copy
        }
        //print_local_masks(local_masks, elems, ndim);
        // Clean up
        TRACER("before Clean up\n");
        /* Remove temporary partial sum arrays */
        for (int i=0; i<ndim_src; ++i) {
            int *tmp = local_masks[i];
            delete [] tmp;
            tmp = NULL;
        }
    }

    TRACER("pack(%d,%d,...) END\n", g_src, g_dst);
}


void pagoda::unravel64(int64_t x, int ndim, int64_t *dims, int64_t *result)
{
    TIMING("unravel64(int64_t,int,int64_t*,int64_t*)");
    unravel64i(x,ndim,dims,result);
}


void pagoda::unravel64(
        int64_t x,
        const vector<int64_t> &dims,
        vector<int64_t> &result)
{
    TIMING("unravel64(int64_t,int,int64_t*,int64_t*)");
    unravel64i(x,dims,result);
}


/**
 * A global enumeration operation.
 *
 * Assumes g_src is a 1D array.
 */
void pagoda::enumerate(Array *src, void *start_val, void *inc_val)
{
    vector<int64_t> lo;
    vector<int64_t> hi;

    TIMING("enumerate(Array*,void*,void*)");
    TRACER("enumerate BEGIN\n");

    src->get_distribution(lo,hi);

    if (lo.size() > 1) {
        Util::abort("enumerate: expected 1D array", lo.size());
    }

    if (src->owns_data()) {
#define enumerate_op(DTYPE,TYPE) \
        if (DTYPE == src->get_type()) { \
            TYPE *src_data = (TYPE*)src->access(); \
            TYPE start = 0; \
            TYPE inc = 1; \
            if (start_val) { \
                start = *((TYPE*)start_val); \
            } \
            if (inc_val) { \
                inc = *((TYPE*)inc_val); \
            } \
            for (int64_t i=0,limit=src->get_local_size(); i<limit; ++i) { \
                src_data[i] = (lo[0]+i)*inc + start; \
            } \
            src->release_update(); \
        } else
        enumerate_op(DataType::INT,       int)
        enumerate_op(DataType::LONG,      long)
        enumerate_op(DataType::LONGLONG,  long long)
        enumerate_op(DataType::FLOAT,     float)
        enumerate_op(DataType::DOUBLE,    double)
        enumerate_op(DataType::LONGDOUBLE,long double)
        ; // for last else above
#undef enumerate_op
    }
    TRACER("enumerate END\n");
}


/**
 * Unpack g_src into g_dst based on the mask g_msk.
 *
 * Assumes g_dst and g_msk have the same distributions and DataTypes.
 */
void pagoda::unpack1d(const Array *src, Array *dst, Array *msk)
{
    int me = Util::nodeid();
    int nproc = Util::num_nodes();
    int *mask;
    vector<long> counts(nproc,0);
    vector<int64_t> lo_src(1,0);
    vector<int64_t> hi_src(1,0);
    DataType type_src = src->get_type();

    TIMING("unpack1d(Array*,Array*,Array*)");
    TRACER("unpack1d BEGIN\n");

    if (!dst->same_distribution(msk)) {
        Util::abort("unpack1d: dst and msk distributions differ");
    }
    if (type_src != dst->get_type()) {
        Util::abort("unpack1d: src and dst DataTypes differ");
    }

    // count mask bits on each proc
    if (!msk->owns_data()) {
        Util::gop_sum(counts);
        TRACER("unpack1d lo,hi N/A 1\n");
        TRACER("unpack1d END\n");
        return; // this process doesn't participate
    } else {
        mask = (int*)msk->access();
        for (int64_t i=0,limit=msk->get_local_size(); i<limit; ++i) {
            if (0 != mask[i]) ++(counts[me]);
        }
        msk->release();
        Util::gop_sum(counts);
        if (0 == counts[me]) {
            TRACER("unpack1d lo,hi N/A 2\n");
            TRACER("unpack1d END\n");
            return; // this process doesn't participate
        }
    }

    // tally up where to start the 'get' of the packed array
    for (int i=0; i<me; ++i) {
        lo_src[0] += counts[i];
    }
    hi_src[0] = lo_src[0] + counts[me] - 1;
    TRACER("unpack1d lo,hi = %ld,%ld\n", lo_src[0], hi_src[0]);
    // do the unpacking
    // assumption is that dst array has same distribution as msk array
#define unpack1d_op(DTYPE,TYPE) \
    if (type_src == DTYPE) { \
        TYPE *src_data = (TYPE*)src->get(lo_src, hi_src); \
        TYPE *dst_data = (TYPE*)dst->access(); \
        TYPE *src_origin = src_data; \
        int  *msk_data = (int*)msk->access(); \
        for (int64_t i=0,limit=msk->get_local_size(); i<limit; ++i) \
        { \
            if (msk_data[i] != 0) { \
                dst_data[i] = *src_data; \
                ++src_data; \
            } \
        } \
        delete src_origin; \
        dst->release_update(); \
        msk->release(); \
    } else
    unpack1d_op(DataType::INT,int)
    unpack1d_op(DataType::LONG,long)
    unpack1d_op(DataType::LONGLONG,long long)
    unpack1d_op(DataType::FLOAT,float)
    unpack1d_op(DataType::DOUBLE,double)
    unpack1d_op(DataType::LONGDOUBLE,long double)
    ; // for last else above
#undef unpack1d_op
    TRACER("unpack1d END\n");
}
