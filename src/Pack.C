#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cstdio>
#include <map>
#include <vector>

#include "Array.H"
#include "Debug.H"
#include "Error.H"
#include "Mask.H"
#include "Pack.H"
#include "Util.H"

using std::printf;
using std::map;
using std::vector;


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
            pagoda::abort("unravel64i: result[i] >= dims[i]");
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
            pagoda::abort("unravel64i: result[i] >= dims[i]");
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
    int nproc = pagoda::num_nodes();
    int me = pagoda::nodeid();
    DataType type_src = g_src->get_type();
    DataType type_dst = g_dst->get_type();
    const void *ptr_src;
    void *ptr_dst;
    int64_t elems;

    TRACER("partial_sum(Array*,Array*,bool)");

    if (! g_src->same_distribution(g_dst)) {
        pagoda::abort("partial_sum: Arrays must have same distribution", 0);
    }
    if (g_src->get_ndim() != 1 || g_dst->get_ndim() != 1) {
        pagoda::abort("partial_sum: supports 1-dim Arrays only", 0);
    }

    //pagoda::barrier(); /** @todo do we need pagoda::barrier()? */

    elems = g_src->get_local_size();

    if (g_src->owns_data()) {
        ptr_src = g_src->access();
        ptr_dst = g_dst->access();
#define partial_sum_op(DTYPE_SRC,TYPE_SRC,DTYPE_DST,TYPE_DST,FMT) \
        if (DTYPE_SRC == type_src && DTYPE_DST == type_dst) { \
            TYPE_DST value = 0; \
            vector<TYPE_DST> values(nproc,0); \
            TYPE_DST *dst = static_cast<TYPE_DST*>(ptr_dst); \
            const TYPE_SRC *src = static_cast<const TYPE_SRC*>(ptr_src); \
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
            pagoda::gop_sum(values); \
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
    }
    else {
        /* no elements stored on this process */
        /* broadcast dummy value to all processes */
#define partial_sum_op(DTYPE,TYPE) \
        if (DTYPE == type_dst) { \
            vector<TYPE> values(nproc, 0); \
            pagoda::gop_sum(values); \
            TRACER("partial_sum_op N/A\n"); \
        } else
        partial_sum_op(DataType::INT,       int)
        partial_sum_op(DataType::LONG,      long)
        partial_sum_op(DataType::LONGLONG,  long long)
        partial_sum_op(DataType::FLOAT,     float)
        partial_sum_op(DataType::DOUBLE,    double)
        partial_sum_op(DataType::LONGDOUBLE,long double) {
            EXCEPT(DataTypeException, "DataType not handled", type_dst);
        }
#undef partial_sum_op
    }
}


/**
 * N-Dimensional packing/subsetting.
 */
void pagoda::pack(const Array *g_src, Array *g_dst,
                  const vector<Mask*> &g_masks)
{
    vector<Array*> sums;
    vector<Array*> masks_copy(g_masks.begin(), g_masks.end());
    vector<Mask*>::const_iterator it;

    for (it=g_masks.begin(); it!=g_masks.end(); ++it) {
        if (*it) {
            sums.push_back((*it)->partial_sum(false));
        } else {
            sums.push_back(NULL);
        }
    }

    pack(g_src, g_dst, masks_copy, sums);
}


/**
 * N-Dimensional packing/subsetting.
 */
void pagoda::pack(const Array *g_src, Array *g_dst,
                  const vector<Array*> &g_masks,
                  const vector<Array*> &g_masksums)
{
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
    //vector<int64_t> ld_dst(ndim_dst-1,0);


    if (ndim_src != ndim_dst) {
        pagoda::abort("pack: src and dst ndims don't match", ndim_src-ndim_dst);
    }

    //pagoda::barrier(); /** @todo do we need this? */

    g_src->get_distribution(lo_src,hi_src);

    if (0 > lo_src[0] && 0 > hi_src[0]) {
        /* no elements on this process */
        TRACER("no elements on this process\n");
        TRACER("no elements on this process\n");
        TRACER("no Clean up\n");
    }
    else {
        vector<int64_t> elems_src(ndim_src,0);
        vector<int64_t> local_counts(ndim_src,0);
        vector<int*> local_masks(ndim_src,NULL);
        int64_t elems_product_src=1;
        int64_t local_counts_product=1;

        /* Now get the portions of the masks associated with each dim */
        for (int i=0; i<ndim_src; ++i) {
            elems_src[i] = hi_src[i]-lo_src[i]+1;
            elems_product_src *= elems_src[i];
            if (g_masks[i]) {
                local_masks[i] = (int*)g_masks[i]->get(lo_src[i], hi_src[i]);
                for (int64_t j=0; j<elems_src[i]; ++j) {
                    if (local_masks[i][j]) {
                        ++(local_counts[i]);
                    }
                }
            } else {
                local_masks[i] = NULL;
                local_counts[i] += elems_src[i];
            }
            local_counts_product *= local_counts[i];
        }
        //print_local_masks(local_masks, elems, ndim);

        if (0 == local_counts_product) {
            /* if any of the local mask counts are zero, no work is needed */
            TRACER("0 == local_counts_product\n");
            TRACER("0 == local_counts_product\n");
        }
        else {
            /* determine where the data is to go */
            for (int i=0; i<ndim_src; ++i) {
                if (g_masksums[i]) {
                    int *tmp = (int*)g_masksums[i]->get(lo_src[i],lo_src[i]);
                    lo_dst[i] = *tmp;
                    hi_dst[i] = *tmp+local_counts[i]-1;
                    delete [] tmp;
                } else {
                    lo_dst[i] = lo_src[i];
                    // Not sure which of the following is correct.
                    //hi_dst[i] = hi_src[i];
                    hi_dst[i] = lo_src[i]+local_counts[i]-1;
                }
            }
            //printf("ld_dst=");
            /*
            for (int i=0; i<ndim_src-1; ++i) {
                ld_dst[i] = local_counts[i+1];
                //printf("%d,", ld_dst[i]);
            }
            */
            //printf("\n");

            /* Create the destination buffer */
#define pack_bit_copy(DTYPE,TYPE,FMT) \
            if (type_src == DTYPE) { \
                int64_t okay_count = 0; \
                const TYPE *sptr = static_cast<const TYPE*>(g_src->access()); \
                TYPE *buf_dst = new TYPE[local_counts_product]; \
                TYPE *dptr = buf_dst; \
                int64_t nd_m1 = ndim_src-1; \
                int64_t *coords = new int64_t[ndim_src]; \
                int64_t *dims_m1 = new int64_t[ndim_src]; \
                int64_t *strides = new int64_t[ndim_src]; \
                int64_t *backstrides = new int64_t[ndim_src]; \
                TRACER("buf_src[0]="#FMT"\n", sptr[0]); \
                for (int64_t i=nd_m1; i>=0; --i) { \
                    coords[i] = 0; \
                    dims_m1[i] = elems_src[i]-1; \
                    strides[i] = (i == nd_m1) ? 1 : strides[i+1]*elems_src[i+1]; \
                    backstrides[i] = dims_m1[i]*strides[i]; \
                } \
                for (int64_t i=0; i<elems_product_src; ++i) { \
                    int j; \
                    for (j=0; j<ndim_src; ++j) { \
                        if (local_masks[j] != NULL) { \
                            if (local_masks[j][coords[j]] == 0) { \
                                break; \
                            } \
                        } \
                    } \
                    if (j == ndim_src) { \
                        ++okay_count; \
                        *(dptr++) = *sptr; \
                    } \
                    for (int64_t i=nd_m1; i>=0; --i) { \
                        if (coords[i] < dims_m1[i]) { \
                            ++coords[i]; \
                            sptr += strides[i]; \
                            break; \
                        } \
                        else { \
                            coords[i] = 0; \
                            sptr -= backstrides[i]; \
                        } \
                    } \
                } \
                if (okay_count != local_counts_product) { \
                    pagoda::abort("pack: mismatch", okay_count); \
                } \
                TRACER("g_dst=%d, lo_dst[0]=%ld, hi_dst[0]=%ld, buf_dst[0]="#FMT"\n", g_dst, lo_dst[0], hi_dst[0], buf_dst[0]); \
                g_dst->put(buf_dst, lo_dst, hi_dst); \
                g_src->release(); \
                delete [] buf_dst; \
                delete [] coords; \
                delete [] dims_m1; \
                delete [] strides; \
                delete [] backstrides; \
            } else
            pack_bit_copy(DataType::INT,int,%d)
            pack_bit_copy(DataType::LONG,long,%ld)
            pack_bit_copy(DataType::LONGLONG,long long,%ld)
            pack_bit_copy(DataType::FLOAT,float,%f)
            pack_bit_copy(DataType::DOUBLE,double,%f)
            pack_bit_copy(DataType::LONGDOUBLE,long double,%f) {
                EXCEPT(DataTypeException, "DataType not handled", type_src);
            }
#undef pack_bit_copy
        }
        //print_local_masks(local_masks, elems, ndim);
        // Clean up
        TRACER("before Clean up\n");
        /* Remove temporary partial sum arrays */
        for (int i=0; i<ndim_src; ++i) {
            int *tmp = local_masks[i];
            if (tmp) {
                delete [] tmp;
                tmp = NULL;
            }
        }
    }

    TRACER("pack(%d,%d,...) END\n", g_src, g_dst);
}


void pagoda::unravel64(int64_t x, int ndim, int64_t *dims, int64_t *result)
{
    unravel64i(x,ndim,dims,result);
}


void pagoda::unravel64(
    int64_t x,
    const vector<int64_t> &dims,
    vector<int64_t> &result)
{
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
    DataType type = src->get_type();

    TRACER("enumerate BEGIN\n");

    src->get_distribution(lo,hi);

    if (lo.size() > 1) {
        pagoda::abort("enumerate: expected 1D array", lo.size());
    }

    if (src->owns_data()) {
#define enumerate_op(DTYPE,TYPE) \
        if (DTYPE == type) { \
            TYPE *src_data = static_cast<TYPE*>(src->access()); \
            TYPE start = 0; \
            TYPE inc = 1; \
            if (start_val) { \
                start = *static_cast<TYPE*>(start_val); \
            } \
            if (inc_val) { \
                inc = *static_cast<TYPE*>(inc_val); \
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
        enumerate_op(DataType::LONGDOUBLE,long double) {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
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
    int me = pagoda::nodeid();
    int nproc = pagoda::num_nodes();
    int *mask;
    vector<long> counts(nproc,0);
    vector<int64_t> lo_src(1,0);
    vector<int64_t> hi_src(1,0);
    DataType type_src = src->get_type();

    TRACER("unpack1d BEGIN\n");

    if (!dst->same_distribution(msk)) {
        pagoda::abort("unpack1d: dst and msk distributions differ");
    }
    if (type_src != dst->get_type()) {
        pagoda::abort("unpack1d: src and dst DataTypes differ");
    }
    if (DataType::INT != msk->get_type()) {
        pagoda::abort("unpack1d: msk must be of type INT",
                      msk->get_type().get_id());
    }

    // count mask bits on each proc
    if (!msk->owns_data()) {
        pagoda::gop_sum(counts);
        TRACER("unpack1d lo,hi N/A 1 counts[me]=%ld\n", counts[me]);
        TRACER("unpack1d END\n");
        return; // this process doesn't participate
    }
    else {
        mask = static_cast<int*>(msk->access());
        for (int64_t i=0,limit=msk->get_local_size(); i<limit; ++i) {
            if (0 != mask[i]) {
                ++(counts[me]);
            }
        }
        msk->release();
        pagoda::gop_sum(counts);
        if (0 == counts[me]) {
            TRACER("unpack1d lo,hi N/A 2 counts[me]=%ld\n", counts[me]);
            TRACER("unpack1d END\n");
            return; // this process doesn't participate
        }
    }

    // tally up where to start the 'get' of the packed array
    for (int i=0; i<me; ++i) {
        lo_src[0] += counts[i];
    }
    hi_src[0] = lo_src[0] + counts[me] - 1;
    TRACER("unpack1d lo,hi,counts[me] = %ld,%ld,%ld\n",
           lo_src[0], hi_src[0], counts[me]);
    // do the unpacking
    // assumption is that dst array has same distribution as msk array
#define unpack1d_op(DTYPE,TYPE) \
    if (type_src == DTYPE) { \
        const TYPE *src_data = static_cast<const TYPE*>( \
                src->get(lo_src, hi_src)); \
        TYPE *dst_data = static_cast<TYPE*>(dst->access()); \
        const TYPE *src_origin = src_data; \
        int  *msk_data = static_cast<int*>(msk->access()); \
        for (int64_t i=0,limit=msk->get_local_size(); i<limit; ++i) \
        { \
            if (msk_data[i] != 0) { \
                dst_data[i] = *(src_data++); \
            } \
        } \
        delete [] src_origin; \
        dst->release_update(); \
        msk->release(); \
    } else
    unpack1d_op(DataType::INT,int)
    unpack1d_op(DataType::LONG,long)
    unpack1d_op(DataType::LONGLONG,long long)
    unpack1d_op(DataType::FLOAT,float)
    unpack1d_op(DataType::DOUBLE,double)
    unpack1d_op(DataType::LONGDOUBLE,long double) {
        EXCEPT(DataTypeException, "DataType not handled", type_src);
    }
#undef unpack1d_op
    TRACER("unpack1d END\n");
}


/**
 * Reassign the values of the given integer Array based on the given index
 * integer Array.
 *
 * The index Array is a 1-D mapping from its zero-based index to a new index
 * value.  The index Array is assumed to be 1-dimensional and an integer
 * Array.
 *
 * @param[in,out] array the Array to renumber
 * @param[in] index the values to use when renumbering
 */
void pagoda::renumber(Array *array, const Array *index)
{
    int *buf;
    map<int64_t,int64_t> m;
    map<int64_t,int64_t>::iterator m_it;
    map<int64_t,int64_t>::iterator m_end;
    vector<int64_t> subscripts;

    if (index->get_ndim() != 1) {
        ERR("index Array must be 1-dimensional");
    }

    if (!array->owns_data()) {
        return;
    }

    buf = static_cast<int*>(array->access());
    for (int64_t i=0,limit=array->get_local_size(); i<limit; ++i) {
        m[buf[i]] = -1;
    }
    array->release();

    subscripts.reserve(m.size());
    for (m_it=m.begin(),m_end=m.end(); m_it!=m_end; ++m_it) {
        subscripts.push_back(m_it->first);
    }

    // do the gather
    buf = (int*)index->gather(subscripts);

    // create the mapping
    for (size_t i=0,limit=subscripts.size(); i<limit; ++i) {
        m[subscripts[i]] = buf[i];
    }
    delete [] buf;

    // use the mapping to renumber the values
    buf = (int*)array->access();
    for (int64_t i=0,limit=array->get_local_size(); i<limit; ++i) {
        buf[i] = m.find(buf[i])->second;
    }
    array->release_update();
}
