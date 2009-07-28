#include <float.h> // for DBL_MIN
#include <limits.h> // for LONG_MIN
#include <strings.h> // for bzero

#include <ga.h>
#include <macdecls.h>

#include "Pack.H"


void partial_sum(int g_src, int g_dst, int excl)
{
    int nproc = GA_Nnodes();
    int me = GA_Nodeid();
    int type_src;
    int type_dst;
    int ndim;
    int64_t dims;
    int64_t lo = -1;
    int64_t hi = -2;
    void *ptr_src;
    void *ptr_dst;
    int64_t elems;

    /* make sure we were given valid args */
    GA_Check_handle(g_src, "partial_sum src");
    GA_Check_handle(g_dst, "partial_sum dst");
    if (1 == GA_Compare_distr(g_src, g_dst)) {
        GA_Error("partial_sum: GAs must have same distribution", 1);
    }
    NGA_Inquire64(g_src, &type_src, &ndim, &dims);
    if (ndim>1) {
        GA_Error("partial_sum: supports 1-dim arrays only", g_src);
    }
    NGA_Inquire64(g_dst, &type_dst, &ndim, &dims);
    if (ndim>1) {
        GA_Error("partial_sum: supports 1-dim arrays only", g_dst);
    }
    if (type_src != type_dst) {
        GA_Error("partial_sum: src and dst array types must match", 1);
    }
    switch (type_src) {
        case MT_INT:
        case MT_LONGINT:
        case MT_REAL:
        case MT_DBL:
            break;
        default:
            GA_Error("partial_sum: only INT,LONG,REAL,DBL types supported", 1);
    }

    GA_Sync();
    
    NGA_Distribution64(g_src, me, &lo, &hi);
    elems = hi-lo+1;

    /* do the partial sum of our local portion, if any */
    if (0 > lo && 0 > hi); /* no elements stored on this process */
    else {
        NGA_Access64(g_src, &lo, &hi, &ptr_src, NULL);
        NGA_Access64(g_dst, &lo, &hi, &ptr_dst, NULL);
        switch (type_src) {
#define partial_sum_op(MTYPE,TYPE) \
            case MTYPE: \
                { \
                    TYPE *dst = (TYPE*)ptr_dst; \
                    TYPE *src = (TYPE*)ptr_src; \
                    if (0 == excl) { \
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
                    break; \
                }
            partial_sum_op(MT_INT,int)
            partial_sum_op(MT_LONGINT,long)
            partial_sum_op(MT_REAL,float)
            partial_sum_op(MT_DBL,double)
#undef partial_sum_op
        }
    }
    if (0 > lo && 0 > hi) {
        /* no elements stored on this process */
        /* broadcast dummy value to all processes */
        switch (type_src) {
#define partial_sum_op(MTYPE,TYPE,TYPE_MIN) \
            case MTYPE: \
                { \
                    TYPE values[nproc]; \
                    for (int64_t i=0; i<nproc; ++i) { \
                        values[i] = TYPE_MIN; \
                    } \
                    GA_Gop(MTYPE, values, nproc, "max"); \
                    break; \
                }
            partial_sum_op(MT_INT,int,INT_MIN)
            partial_sum_op(MT_LONGINT,long,LONG_MIN)
            partial_sum_op(MT_REAL,float,FLT_MIN)
            partial_sum_op(MT_DBL,double,DBL_MIN)
#undef partial_sum_op
        }
    } else {
        /* broadcast last value to all processes */
        /* then sum those values and add to each element as appropriate */
        switch (type_src) {
#define partial_sum_op(MTYPE,TYPE,TYPE_MIN) \
            case MTYPE: \
                { \
                    TYPE value = 0; \
                    TYPE values[nproc]; \
                    TYPE *src = (TYPE*)ptr_src; \
                    TYPE *dst = (TYPE*)ptr_dst; \
                    for (int64_t i=0; i<nproc; ++i) { \
                        values[i] = TYPE_MIN; \
                    } \
                    values[me] = src[elems-1]; \
                    GA_Gop(MTYPE, values, nproc, "max"); \
                    for (int64_t i=0; i<me; ++i) { \
                        if (TYPE_MIN != values[i]) { \
                            value += values[i]; \
                        } \
                    } \
                    for (int64_t i=0; i<elems; ++i) { \
                        dst[i] += value; \
                    } \
                    break; \
                }
            partial_sum_op(MT_INT,int,INT_MIN)
            partial_sum_op(MT_LONGINT,long,LONG_MIN)
            partial_sum_op(MT_REAL,float,FLT_MIN)
            partial_sum_op(MT_DBL,double,DBL_MIN)
#undef partial_sum_op
        }
        NGA_Release64(g_src, &lo, &hi);
        NGA_Release_update64(g_dst, &lo, &hi);
    }
}



void pack(int g_src, int g_dst, int *g_masks)
{
    int nproc = GA_Nnodes();
    int me = GA_Nodeid();
    int type;
    int ndim = GA_Ndim(g_src);
    int ndim_dst = GA_Ndim(g_dst);
    int64_t dims[ndim];
    int64_t lo[ndim];
    int64_t hi[ndim];
    int64_t ld[ndim];
    int64_t elems[ndim];
    int64_t elems_product=1;
    int64_t lo_dst[ndim];
    int64_t hi_dst[ndim];
    int64_t ld_dst[ndim];
    int64_t index[ndim];
    int g_masksums[ndim];
    long local_counts[ndim];
    long local_counts_product=1;
    int *local_masks[ndim];

    if (ndim != ndim_dst) {
        GA_Error("pack: src and dst ndims don't match", 0);
    }

    NGA_Inquire64(g_src, &type, &ndim, dims);

    GA_Sync();

    /* We must perform the partial sum on the masks*/
    for (int i=0; i<ndim; ++i) {
        g_masksums[i] = GA_Duplicate(g_masks[i], "maskcopy");
        partial_sum(g_masks[i], g_masksums[i], 0);
    }

    NGA_Distribution64(g_src, me, lo, hi);

    if (0 > lo[0] && 0 > hi[0]); /* no elements stored on this process */
    else {
        /* Now get the portions of the masks associated with each dim */
        bzero(local_counts, sizeof(long)*ndim);
        for (int i=0; i<ndim; ++i) {
            elems[i] = hi[i]-lo[i]+1;
            elems_product *= elems[i];
            local_masks[i] = new int[elems[i]];
            NGA_Get64(g_masks[i], lo+i, hi+i, local_masks[i], NULL);
            for (int64_t j=0; j<elems[i]; ++j) {
                if (local_masks[i][j]) {
                    ++(local_counts[i]);
                }
            }
            local_counts_product *= local_counts[i];
        }

        /* if any of the local mask counts are zero, no work is needed */
        if (0 == local_counts_product); 
        else {
            /* determine where the data is to go */
            for (int i=0; i<ndim; ++i) {
                int tmp;
                NGA_Get64(g_masksums[i], lo+i, lo+i, &tmp, NULL);
                lo_dst[i] = tmp;
                hi_dst[i] = local_counts[i]-1;
            }
            for (int i=0; i<ndim-1; ++i) {
                ld_dst[i] = local_counts[i+1];
            }
            
            /* Create the destination buffer */
            switch (type) {
#define pack_bit_copy(MTYPE,TYPE) \
                case MTYPE: \
                    { \
                        int64_t buf_dst_index = 0; \
                        TYPE *buf_src = NULL; \
                        TYPE *buf_dst = new TYPE[local_counts_product]; \
                        NGA_Access64(g_src, lo, hi, &buf_src, ld); \
                        for (int64_t i=0; i<elems_product; ++i) { \
                            unravel64(i, ndim, dims, index); \
                            int okay = 1; \
                            for (int j=0; j<ndim; ++j) { \
                                okay *= local_masks[j][index[j]]; \
                            } \
                            if (0 != okay) { \
                                buf_dst[buf_dst_index++] = buf_src[i]; \
                            } \
                        } \
                        NGA_Put64(g_dst, lo_dst, hi_dst, buf_dst, ld_dst); \
                        NGA_Release64(g_src, lo, hi); \
                        delete [] buf_dst; \
                        break; \
                    }
                pack_bit_copy(MT_INT,int)
                pack_bit_copy(MT_LONGINT,long)
                pack_bit_copy(MT_REAL,float)
                pack_bit_copy(MT_DBL,double)
#undef pack_bit_copy
            }
        }
    }

    // Clean up
    /* Remove temporary partial sum arrays */
    for (int i=0; i<ndim; ++i) {
        GA_Destroy(g_masksums[i]);
        delete [] local_masks[i];
    }
}


void unravel64(int64_t x, int ndim, int64_t *dims, int64_t *result)
{
    // x and dims of [a,b,c,d] --> [x/dcb % a, x/dc % b, x/d % c, x/1 % d]
    result[ndim-1] = x % dims[ndim-1];
    for (int i=ndim-2; i>=0; --i) {
        x /= dims[i+1];
        result[i] = x % dims[i];
    }
}


/**
 * A global enumeration operation.
 *
 * Assumes a 1-D integer GA (of type MT_INT). Also assumes standard
 * distribution of the 1-D array.
 */
void enumerate(int g_src, int start, int inc)
{
    int me = GA_Nodeid();
    int nproc = GA_Nnodes();
    int64_t map[nproc*2];
    int procs[nproc];
    int64_t lo = 0;
    int64_t hi;
    int type;
    int ndim;
    int64_t size;
    int result;
    int64_t count = 0;
    int64_t *ptr = map;
    int *src;

    NGA_Inquire64(g_src, &type, &ndim, &size);
    hi = size-1;
    result = NGA_Locate_region64(g_src, &lo, &hi, map, procs);
    for (int i=0; procs[i]<me; ++i) {
        count += ptr[1]-ptr[0]+1;
        ptr += 2;
    }

    NGA_Distribution64(g_src, me, &lo, &hi);
    if (0 > lo || 0 > hi) {
        return;
    }

    NGA_Access64(g_src, &lo, &hi, &src, NULL);
    *src = count*inc + start;
    for (size_t i=1,limit=hi-lo+1; i<limit; ++i) {
        src[i] = src[i-1] + inc;
    }
    NGA_Release_update64(g_src, &lo, &hi);
}


/**
 * Unpack g_src into g_dst based on the mask g_msk.
 *
 * Assumes g_dst and g_msk have the same distributions.
 */
void unpack1d(int g_src, int g_dst, int g_msk)
{
    int me = GA_Nodeid();
    int nproc = GA_Nnodes();
    int *mask;
    long counts[nproc];
    int64_t lo_msk = 0;
    int64_t hi_msk = 0;
    int64_t lo_src = 0;
    int64_t hi_src = 0;
    int type;
    int ndim;
    int64_t dims;

    // get type, that's all we want...
    NGA_Inquire64(g_src, &type, &ndim, &dims);

    // count mask bits on each proc
    bzero(counts, sizeof(long)*nproc);
    NGA_Distribution64(g_msk, me, &lo_msk, &hi_msk);
    if (0 <= lo_msk || 0 <= hi_msk) {
        NGA_Access64(g_msk, &lo_msk, &hi_msk, &mask, NULL);
        for (int64_t i=0,limit=(hi_msk-lo_msk+1); i<limit; ++i) {
            if (mask[i] != 0) ++counts[me];
        }
        NGA_Release64(g_msk, &lo_msk, &hi_msk);
    }
    GA_Lgop(counts, nproc, "+");

    // if this proc got none of the mask, we can safely (and must) return now
    if (0 == counts[me]) {
        return;
    }
    
    // tally up where to start the 'get' of the packed array
    for (int i=0; i<me; ++i) {
        lo_src += counts[i];
    }
    hi_src = lo_src + counts[me] - 1;

    // do the unpacking
    // assumption is that dst array has same distribution as msk array
    if (0 <= lo_msk || 0 <= hi_msk) {
        switch (type) {
#define unpack1d_op(MTYPE,TYPE) \
            case MTYPE: { \
                TYPE srcbuf[hi_src-lo_src+1]; \
                TYPE *src = srcbuf; \
                TYPE *dst; \
                int *msk; \
                NGA_Get64(g_src, &lo_src, &hi_src, srcbuf, NULL); \
                NGA_Access64(g_dst, &lo_msk, &hi_msk, &dst, NULL); \
                NGA_Access64(g_msk, &lo_msk, &hi_msk, &msk, NULL); \
                for (size_t i=0,limit=hi_msk-lo_msk+1; i<limit; ++i) { \
                    if (msk[i] != 0) { \
                        dst[i] = *src; \
                        ++src; \
                    } \
                } \
                NGA_Release64(g_msk, &lo_msk, &hi_msk); \
                NGA_Release_update64(g_dst, &lo_msk, &hi_msk); \
                break; \
            }
            unpack1d_op(MT_INT,int)
            unpack1d_op(MT_LONGINT,long)
            unpack1d_op(MT_REAL,float)
            unpack1d_op(MT_DBL,double)
#undef unpack1d_op
        }
    }
}

