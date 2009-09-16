#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <cstring> // for memset

#include <ga.h>
#include <macdecls.h>
#include <message.h>

#include "Debug.H"
#include "Pack.H"


static inline void unravel64i(int64_t x, int ndim, int64_t *dims, int64_t *result)
{
    // x and dims of [a,b,c,d] --> [x/dcb % a, x/dc % b, x/d % c, x/1 % d]
    result[ndim-1] = x % dims[ndim-1];
    for (int i=ndim-2; i>=0; --i) {
        x /= dims[i+1];
        result[i] = x % dims[i];
    }
    for (int i=0; i<ndim; ++i) {
        if (result[i] >= dims[i]) {
            GA_Error("unravel64: result[i] >= dims[i]", 0);
        }
    }
}


void partial_sum(int g_src, int g_dst, int excl)
{
    TRACER("partial_sum\n");
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

    GA_Sync();
    
    NGA_Distribution64(g_src, me, &lo, &hi);
    elems = hi-lo+1;

    /* do the partial sum of our local portion, if any */
    if (0 > lo && 0 > hi); /* no elements stored on this process */
    else {
        NGA_Access64(g_src, &lo, &hi, &ptr_src, NULL);
        NGA_Access64(g_dst, &lo, &hi, &ptr_dst, NULL);
#define partial_sum_op(MTYPE_SRC,TYPE_SRC,MTYPE_DST,TYPE_DST) \
        if (MTYPE_SRC == type_src && MTYPE_DST == type_dst) { \
            TYPE_DST *dst = (TYPE_DST*)ptr_dst; \
            TYPE_SRC *src = (TYPE_SRC*)ptr_src; \
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
        } else
        partial_sum_op(C_INT,int,C_INT,int)
        partial_sum_op(C_INT,int,C_LONG,long)
        partial_sum_op(C_INT,int,C_LONGLONG,long long)
        partial_sum_op(C_LONG,long,C_LONG,long)
        partial_sum_op(C_LONG,long,C_LONGLONG,long long)
        partial_sum_op(C_LONGLONG,long long,C_LONGLONG,long long)
        partial_sum_op(C_FLOAT,float,C_FLOAT,float)
        partial_sum_op(C_FLOAT,float,C_DBL,double)
        partial_sum_op(C_DBL,double,C_DBL,double)
        ; // for last else above
#undef partial_sum_op
        NGA_Release_update64(g_dst, &lo, &hi);
        NGA_Release64(g_src, &lo, &hi);
    }
    if (0 > lo && 0 > hi) {
        /* no elements stored on this process */
        /* broadcast dummy value to all processes */
#define partial_sum_op(MTYPE,TYPE,GOP_OP) \
        if (MTYPE == type_dst) { \
            TYPE values[nproc]; \
            memset(values, 0, sizeof(TYPE)*nproc); \
            GOP_OP(values, nproc, "+"); \
            TRACER("partial_sum_op N/A\n"); \
        } else
        partial_sum_op(C_INT,int,armci_msg_igop)
        partial_sum_op(C_LONG,long,armci_msg_lgop)
        partial_sum_op(C_LONGLONG,long long,armci_msg_llgop)
        partial_sum_op(C_FLOAT,float,armci_msg_fgop)
        partial_sum_op(C_DBL,double,armci_msg_dgop)
        ; // for last else above
#undef partial_sum_op
    } else {
        /* broadcast last value to all processes */
        /* then sum those values and add to each element as appropriate */
        NGA_Access64(g_src, &lo, &hi, &ptr_src, NULL);
        NGA_Access64(g_dst, &lo, &hi, &ptr_dst, NULL);
#define partial_sum_op(MTYPE_SRC,TYPE_SRC,MTYPE_DST,TYPE_DST,GOP_OP,FMT) \
        if (MTYPE_DST == type_dst) { \
            TYPE_DST value = 0; \
            TYPE_DST values[nproc]; \
            TYPE_SRC *src = (TYPE_SRC*)ptr_src; \
            TYPE_DST *dst = (TYPE_DST*)ptr_dst; \
            memset(values, 0, sizeof(TYPE_DST)*nproc); \
            values[me] = dst[elems-1]; \
            if (0 == excl) { \
                values[me] += src[elems-1]; \
            } \
            GOP_OP(values, nproc, "+"); \
            for (int64_t i=0; i<me; ++i) { \
                value += values[i]; \
            } \
            for (int64_t i=0; i<elems; ++i) { \
                dst[i] += value; \
            } \
            TRACER1("partial_sum_op "#FMT"\n", value); \
        } else
        partial_sum_op(C_INT,int,C_INT,int,armci_msg_igop,%d)
        partial_sum_op(C_INT,int,C_LONG,long,armci_msg_lgop,%ld)
        partial_sum_op(C_INT,int,C_LONGLONG,long long,armci_msg_llgop,%lld)
        partial_sum_op(C_LONG,long,C_LONG,long,armci_msg_lgop,%ld)
        partial_sum_op(C_LONG,long,C_LONGLONG,long long,armci_msg_llgop,%lld)
        partial_sum_op(C_LONGLONG,long long,C_LONGLONG,long long,armci_msg_llgop,%lld)
        partial_sum_op(C_FLOAT,float,C_FLOAT,float,armci_msg_fgop,%f)
        partial_sum_op(C_FLOAT,float,C_DBL,double,armci_msg_dgop,%f)
        partial_sum_op(C_DBL,double,C_DBL,double,armci_msg_dgop,%f)
        ; // for last else above
#undef partial_sum_op
        NGA_Release_update64(g_dst, &lo, &hi);
        NGA_Release64(g_src, &lo, &hi);
    }
}



void pack(int g_src, int g_dst, int *g_masks)
{
    TRACER("pack\n");
    //int nproc = GA_Nnodes();
    int me = GA_Nodeid();

    int type_src;
    int ndim_src = GA_Ndim(g_src);
    int64_t dims_src[ndim_src];
    int64_t lo_src[ndim_src];
    int64_t hi_src[ndim_src];
    int64_t ld_src[ndim_src-1];
    int64_t elems_src[ndim_src];
    int64_t elems_product_src=1;

    int type_dst;
    int ndim_dst = GA_Ndim(g_dst);
    int64_t dims_dst[ndim_dst];
    int64_t lo_dst[ndim_dst];
    int64_t hi_dst[ndim_dst];
    int64_t ld_dst[ndim_dst-1];

    int64_t index[ndim_src];
    int g_masksums[ndim_src];
    int64_t local_counts[ndim_src];
    int64_t local_counts_product=1;
    int *local_masks[ndim_src];

    if (ndim_src != ndim_dst) {
        GA_Error("pack: src and dst ndims don't match", 0);
    }

    NGA_Inquire64(g_src, &type_src, &ndim_src, dims_src);
    NGA_Inquire64(g_dst, &type_dst, &ndim_dst, dims_dst);

    GA_Sync(); // do we need this?

    /* We must perform the partial sum on the masks*/
    for (int i=0; i<ndim_src; ++i) {
        //static bool done=false;
        g_masksums[i] = GA_Duplicate(g_masks[i], "maskcopy");
        partial_sum(g_masks[i], g_masksums[i], 0);
        /*
        if (0 == me && !done) {
            done = true;
            int type_msk;
            int ndim_msk;
            int64_t dims_msk[7];
            NGA_Inquire64(g_masks[i], &type_msk, &ndim_msk, dims_msk);
            int64_t lo=0;
            int64_t hi=dims_msk[i]-1;
            int msk[dims_msk[i]];
            int sum[dims_msk[i]];
            NGA_Get64(g_masks[i], &lo, &hi, msk, NULL);
            NGA_Get64(g_masksums[i], &lo, &hi, sum, NULL);
            for (int j=0; j<=hi; ++j) {
                printf("[%d] %d %d\n", j, msk[j], sum[j]);
            }
        }
        */
    }

    NGA_Distribution64(g_src, me, lo_src, hi_src);

    if (0 > lo_src[0] && 0 > hi_src[0]); /* no elements on this process */
    else {
        /* Now get the portions of the masks associated with each dim */
        memset(local_counts, 0, sizeof(int64_t)*ndim_src);
        for (int i=0; i<ndim_src; ++i) {
            elems_src[i] = hi_src[i]-lo_src[i]+1;
            elems_product_src *= elems_src[i];
            local_masks[i] = new int[elems_src[i]];
            NGA_Get64(g_masks[i], lo_src+i, hi_src+i, local_masks[i], NULL);
            for (int64_t j=0; j<elems_src[i]; ++j) {
                if (local_masks[i][j]) {
                    ++(local_counts[i]);
                }
            }
            local_counts_product *= local_counts[i];
        }
        //print_local_masks(local_masks, elems, ndim);

        /* if any of the local mask counts are zero, no work is needed */
        if (0 == local_counts_product); 
        else {
            /* determine where the data is to go */
            for (int i=0; i<ndim_src; ++i) {
                int tmp;
                NGA_Get64(g_masksums[i], lo_src+i, lo_src+i, &tmp, NULL);
                lo_dst[i] = tmp;
                hi_dst[i] = tmp+local_counts[i]-1;
            }
            //printf("ld_dst=");
            for (int i=0; i<ndim_src-1; ++i) {
                ld_dst[i] = local_counts[i+1];
                //printf("%d,", ld_dst[i]);
            }
            //printf("\n");
            
            /* Create the destination buffer */
            switch (type_src) {
#define pack_bit_copy(MTYPE,TYPE) \
                case MTYPE: \
                    { \
                        int64_t buf_dst_index = 0; \
                        TYPE *buf_src = NULL; \
                        TYPE *buf_dst = new TYPE[local_counts_product]; \
                        NGA_Access64(g_src, lo_src, hi_src, &buf_src, ld_src); \
                        for (int64_t i=0; i<elems_product_src; ++i) { \
                            unravel64i(i, ndim_src, elems_src, index); \
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
                            GA_Error("pack: mismatch", buf_dst_index); \
                        } \
                        NGA_Put64(g_dst, lo_dst, hi_dst, buf_dst, ld_dst); \
                        NGA_Release64(g_src, lo_src, hi_src); \
                        delete [] buf_dst; \
                        break; \
                    }
                pack_bit_copy(C_INT,int)
                pack_bit_copy(C_LONG,long)
                pack_bit_copy(C_FLOAT,float)
                pack_bit_copy(C_DBL,double)
#undef pack_bit_copy
            }
        }
        //print_local_masks(local_masks, elems, ndim);
    }

    // Clean up
    //printf("before Clean up\n");
    /* Remove temporary partial sum arrays */
    for (int i=0; i<ndim_src; ++i) {
        GA_Destroy(g_masksums[i]);
        int *tmp = local_masks[i];
        delete [] tmp;
        tmp = NULL;
    }
    //printf("pack(%d,%d,...) END\n", g_src, g_dst);
}


void unravel64(int64_t x, int ndim, int64_t *dims, int64_t *result)
{
    unravel64i(x,ndim,dims,result);
}


/**
 * A global enumeration operation.
 *
 * Assumes g_src is a 1D array.
 */
void enumerate(int g_src, void *start_val, void *inc_val)
{
    TRACER("enumerate BEGIN\n");
    int me = GA_Nodeid();
    int nproc = GA_Nnodes();
    int64_t src_lo;
    int64_t src_hi;
    int src_type;
    int src_ndim;
    int64_t src_size;
    int64_t loc_lo;
    int64_t loc_hi;
    int result;
    int64_t map[nproc*2];
    //int64_t *ptr = map;
    int procs[nproc];
    int64_t count;
    void *buf;

    NGA_Inquire64(g_src, &src_type, &src_ndim, &src_size);
    if (src_ndim > 1) {
        GA_Error("enumerate: expected 1D array", src_ndim);
    }

    NGA_Distribution64(g_src, me, &src_lo, &src_hi);
    //TRACER2("enumerate lo,hi = %lld,%lld\n", src_lo, src_hi);
    if (0 > src_lo && 0 > src_hi) {
        //TRACER("enumerate result = N/A\n");
        //TRACER("enumerate count = N/A\n");
    } else {
        loc_lo = 0;
        loc_hi = src_size-1;
        count = 0;
        result = NGA_Locate_region64(g_src, &loc_lo, &loc_hi, map, procs);
        //TRACER1("enumerate result = %d\n", result);
        for (int i=0; i<result; ++i) {
            if (procs[i] < me) {
                count += map[i*2+1]-map[i*2]+1;
            }
        }
        /*
        for (int i=0; procs[i]<me; ++i) {
            count += ptr[1]-ptr[0]+1;
            ptr += 2;
        }
        */
        //TRACER1("enumerate count = %lld\n", count);

        NGA_Access64(g_src, &src_lo, &src_hi, &buf, NULL);
#define enumerate_op(MTYPE,TYPE) \
        if (MTYPE == src_type) { \
            TYPE *src = (TYPE*)buf; \
                TYPE start = 0; \
                TYPE inc = 1; \
                if (start_val) { \
                    start = *((TYPE*)start_val); \
                } \
            if (inc_val) { \
                inc = *((TYPE*)inc_val); \
            } \
            for (int64_t i=0,limit=src_hi-src_lo+1; i<limit; ++i) { \
                src[i] = (count+i)*inc + start; \
            } \
        } else
        enumerate_op(C_INT,int)
        enumerate_op(C_LONG,long)
        enumerate_op(C_LONGLONG,long long)
        enumerate_op(C_FLOAT,float)
        enumerate_op(C_DBL,double)
        ; // for last else above
#undef enumerate_op
        NGA_Release_update64(g_src, &src_lo, &src_hi);
    }
    TRACER("enumerate END\n");
}


/**
 * Unpack g_src into g_dst based on the mask g_msk.
 *
 * Assumes g_dst and g_msk have the same distributions.
 */
void unpack1d(int g_src, int g_dst, int g_msk)
{
    TRACER("unpack1d BEGIN\n");
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

    if (1 == GA_Compare_distr(g_dst, g_msk)) {
        GA_Error("unpack1d: dst and msk distributions differ", 0);
    }

    // count mask bits on each proc
    memset(counts, 0, sizeof(long)*nproc);
    NGA_Distribution64(g_msk, me, &lo_msk, &hi_msk);
    if (0 > lo_msk && 0 > hi_msk) {
        GA_Lgop(counts, nproc, "+");
        TRACER("unpack1d lo,hi N/A 1\n");
    } else {
        NGA_Access64(g_msk, &lo_msk, &hi_msk, &mask, NULL);
        for (int64_t i=0,limit=(hi_msk-lo_msk+1); i<limit; ++i) {
            if (0 != mask[i]) ++(counts[me]);
        }
        NGA_Release64(g_msk, &lo_msk, &hi_msk);
        mask = NULL;
        GA_Lgop(counts, nproc, "+");
        if (0 == counts[me]) {
            TRACER("unpack1d lo,hi N/A 2\n");
        } else {
            // tally up where to start the 'get' of the packed array
            for (int i=0; i<me; ++i) {
                lo_src += counts[i];
            }
            hi_src = lo_src + counts[me] - 1;
            TRACER2("unpack1d lo,hi = %ld,%ld\n", lo_src, hi_src);
            // do the unpacking
            // assumption is that dst array has same distribution as msk array
            // get src (and dst) type, that's all we want...
            NGA_Inquire64(g_src, &type, &ndim, &dims);
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
                unpack1d_op(C_INT,int)
                unpack1d_op(C_LONG,long)
                unpack1d_op(C_LONGLONG,long long)
                unpack1d_op(C_FLOAT,float)
                unpack1d_op(C_DBL,double)
#undef unpack1d_op
            }
        }
    }
    TRACER("unpack1d END\n");
}

