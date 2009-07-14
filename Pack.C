#include <strings.h> // for bzero

#include <ga.h>
#include <macdecls.h>

#include "Pack.H"

//extern void* ga_malloc(Integer nelem, int type, char *name);

void PartialSum(int g_src, int g_dst, int excl)
{
    int nproc = GA_Nnodes();
    int me = GA_Nodeid();
    int type;
    int ndim;
    int64_t dims;
    int64_t lo = 0;
    int64_t hi = -1;
    long *lim = new long[nproc];
    int *ptr_src;
    int *ptr_dst;
    int64_t i;
    int64_t counter;
    int64_t first;
    int64_t elems;
    int64_t myplace=0;
    int64_t icount=0;

    GA_Check_handle(g_src, "PartialSum src");
    GA_Check_handle(g_dst, "PartialSum dst");
    if (1 == GA_Compare_distr(g_src, g_dst)) {
        GA_Error("PartialSum: GAs must have same distribution", 1);
    }

    NGA_Inquire64(g_src, &type, &ndim, &dims);
    if (ndim>1) {
        GA_Error("PartialSum: supports 1-dim arrays only", ndim);
    }

    GA_Sync();
    
    /* how many elements do we have to copy? */
    bzero(lim, sizeof(long)*nproc);
    //for (i=0; i<nproc; ++i) lim[i] = 0;
    NGA_Distribution64(g_src, me, &lo, &hi);
    elems = hi-lo+1;
    if (0 > lo && 0 > hi); /* no elements stored on this process */
    else {
        NGA_Access64(g_src, &lo, &hi, &ptr_src, NULL);
        /* find number of elements to be contributed */
        for (i=counter=0,first=-1; i<elems; ++i) {
            if (ptr_src[i]) {
                ++counter;
                if (first == -1) {
                    first = i;
                }
            }
        }
        lim[me] = counter;
        NGA_Release64(g_src, &lo, &hi);
    }

    /* find number of elements everybody else is contributing */
    GA_Lgop(lim, nproc, "+");

    /* find where this proc should put its data and the total count of items */
    for (i=myplace=icount=0; i<nproc; ++i) {
        if (i<me && lim[i]) {
            myplace += lim[i];
        }
        icount += lim[i];
    }

    if (0 > lo && 0 > hi); /* no elements stored on this process */
    else {
        NGA_Access64(g_src, &lo, &hi, &ptr_src, NULL);
        NGA_Access64(g_dst, &lo, &hi, &ptr_dst, NULL);
        if (0 == excl) {
            ptr_dst[0] = myplace;
            for (i=1; i<elems; ++i) {
                ptr_dst[i] = ptr_dst[i-1] + ptr_src[i-1];
            }
        } else {
            ptr_dst[0] = myplace + ptr_src[0];
            for (i=1; i<elems; ++i) {
                ptr_dst[i] = ptr_dst[i-1] + ptr_src[i];
            }
        }
        NGA_Release64(g_src, &lo, &hi);
        NGA_Release_update64(g_dst, &lo, &hi);
    }
    delete [] lim;
}

