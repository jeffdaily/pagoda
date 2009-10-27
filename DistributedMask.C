#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <numeric>

#include <macdecls.h>
#include <message.h>

#include "Common.H"
#include "Debug.H"
#include "Dimension.H"
#include "DistributedMask.H"
#include "Pack.H"
#include "Slice.H"
#include "Timing.H"
#include "Variable.H"

using std::accumulate;
using std::fill;


DistributedMask::DistributedMask(Dimension *dim, int value)
    :   Mask(dim)
    ,   handle(0)
    ,   handle_index(0)
    ,   lo(0)
    ,   hi(0)
    ,   counts(NPROC, 0)
{
    TIMING("DistributedMask::DistributedMask(...)");
    handle = NGA_Create64(C_INT, 1, &size, const_cast<char*>(name.c_str()), NULL);
    TRACER3("DistributedMask ctor name=%s,size=%ld,handle=%d\n",
            name.c_str(), size, handle);
    NGA_Distribution64(handle, ME, &lo, &hi);
    GA_Fill(handle, &value);
}


DistributedMask::~DistributedMask()
{
    TIMING("DistributedMask::~DistributedMask()");
    GA_Destroy(handle);
    if (handle_index != 0) {
        GA_Destroy(handle_index);
    }
}


void DistributedMask::get_data(vector<int> &ret)
{
    TIMING("DistributedMask::get_data(vector<int>)");
    int64_t glo = 0;
    int64_t ghi = size - 1;
    ret.resize(size);
    NGA_Get64(handle, &glo, &ghi, &ret[0], NULL);
}


void DistributedMask::get_data(vector<int> &ret, int64_t lo, int64_t hi)
{
    TIMING("DistributedMask::get_data(vector<int>,int64_t,int64_t)");
    ret.resize(hi-lo+1);
    NGA_Get64(handle, &lo, &hi, &ret[0], NULL);
}


void DistributedMask::clear()
{
    TIMING("DistributedMask::clear()");
    // bail if already cleared once (a one-time operation)
    if (cleared) {
        return;
    } else {
        cleared = true;
        int ZERO = 0;
        GA_Fill(handle, &ZERO);
    }
}


void DistributedMask::adjust(const DimSlice &slice)
{
    TIMING("DistributedMask::adjust(DimSlice)");
    need_recount = true;
    clear();

    // bail if we don't own any of the data
    if (0 > lo || 0 > hi) return;

    int64_t slo;
    int64_t shi;
    int64_t step;
    int *data;

    slice.indices(size, slo, shi, step);
    NGA_Access64(handle, &lo, &hi, &data, NULL);
    if (lo <= slo) {
        int64_t start = slo-lo;
        if (hi >= shi) {
            for (int64_t i=start,limit=(shi-lo); i<limit; i+=step) {
                data[i] = 1;
            }
        } else { // hi < shi
            for (int64_t i=start,limit=(hi-lo+1); i<limit; i+=step) {
                data[i] = 1;
            }
        }
    } else if (lo <= shi) { // && lo > slo
        int64_t start = slo-lo;
        while (start < 0) start+=step;
        if (hi >= shi) {
            for (int64_t i=start,limit=(shi-lo); i<limit; i+=step) {
                data[i] = 1;
            }
        } else { // hi < shi
            for (int64_t i=start,limit=(hi-lo+1); i<limit; i+=step) {
                data[i] = 1;
            }
        }
    }
    NGA_Release_update64(handle, &lo, &hi);
}


void DistributedMask::adjust(const LatLonBox &box, Variable *lat, Variable *lon)
{
    TIMING("DistributedMask::adjust(LatLonBox,Variable*,Variable*)");
    need_recount = true;
    clear();

    // bail if we don't own any of the data
    if (0 > lo || 0 > hi) return;

    // currently assumes the Variables are 1-D (coorindate) variables
    // which means they have the same data distribution as this mask
    int *mask_data;
    void *lat_data;
    void *lon_data;
    int lat_handle = lat->get_handle();
    int lon_handle = lon->get_handle();
    int type;
    int ndim;
    int64_t dims[1];
    int64_t size = hi-lo+1;

    lat->read();
    lon->read();
    NGA_Inquire64(lat_handle, &type, &ndim, dims);
    NGA_Access64(handle, &lo, &hi, &mask_data, NULL);
    NGA_Access64(lat_handle, &lo, &hi, &lat_data, NULL);
    NGA_Access64(lon_handle, &lo, &hi, &lon_data, NULL);

    switch (type) {
#define adjust_op(MTYPE,TYPE) \
        case MTYPE: { \
            TYPE *plat = (TYPE*)lat_data; \
            TYPE *plon = (TYPE*)lon_data; \
            for (int64_t i=0; i<size; ++i) { \
                mask_data[i] = box.contains(plat[i], plon[i]) ? 1 : 0; \
            } \
            break; \
        }
        adjust_op(C_INT,int)
        adjust_op(C_LONG,long)
        adjust_op(C_LONGLONG,long long)
        adjust_op(C_FLOAT,float)
        adjust_op(C_DBL,double)
#undef adjust_op
    }

    NGA_Release64(lon_handle, &lo, &hi);
    NGA_Release64(lat_handle, &lo, &hi);
    NGA_Release_update64(handle, &lo, &hi);
    lat->release_handle();
    lon->release_handle();
}


void DistributedMask::adjust(
        double min,
        double max,
        Variable *var,
        bool bitwise_or)
{
    TIMING("DistributedMask::adjust(double,double,Variable*,bool)");
    need_recount = true;
    clear();

    // bail if we don't own any of the data
    if (0 > lo || 0 > hi) return;

    // currently assumes the Variable is a 1-D (coorindate) variable 
    // which means it has the same data distribution as this mask
    int *mask;
    void *data;
    int var_handle = var->get_handle();
    int type;
    int ndim;
    int64_t dims[1];

    NGA_Inquire64(var_handle, &type, &ndim, dims);
    var->read();
    NGA_Access64(handle, &lo, &hi, &mask, NULL);
    NGA_Access64(var_handle, &lo, &hi, &data, NULL);

    switch (type) {
#define adjust_op(MTYPE,type) \
        case MTYPE: { \
            type *pdata = (type*)data; \
            if (bitwise_or) { \
                for (int64_t i=0,limit=hi-lo+1; i<limit; ++i) { \
                    mask[i] |= (pdata[i] >= lo && pdata[i] <= hi); \
                } \
            } else { \
                for (int64_t i=0,limit=hi-lo+1; i<limit; ++i) { \
                    mask[i] &= (pdata[i] >= lo && pdata[i] <= hi); \
                } \
            } \
            break; \
        }
        adjust_op(C_INT,int)
        adjust_op(C_LONG,long)
        adjust_op(C_LONGLONG,long long)
        adjust_op(C_FLOAT,float)
        adjust_op(C_DBL,double)
#undef adjust_op
    }

    NGA_Release64(var_handle, &lo, &hi);
    NGA_Release_update64(handle, &lo, &hi);
    var->release_handle();
}


void DistributedMask::recount()
{
    TIMING("DistributedMask::recount()");
    int *data;
    int64_t ZERO = 0;
    fill(counts.begin(), counts.end(), ZERO);
    if (0 > lo || 0 > hi) {
    } else {
        NGA_Access64(handle, &lo, &hi, &data, NULL);
        for (int64_t i=0,limit=(hi-lo+1); i<limit; ++i) {
            if (data[i] != 0) {
                ++(counts[ME]);
            }
        }
        NGA_Release64(handle, &lo, &hi);
    }
#if SIZEOF_INT64_T == SIZEOF_INT
    armci_msg_igop(&counts[0], NPROC, "+");
#elif SIZEOF_INT64_T == SIZEOF_LONG
    armci_msg_lgop(&counts[0], NPROC, "+");
#elif SIZEOF_INT64_T == SIZEOF_LONG_LONG
    armci_msg_llgop(&counts[0], NPROC, "+");
#endif
    count = accumulate(counts.begin(), counts.end(), ZERO);
}


void DistributedMask::reindex()
{
    TIMING("DistributedMask::reindex()");
    TRACER("DistributedMask::reindex() BEGIN\n");
    if (0 == handle_index) {
        int type;
        int ndim;
        int64_t dims;

        NGA_Inquire64(handle, &type, &ndim, &dims);
        handle_index = NGA_Create64(C_INT, 1, &dims, "index", NULL);
    }
    int ONE_NEG = -1;
    int handle_tmp;
    int64_t tmp_count = get_count();
    //int64_t icount;

    GA_Fill(handle_index, &ONE_NEG);
    handle_tmp = NGA_Create64(C_INT, 1, &tmp_count, "tmp_enum", NULL);
    //GA_Patch_enum64(handle_tmp, 0, tmp_count-1, 0, 1);
    enumerate(handle_tmp, NULL, NULL);
    //GA_Unpack64(handle_tmp, handle_index, handle, 0, tmp_count-1, &icount);
    unpack1d(handle_tmp, handle_index, handle);
    GA_Destroy(handle_tmp);
    /*
    if (GA_Nnodes() == 1) {
        int64_t ZERO = 0;
        int64_t bighi = size-1;
        int64_t smlhi = tmp_count-1;
        int *src,*dst,*mask;
        NGA_Access64(handle_index, &ZERO, &bighi, &dst, NULL);
        NGA_Access64(handle, &ZERO, &bighi, &mask, NULL);
        NGA_Access64(handle_tmp, &ZERO, &smlhi, &src, NULL);
        for (int64_t i=0; i<=bighi; ++i) {
            std::cout << "[" << i << "]" << dst[i] << " " << mask[i] << std::endl;
        }
    }
    */
    TRACER("DistributedMask::reindex() END\n");
}


bool DistributedMask::access(int64_t &alo, int64_t &ahi, int* &data)
{
    // bail if we don't own any of the data
    if (0 > lo || 0 > hi) {
        return false;
    }
    alo = lo;
    ahi = hi;
    NGA_Access64(handle, &lo, &hi, &data, NULL);
    return true;
}


void DistributedMask::release()
{
    NGA_Release_update64(handle, &lo, &hi);
}
